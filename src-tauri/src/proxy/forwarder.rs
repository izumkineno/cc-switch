//! 请求转发器
//!
//! 负责将请求转发到上游Provider，支持故障转移

use super::hyper_client::{ProxyResponse, MAX_RESPONSE_BODY_BYTES};
use super::{
    body_filter::filter_private_params_with_whitelist,
    content_encoding::{decompress_body_with_limit, get_content_encoding},
    error::*,
    failover_switch::FailoverSwitchManager,
    json_canonical::{canonicalize_value, short_value_hash},
    log_codes::fwd as log_fwd,
    provider_router::ProviderRouter,
    providers::{
        codex_chat_history::CodexChatHistoryStore, gemini_shadow::GeminiShadowStore, get_adapter,
        AuthInfo, AuthStrategy, ProviderAdapter, ProviderType,
    },
    thinking_budget_rectifier::{rectify_thinking_budget, should_rectify_thinking_budget},
    thinking_rectifier::{
        normalize_thinking_type, rectify_anthropic_request, should_rectify_thinking_signature,
    },
    types::{CopilotOptimizerConfig, OptimizerConfig, ProxyStatus, RectifierConfig},
    usage::{TokenUsage, UsageLogger},
    ProxyError,
};
use crate::commands::{CodexOAuthState, CopilotAuthState, XaiOAuthState};
use crate::proxy::providers::copilot_auth::CopilotAuthManager;
use crate::proxy::providers::xai_oauth_auth::XaiOAuthManager;
use crate::{
    app_config::AppType,
    database::Database,
    provider::{LocalProxyRequestOverrides, Provider, ProviderRetryPolicy},
};
use bytes::Bytes;
use futures::StreamExt;
use http::Extensions;
use serde_json::Value;
use std::sync::Arc;
use tauri::Manager;
use tokio::sync::RwLock;

const PROXY_AUTH_PLACEHOLDER: &str = "PROXY_MANAGED";

fn validate_codex_official_authorization(
    headers: &http::HeaderMap,
    provider: &Provider,
) -> Result<(), ProxyError> {
    let authorization = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::trim);
    match authorization {
        None | Some("") => Err(ProxyError::AuthError(
            "Codex 官方登录不可用，请先在 Codex 中完成 ChatGPT 登录".to_string(),
        )),
        Some(value) if value.contains(PROXY_AUTH_PLACEHOLDER) => Err(ProxyError::AuthError(
            "已切换到 OpenAI 官方供应商，请重启 Codex 或新建会话以加载官方登录配置".to_string(),
        )),
        Some(_) => {
            let expected_account_id = provider
                .meta
                .as_ref()
                .and_then(|meta| meta.managed_account_id_for("codex_oauth"))
                .map(|account_id| account_id.trim().to_string())
                .filter(|account_id| !account_id.is_empty());
            if let Some(expected_account_id) = expected_account_id {
                let request_account_id = headers
                    .get("chatgpt-account-id")
                    .and_then(|value| value.to_str().ok())
                    .map(str::trim)
                    .filter(|account_id| !account_id.is_empty());
                if request_account_id != Some(expected_account_id.as_str()) {
                    return Err(ProxyError::AuthError(
                        "当前 Codex 会话未加载所选 ChatGPT 账号，请重启 Codex 或新建会话后重试"
                            .to_string(),
                    ));
                }
            }
            Ok(())
        }
    }
}

pub struct ForwardResult {
    pub response: ProxyResponse,
    pub provider: Provider,
    pub claude_api_format: Option<String>,
    /// 实际发往上游的模型名（路由接管/模型映射后的真值）。
    ///
    /// usage 归因不能依赖 ctx.request_model（映射前的客户端别名）：上游响应
    /// 缺失 model 或回显别名时，接管流量会被记成 claude-* 并按其定价计费。
    pub outbound_model: Option<String>,
    /// 活跃连接 RAII guard：随响应一起流转到 response_processor / handle_claude_transform，
    /// 最终被 move 进流式 body future（或非流式响应作用域），覆盖整个响应生命周期。
    pub(crate) connection_guard: Option<ActiveConnectionGuard>,
}

pub struct ForwardError {
    pub error: ProxyError,
    pub provider: Option<Provider>,
}

/// 活跃连接 RAII guard
///
/// 构造时把 `ProxyStatus.active_connections` +1；Drop 时在 tokio runtime 上调度
/// 一个异步任务执行 -1，从而支持把 guard move 进流式 body future（stream 自然结束
/// 时 guard 与 future 一起 drop）。
///
/// 设计动机：之前在 `forward_with_retry` 出口处同步 -1，但流式响应的 body 实际
/// 在 `create_logged_passthrough_stream` 内还会继续 yield 字节流，导致 UI 的
/// `active_connections` 计数过早归零。RAII guard 让"减量"由 Rust 类型系统驱动，
/// 不需要每条出口路径都手动调用。
pub(crate) struct ActiveConnectionGuard {
    status: Arc<RwLock<ProxyStatus>>,
}

impl ActiveConnectionGuard {
    pub(crate) async fn acquire(status: Arc<RwLock<ProxyStatus>>) -> Self {
        {
            let mut s = status.write().await;
            s.active_connections = s.active_connections.saturating_add(1);
        }
        Self { status }
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        // Drop 不能 await：把减量操作调度到 tokio runtime
        let status = self.status.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let mut s = status.write().await;
                s.active_connections = s.active_connections.saturating_sub(1);
            });
        }
        // 没有 runtime 时静默丢失计数（仅 UI 展示用，可接受最终一致性）
    }
}
#[derive(Debug, Clone, Default)]
struct ProviderExecutionPin {
    /// Resolved once at SlotStart and reused by ordinary and repair sends.
    base_url: Option<String>,
    /// Managed/static credentials are resolved once for the provider slot.
    auth: Option<AuthInfo>,
    /// `None` is a valid result for providers that do not require auth, so
    /// keep an explicit resolution bit instead of resolving on every retry.
    auth_resolved: bool,
    /// Codex OAuth's selected managed account is part of the pinned auth
    /// context. The manager's default account must not change mid-slot.
    codex_oauth_account_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderSlotState {
    SlotStart,
    OrdinaryAttempt(usize),
    Complete,
    Retryable,
    RepairNeeded,
}

struct ProviderAttemptPermit {
    router: Arc<ProviderRouter>,
    provider_id: String,
    app_type: String,
    used_half_open_permit: bool,
    finalized: bool,
    finalizing: bool,
}

impl ProviderAttemptPermit {
    fn new(
        router: Arc<ProviderRouter>,
        provider_id: &str,
        app_type: &str,
        used_half_open_permit: bool,
    ) -> Self {
        Self {
            router,
            provider_id: provider_id.to_string(),
            app_type: app_type.to_string(),
            used_half_open_permit,
            finalized: false,
            finalizing: false,
        }
    }

    async fn record_success(&mut self, forwarder: &RequestForwarder) {
        if self.finalized || self.finalizing {
            return;
        }
        self.finalizing = true;
        forwarder
            .record_success_result(
                &self.provider_id,
                &self.app_type,
                self.used_half_open_permit,
            )
            .await;
        self.finalizing = false;
        self.finalized = true;
    }

    async fn record_failure(&mut self, error: &ProxyError) {
        if self.finalized || self.finalizing {
            return;
        }
        self.finalizing = true;
        let _ = self
            .router
            .record_result(
                &self.provider_id,
                &self.app_type,
                self.used_half_open_permit,
                false,
                Some(error.to_string()),
            )
            .await;
        self.finalizing = false;
        self.finalized = true;
    }

    async fn release_neutral(&mut self) {
        if self.finalized || self.finalizing {
            return;
        }
        self.finalizing = true;
        self.router
            .release_permit_neutral(
                &self.provider_id,
                &self.app_type,
                self.used_half_open_permit,
            )
            .await;
        self.finalizing = false;
        self.finalized = true;
    }
}

impl Drop for ProviderAttemptPermit {
    fn drop(&mut self) {
        if self.finalized || !self.used_half_open_permit {
            return;
        }
        let router = self.router.clone();
        let provider_id = self.provider_id.clone();
        let app_type = self.app_type.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                router
                    .release_permit_neutral(&provider_id, &app_type, true)
                    .await;
            });
        }
    }
}

struct ProviderSlotFailure {
    error: ProxyError,
    /// Repair failures that are client/config errors retain the existing
    /// one-shot repair behavior and must not enter cross-provider failover.
    allow_failover: bool,
}

struct ProviderForwardedResponse {
    response: ProxyResponse,
    claude_api_format: Option<String>,
    outbound_model: Option<String>,
}
enum ProviderRepairOutcome {
    NotNeeded,
    Success(ProviderForwardedResponse),
    Failed {
        error: ProxyError,
        allow_failover: bool,
    },
    FailedOriginal {
        allow_failover: bool,
    },
}

pub struct RequestForwarder {
    /// 共享的 ProviderRouter（持有熔断器状态）
    router: Arc<ProviderRouter>,
    /// 失败上游尝试的 token usage 与普通代理日志写入同一数据库。
    db: Arc<Database>,
    /// 请求开始时快照的 usage logging 开关。
    usage_logging_enabled: bool,
    status: Arc<RwLock<ProxyStatus>>,
    current_providers: Arc<RwLock<std::collections::HashMap<String, (String, String)>>>,
    gemini_shadow: Arc<GeminiShadowStore>,
    codex_chat_history: Arc<CodexChatHistoryStore>,
    /// 故障转移切换管理器
    failover_manager: Arc<FailoverSwitchManager>,
    /// AppHandle，用于发射事件和更新托盘
    app_handle: Option<tauri::AppHandle>,
    /// 请求开始时的"当前供应商 ID"（用于判断是否需要同步 UI/托盘）
    current_provider_id_at_start: String,
    /// 代理会话 ID（用于 Gemini Native shadow replay）
    session_id: String,
    /// Session ID 是否由客户端提供；生成值不能作为上游缓存身份。
    session_client_provided: bool,
    /// 整流器配置
    rectifier_config: RectifierConfig,
    /// 优化器配置
    optimizer_config: OptimizerConfig,
    /// Copilot 优化器配置
    copilot_optimizer_config: CopilotOptimizerConfig,
    /// 非流式请求超时（秒）
    non_streaming_timeout: std::time::Duration,
    /// 流式请求响应头等待超时（秒）
    streaming_first_byte_timeout: std::time::Duration,
    /// 单个客户端请求最多尝试的 provider 数。
    ///
    /// 由 `AppProxyConfig.max_retries` (UI: "请求失败时的重试次数, 0-10") 派生：
    /// `max_attempts = max_retries + 1`，所以 max_retries=0 表示仅尝试一家、
    /// max_retries=3（默认）表示最多 4 家。loop 同时受 providers.len() 自然限制。
    max_attempts: usize,
}

impl RequestForwarder {
    /// 预防式 media 降级：发送前对 text-only 模型把图片块替换为标记。
    ///
    /// 受 `enabled && request_media_fallback` 管辖；其中"启发式模型名单预测"
    /// 再受 `request_media_heuristic` 单独管辖（显式声明 text-only 始终生效）。
    /// 返回被替换的图片块数量（0 = 未触发或开关关闭）。
    fn apply_media_prevention(&self, body: &mut Value, provider: &Provider) -> usize {
        if !(self.rectifier_config.enabled && self.rectifier_config.request_media_fallback) {
            return 0;
        }
        let replaced_images = super::media_sanitizer::replace_images_for_text_only_model(
            body,
            provider,
            self.rectifier_config.request_media_heuristic,
        );
        if replaced_images > 0 {
            let model = body.get("model").and_then(Value::as_str).unwrap_or("");
            log::info!(
                "[Media] Replaced {replaced_images} image block(s) with {} for text-only provider={}, model={}",
                super::media_sanitizer::UNSUPPORTED_IMAGE_MARKER,
                provider.id,
                model
            );
        }
        replaced_images
    }

    /// 反应式 media 重试判定：上游因图片输入报错后，是否应替换图片块并对同一供应商重试一次。
    ///
    /// 受 `enabled && request_media_fallback` 管辖；不涉及 `request_media_heuristic`——
    /// 这里是上游"实测"错误后的纯恢复，不是预测，故启发式开关与它无关。
    fn media_retry_should_trigger(
        &self,
        adapter_name: &str,
        already_retried: bool,
        provider_body: &Value,
        error: &ProxyError,
    ) -> bool {
        matches!(adapter_name, "Claude" | "Codex")
            && self.rectifier_config.enabled
            && self.rectifier_config.request_media_fallback
            && !already_retried
            && super::media_sanitizer::contains_image_blocks(provider_body)
            && super::media_sanitizer::is_unsupported_image_error(error)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        router: Arc<ProviderRouter>,
        db: Arc<Database>,
        usage_logging_enabled: bool,
        non_streaming_timeout: u64,
        status: Arc<RwLock<ProxyStatus>>,
        current_providers: Arc<RwLock<std::collections::HashMap<String, (String, String)>>>,
        gemini_shadow: Arc<GeminiShadowStore>,
        codex_chat_history: Arc<CodexChatHistoryStore>,
        failover_manager: Arc<FailoverSwitchManager>,
        app_handle: Option<tauri::AppHandle>,
        current_provider_id_at_start: String,
        session_id: String,
        session_client_provided: bool,
        streaming_first_byte_timeout: u64,
        _streaming_idle_timeout: u64,
        rectifier_config: RectifierConfig,
        optimizer_config: OptimizerConfig,
        copilot_optimizer_config: CopilotOptimizerConfig,
        max_retries: u32,
    ) -> Self {
        // max_retries 是「失败后重试次数」语义，attempt 上限 = retries + 1。
        // saturating_add 防止 u32::MAX + 1 溢出。
        let max_attempts = (max_retries as usize).saturating_add(1);
        Self {
            router,
            db,
            usage_logging_enabled,
            status,
            current_providers,
            gemini_shadow,
            codex_chat_history,
            failover_manager,
            app_handle,
            current_provider_id_at_start,
            session_id,
            session_client_provided,
            rectifier_config,
            optimizer_config,
            copilot_optimizer_config,
            non_streaming_timeout: std::time::Duration::from_secs(non_streaming_timeout),
            streaming_first_byte_timeout: std::time::Duration::from_secs(
                streaming_first_byte_timeout,
            ),
            max_attempts,
        }
    }

    async fn record_success_result(
        &self,
        provider_id: &str,
        app_type: &str,
        used_half_open_permit: bool,
    ) {
        if used_half_open_permit {
            if let Err(e) = self
                .router
                .record_result(provider_id, app_type, true, true, None)
                .await
            {
                log::warn!(
                    "[{app_type}] 记录 Provider 成功结果失败: provider_id={provider_id}, error={e}"
                );
            }
            return;
        }

        let router = self.router.clone();
        let provider_id = provider_id.to_string();
        let app_type = app_type.to_string();
        tokio::spawn(async move {
            if let Err(e) = router
                .record_result(&provider_id, &app_type, false, true, None)
                .await
            {
                log::warn!(
                    "[{app_type}] 异步记录 Provider 成功结果失败: provider_id={provider_id}, error={e}"
                );
            }
        });
    }

    /// 转发请求（带故障转移）
    ///
    /// 这是 thin wrapper：在客户端请求维度记一次 `total_requests` / 调整
    /// `active_connections` / 刷新 `last_request_at`，无论 inner 走哪条出口路径，
    /// 出口处都会把 `active_connections` 回收。Per-attempt 维度（成功/失败/熔断
    /// 等）仍由 inner 内自行更新 `success_requests` / `failed_requests`。
    #[allow(clippy::too_many_arguments)]
    pub async fn forward_with_retry(
        &self,
        app_type: &AppType,
        method: http::Method,
        endpoint: &str,
        body: Value,
        headers: axum::http::HeaderMap,
        extensions: Extensions,
        providers: Vec<Provider>,
    ) -> Result<ForwardResult, ForwardError> {
        let guard = ActiveConnectionGuard::acquire(self.status.clone()).await;
        {
            let mut s = self.status.write().await;
            s.total_requests = s.total_requests.saturating_add(1);
            s.last_request_at = Some(chrono::Utc::now().to_rfc3339());
        }
        let result = self
            .forward_with_retry_inner(
                app_type, method, endpoint, body, headers, extensions, providers,
            )
            .await;
        // 把 guard 注入到 Ok 结果，让它随响应一起流转到 response_processor，
        // 在流式 body 的 future 内才真正 drop。
        // Err 路径：guard 在函数 scope 内随返回值落地时自动 drop。
        result.map(|mut fr| {
            fr.connection_guard = Some(guard);
            fr
        })
    }

    /// 实际转发逻辑（不包含客户端维度的入口/出口计数）
    ///
    /// ProviderRouter 仍只负责在请求开始时选择并排序 providers；同一 provider
    /// 的 policy loop 在这里运行，不会重新选择 provider。
    #[allow(clippy::too_many_arguments)]
    async fn forward_with_retry_inner(
        &self,
        app_type: &AppType,
        method: http::Method,
        endpoint: &str,
        body: Value,
        headers: axum::http::HeaderMap,
        extensions: Extensions,
        providers: Vec<Provider>,
    ) -> Result<ForwardResult, ForwardError> {
        let adapter = get_adapter(app_type).ok_or_else(|| ForwardError {
            error: ProxyError::ConfigError(format!(
                "{} does not support proxy routing",
                app_type.as_str()
            )),
            provider: None,
        })?;
        let app_type_str = app_type.as_str();

        if providers.is_empty() {
            return Err(ForwardError {
                error: ProxyError::NoAvailableProvider,
                provider: None,
            });
        }

        let mut last_error = None;
        let mut last_provider = None;
        let mut attempted_providers = 0usize;
        let bypass_circuit_breaker = providers.len() == 1;

        for provider in &providers {
            if attempted_providers >= self.max_attempts {
                log::warn!(
                    "[{app_type_str}] 已达最大尝试次数上限 ({}/{}), 停止故障转移",
                    attempted_providers,
                    self.max_attempts
                );
                break;
            }

            let (allowed, used_half_open_permit) = if bypass_circuit_breaker {
                (true, false)
            } else {
                let permit = self
                    .router
                    .allow_provider_request(&provider.id, app_type_str)
                    .await;
                (permit.allowed, permit.used_half_open_permit)
            };
            if !allowed {
                continue;
            }

            attempted_providers += 1;
            {
                let mut status = self.status.write().await;
                status.current_provider = Some(provider.name.clone());
                status.current_provider_id = Some(provider.id.clone());
            }

            let retry_policy = if app_type.supports_local_proxy() {
                provider
                    .meta
                    .as_ref()
                    .map(|meta| meta.normalized_retry_policy())
                    .unwrap_or_else(ProviderRetryPolicy::disabled)
            } else {
                ProviderRetryPolicy::disabled()
            };
            let mut permit = ProviderAttemptPermit::new(
                self.router.clone(),
                &provider.id,
                app_type_str,
                used_half_open_permit,
            );

            match self
                .execute_provider_slot(
                    app_type,
                    method.clone(),
                    endpoint,
                    &body,
                    &headers,
                    &extensions,
                    provider,
                    adapter.as_ref(),
                    retry_policy,
                    &mut permit,
                )
                .await
            {
                Ok(forwarded) => {
                    {
                        let mut current_providers = self.current_providers.write().await;
                        current_providers.insert(
                            app_type_str.to_string(),
                            (provider.id.clone(), provider.name.clone()),
                        );
                    }

                    let should_switch =
                        self.current_provider_id_at_start.as_str() != provider.id.as_str();
                    {
                        let mut status = self.status.write().await;
                        status.success_requests += 1;
                        status.last_error = None;
                        if should_switch {
                            status.failover_count += 1;
                        }
                        if status.total_requests > 0 {
                            status.success_rate = (status.success_requests as f32
                                / status.total_requests as f32)
                                * 100.0;
                        }
                    }
                    if should_switch {
                        let fm = self.failover_manager.clone();
                        let ah = self.app_handle.clone();
                        let pid = provider.id.clone();
                        let pname = provider.name.clone();
                        let at = app_type_str.to_string();
                        tokio::spawn(async move {
                            let _ = fm.try_switch(ah.as_ref(), &at, &pid, &pname).await;
                        });
                    }

                    return Ok(ForwardResult {
                        response: forwarded.response,
                        provider: provider.clone(),
                        claude_api_format: forwarded.claude_api_format,
                        outbound_model: forwarded.outbound_model,
                        connection_guard: None,
                    });
                }
                Err(failure) => {
                    if !failure.allow_failover {
                        let mut status = self.status.write().await;
                        status.failed_requests += 1;
                        status.last_error = Some(failure.error.to_string());
                        if status.total_requests > 0 {
                            status.success_rate = (status.success_requests as f32
                                / status.total_requests as f32)
                                * 100.0;
                        }
                        return Err(ForwardError {
                            error: failure.error,
                            provider: Some(provider.clone()),
                        });
                    }

                    match self.categorize_proxy_error(&failure.error, provider) {
                        ErrorCategory::Retryable => {
                            {
                                let mut status = self.status.write().await;
                                status.last_error = Some(format!(
                                    "Provider {} 失败: {}",
                                    provider.name, failure.error
                                ));
                            }
                            let (log_code, log_message) = build_retryable_failure_log(
                                &provider.name,
                                attempted_providers,
                                providers.len(),
                                &failure.error,
                            );
                            log::warn!("[{app_type_str}] [{log_code}] {log_message}");
                            last_error = Some(failure.error);
                            last_provider = Some(provider.clone());
                        }
                        ErrorCategory::NonRetryable | ErrorCategory::ClientAbort => {
                            last_error = Some(failure.error);
                            last_provider = Some(provider.clone());
                            let mut status = self.status.write().await;
                            status.failed_requests += 1;
                            status.last_error = last_error.as_ref().map(ToString::to_string);
                            if status.total_requests > 0 {
                                status.success_rate = (status.success_requests as f32
                                    / status.total_requests as f32)
                                    * 100.0;
                            }
                            return Err(ForwardError {
                                error: last_error.take().unwrap_or(ProxyError::MaxRetriesExceeded),
                                provider: last_provider.take(),
                            });
                        }
                    }
                }
            }
        }

        if attempted_providers == 0 {
            let mut status = self.status.write().await;
            status.failed_requests += 1;
            status.last_error = Some("所有供应商暂时不可用（熔断器限制）".to_string());
            if status.total_requests > 0 {
                status.success_rate =
                    (status.success_requests as f32 / status.total_requests as f32) * 100.0;
            }
            return Err(ForwardError {
                error: ProxyError::NoAvailableProvider,
                provider: None,
            });
        }

        {
            let mut status = self.status.write().await;
            status.failed_requests += 1;
            status.last_error = Some("所有供应商都失败".to_string());
            if status.total_requests > 0 {
                status.success_rate =
                    (status.success_requests as f32 / status.total_requests as f32) * 100.0;
            }
        }

        if let Some((log_code, log_message)) =
            build_terminal_failure_log(attempted_providers, providers.len(), last_error.as_ref())
        {
            log::warn!("[{app_type_str}] [{log_code}] {log_message}");
        }

        Err(ForwardError {
            error: last_error.unwrap_or(ProxyError::MaxRetriesExceeded),
            provider: last_provider,
        })
    }

    fn effective_attempt_timeout(
        &self,
        policy: &ProviderRetryPolicy,
        request_is_streaming: bool,
    ) -> std::time::Duration {
        let provider_timeout = std::time::Duration::from_secs(policy.per_attempt_timeout_seconds);
        let app_timeout = if request_is_streaming {
            self.streaming_first_byte_timeout
        } else {
            self.non_streaming_timeout
        };
        match (provider_timeout.is_zero(), app_timeout.is_zero()) {
            (true, true) => std::time::Duration::ZERO,
            (true, false) => app_timeout,
            (false, true) => provider_timeout,
            (false, false) => provider_timeout.min(app_timeout),
        }
    }
    fn attempt_deadline(timeout: std::time::Duration) -> Option<tokio::time::Instant> {
        (!timeout.is_zero()).then(|| tokio::time::Instant::now() + timeout)
    }

    fn remaining_attempt_time(
        deadline: Option<tokio::time::Instant>,
    ) -> Option<std::time::Duration> {
        deadline.map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()))
    }
    fn provider_attempt_indices(policy: ProviderRetryPolicy) -> impl Iterator<Item = usize> {
        let ordinary_budget = (policy.retry_count as usize).saturating_add(1);
        std::iter::successors(Some(0usize), move |attempt_index| {
            if policy.unlimited_retries {
                Some(attempt_index.saturating_add(1))
            } else if attempt_index.saturating_add(1) < ordinary_budget {
                Some(attempt_index.saturating_add(1))
            } else {
                None
            }
        })
    }

    async fn wait_before_retry(retry_wait_seconds: u64) {
        if retry_wait_seconds > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(retry_wait_seconds)).await;
        }
    }

    fn categorize_same_provider_error(&self, error: &ProxyError) -> ErrorCategory {
        match error {
            ProxyError::Timeout(_) => ErrorCategory::Retryable,
            ProxyError::ForwardFailed(message) if is_connect_failure_message(message) => {
                ErrorCategory::Retryable
            }
            ProxyError::UpstreamError { status, .. } if (500..=599).contains(status) => {
                ErrorCategory::Retryable
            }
            _ => ErrorCategory::NonRetryable,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_provider_slot(
        &self,
        app_type: &AppType,
        method: http::Method,
        endpoint: &str,
        body: &Value,
        headers: &axum::http::HeaderMap,
        extensions: &Extensions,
        provider: &Provider,
        adapter: &dyn ProviderAdapter,
        policy: ProviderRetryPolicy,
        permit: &mut ProviderAttemptPermit,
    ) -> Result<ProviderForwardedResponse, ProviderSlotFailure> {
        let mut state = ProviderSlotState::SlotStart;
        log::trace!(
            "[{}] retry state={state:?}, provider_id={}",
            app_type.as_str(),
            provider.id
        );
        let ordinary_budget = (policy.retry_count as usize).saturating_add(1);
        let policy_enabled = policy.enabled();
        let mut provider_body = if self.optimizer_config.enabled && is_bedrock_provider(provider) {
            let mut optimized = body.clone();
            if self.optimizer_config.thinking_optimizer {
                super::thinking_optimizer::optimize(&mut optimized, &self.optimizer_config);
            }
            if self.optimizer_config.cache_injection {
                super::cache_injector::inject(&mut optimized, &self.optimizer_config);
            }
            optimized
        } else {
            body.clone()
        };
        self.apply_media_prevention(&mut provider_body, provider);

        let mut rectifier_retried = false;
        let mut budget_rectifier_retried = false;
        let mut media_rectifier_retried = false;
        let mut pinned = ProviderExecutionPin::default();

        let attempt_indices = Self::provider_attempt_indices(policy);
        for attempt_index in attempt_indices {
            state = ProviderSlotState::OrdinaryAttempt(attempt_index);
            log::trace!(
                "[{}] retry state={state:?}, provider_id={}, policy_retry_count={}",
                app_type.as_str(),
                provider.id,
                policy.retry_count
            );
            let request_is_streaming = is_streaming_request(endpoint, &provider_body, headers);
            let attempt_timeout = self.effective_attempt_timeout(&policy, request_is_streaming);
            let attempt_started = tokio::time::Instant::now();

            let result = self
                .forward(
                    app_type,
                    &method,
                    provider,
                    endpoint,
                    &provider_body,
                    headers,
                    extensions,
                    adapter,
                    &mut pinned,
                    attempt_timeout,
                    policy_enabled,
                )
                .await;

            let error = match result {
                Ok((response, claude_api_format, outbound_model)) => {
                    log::debug!(
                        "[{}] provider_attempt provider_id={}, phase=ordinary, attempt={}, outcome=success, elapsed_ms={}",
                        app_type.as_str(),
                        provider.id,
                        attempt_index,
                        attempt_started.elapsed().as_millis()
                    );
                    state = ProviderSlotState::Complete;
                    log::trace!(
                        "[{}] retry state={state:?}, provider_id={}, attempt={}",
                        app_type.as_str(),
                        provider.id,
                        attempt_index
                    );
                    permit.record_success(self).await;
                    return Ok(ProviderForwardedResponse {
                        response,
                        claude_api_format,
                        outbound_model,
                    });
                }
                Err(error) => {
                    log::debug!(
                        "[{}] provider_attempt provider_id={}, phase=ordinary, attempt={}, outcome=error, same_provider_category={:?}, elapsed_ms={}",
                        app_type.as_str(),
                        provider.id,
                        attempt_index,
                        self.categorize_same_provider_error(&error),
                        attempt_started.elapsed().as_millis()
                    );
                    error
                }
            };

            state = ProviderSlotState::RepairNeeded;
            log::trace!(
                "[{}] retry state={state:?}, provider_id={}, attempt={}",
                app_type.as_str(),
                provider.id,
                attempt_index
            );
            let repair = self
                .try_provider_repairs(
                    app_type,
                    &method,
                    provider,
                    endpoint,
                    &mut provider_body,
                    headers,
                    extensions,
                    adapter,
                    &error,
                    &mut rectifier_retried,
                    &mut budget_rectifier_retried,
                    &mut media_rectifier_retried,
                    &mut pinned,
                    attempt_timeout,
                    policy_enabled,
                )
                .await;

            let error = match repair {
                ProviderRepairOutcome::NotNeeded => error,
                ProviderRepairOutcome::Success(forwarded) => {
                    state = ProviderSlotState::Complete;
                    log::trace!(
                        "[{}] retry state={state:?}, provider_id={}, repair_success=true",
                        app_type.as_str(),
                        provider.id
                    );
                    permit.record_success(self).await;
                    return Ok(forwarded);
                }
                ProviderRepairOutcome::Failed {
                    error,
                    allow_failover,
                } => {
                    if !allow_failover {
                        permit.release_neutral().await;
                        return Err(ProviderSlotFailure {
                            error,
                            allow_failover: false,
                        });
                    }
                    error
                }
                ProviderRepairOutcome::FailedOriginal { allow_failover } => {
                    if !allow_failover {
                        permit.release_neutral().await;
                        return Err(ProviderSlotFailure {
                            error,
                            allow_failover: false,
                        });
                    }
                    error
                }
            };

            if self.categorize_same_provider_error(&error) == ErrorCategory::Retryable
                && (policy.unlimited_retries || attempt_index.saturating_add(1) < ordinary_budget)
            {
                state = ProviderSlotState::Retryable;
                log::trace!(
                    "[{}] retry state={state:?}, provider_id={}, attempt={}, wait_seconds={}",
                    app_type.as_str(),
                    provider.id,
                    attempt_index,
                    policy.retry_wait_seconds
                );
                Self::wait_before_retry(policy.retry_wait_seconds).await;
                continue;
            }

            let outer_retryable =
                self.categorize_proxy_error(&error, provider) == ErrorCategory::Retryable;
            match self.categorize_same_provider_error(&error) {
                ErrorCategory::Retryable => {
                    if outer_retryable {
                        permit.record_failure(&error).await;
                    } else {
                        permit.release_neutral().await;
                    }
                    return Err(ProviderSlotFailure {
                        error,
                        allow_failover: true,
                    });
                }
                ErrorCategory::NonRetryable | ErrorCategory::ClientAbort => {
                    if outer_retryable {
                        permit.record_failure(&error).await;
                    } else {
                        permit.release_neutral().await;
                    }
                    return Err(ProviderSlotFailure {
                        error,
                        allow_failover: true,
                    });
                }
            }
        }

        let error = ProxyError::MaxRetriesExceeded;
        permit.record_failure(&error).await;
        Err(ProviderSlotFailure {
            error,
            allow_failover: true,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_provider_repairs(
        &self,
        app_type: &AppType,
        method: &http::Method,
        provider: &Provider,
        endpoint: &str,
        provider_body: &mut Value,
        headers: &axum::http::HeaderMap,
        extensions: &Extensions,
        adapter: &dyn ProviderAdapter,
        error: &ProxyError,
        rectifier_retried: &mut bool,
        budget_rectifier_retried: &mut bool,
        media_rectifier_retried: &mut bool,
        pinned: &mut ProviderExecutionPin,
        attempt_timeout: std::time::Duration,
        policy_enabled: bool,
    ) -> ProviderRepairOutcome {
        let provider_type = ProviderType::from_app_type_and_config(app_type, provider);
        let is_anthropic_provider = matches!(
            provider_type,
            Some(ProviderType::Claude | ProviderType::ClaudeAuth)
        );

        if self.media_retry_should_trigger(
            adapter.name(),
            *media_rectifier_retried,
            provider_body,
            error,
        ) {
            let mut media_body = provider_body.clone();
            let replaced_images =
                super::media_sanitizer::replace_image_blocks_with_marker(&mut media_body);
            if replaced_images > 0 {
                *media_rectifier_retried = true;
                *provider_body = media_body.clone();
                match self
                    .forward(
                        app_type,
                        method,
                        provider,
                        endpoint,
                        &media_body,
                        headers,
                        extensions,
                        adapter,
                        pinned,
                        attempt_timeout,
                        policy_enabled,
                    )
                    .await
                {
                    Ok((response, claude_api_format, outbound_model)) => {
                        return ProviderRepairOutcome::Success(ProviderForwardedResponse {
                            response,
                            claude_api_format,
                            outbound_model,
                        });
                    }
                    Err(retry_error) => {
                        return ProviderRepairOutcome::Failed {
                            allow_failover: self.categorize_proxy_error(&retry_error, provider)
                                == ErrorCategory::Retryable,
                            error: retry_error,
                        };
                    }
                }
            }
        }

        if is_anthropic_provider {
            let error_message = extract_error_message(error);
            if should_rectify_thinking_signature(error_message.as_deref(), &self.rectifier_config) {
                if *rectifier_retried {
                    return ProviderRepairOutcome::FailedOriginal {
                        allow_failover: false,
                    };
                }

                let rectified = rectify_anthropic_request(provider_body);
                if rectified.applied {
                    *rectifier_retried = true;
                    match self
                        .forward(
                            app_type,
                            method,
                            provider,
                            endpoint,
                            provider_body,
                            headers,
                            extensions,
                            adapter,
                            pinned,
                            attempt_timeout,
                            policy_enabled,
                        )
                        .await
                    {
                        Ok((response, claude_api_format, outbound_model)) => {
                            return ProviderRepairOutcome::Success(ProviderForwardedResponse {
                                response,
                                claude_api_format,
                                outbound_model,
                            });
                        }
                        Err(retry_error) => {
                            return ProviderRepairOutcome::Failed {
                                allow_failover: self.categorize_proxy_error(&retry_error, provider)
                                    == ErrorCategory::Retryable,
                                error: retry_error,
                            };
                        }
                    }
                }
            }

            if should_rectify_thinking_budget(error_message.as_deref(), &self.rectifier_config) {
                if *budget_rectifier_retried {
                    return ProviderRepairOutcome::FailedOriginal {
                        allow_failover: false,
                    };
                }
                let rectified = rectify_thinking_budget(provider_body);
                if rectified.applied {
                    *budget_rectifier_retried = true;
                    match self
                        .forward(
                            app_type,
                            method,
                            provider,
                            endpoint,
                            provider_body,
                            headers,
                            extensions,
                            adapter,
                            pinned,
                            attempt_timeout,
                            policy_enabled,
                        )
                        .await
                    {
                        Ok((response, claude_api_format, outbound_model)) => {
                            return ProviderRepairOutcome::Success(ProviderForwardedResponse {
                                response,
                                claude_api_format,
                                outbound_model,
                            });
                        }
                        Err(retry_error) => {
                            return ProviderRepairOutcome::Failed {
                                allow_failover: self.categorize_proxy_error(&retry_error, provider)
                                    == ErrorCategory::Retryable,
                                error: retry_error,
                            };
                        }
                    }
                }
            }

            // A matching signature/budget error with no applicable repair is a
            // client/configuration error, preserving the old one-shot behavior.
            if should_rectify_thinking_signature(error_message.as_deref(), &self.rectifier_config)
                || should_rectify_thinking_budget(error_message.as_deref(), &self.rectifier_config)
            {
                return ProviderRepairOutcome::FailedOriginal {
                    allow_failover: false,
                };
            }
        }

        ProviderRepairOutcome::NotNeeded
    }

    fn record_failed_attempt_usage(
        &self,
        app_type: &AppType,
        provider: &Provider,
        request_model: &str,
        error: &ProxyError,
        latency_ms: u64,
        is_streaming: bool,
    ) {
        if !self.usage_logging_enabled {
            return;
        }
        let Some(usage) = parse_failed_attempt_usage(error) else {
            return;
        };
        let model = usage
            .model
            .clone()
            .filter(|model| !model.trim().is_empty())
            .unwrap_or_else(|| request_model.to_string());
        let provider_type = ProviderType::from_app_type_and_config(app_type, provider)
            .map(|provider_type| provider_type.as_str().to_string());
        let session_id = (!self.session_id.is_empty()).then(|| self.session_id.clone());
        let logger = UsageLogger::new(&self.db);
        if let Err(error) = logger.log_error_with_usage_context(
            format!("provider-attempt:{}", uuid::Uuid::new_v4()),
            provider.id.clone(),
            app_type.as_str().to_string(),
            model,
            request_model.to_string(),
            usage,
            super::error_mapper::map_proxy_error_to_status(error),
            summarize_proxy_error(error),
            latency_ms,
            is_streaming,
            session_id,
            provider_type,
        ) {
            log::warn!(
                "[{}] 记录失败 Provider 尝试的 token usage 失败: provider_id={}, error={error}",
                app_type.as_str(),
                provider.id
            );
        }
    }

    /// 转发单次上游尝试，并在失败响应携带 usage 时记录实际 token 消耗。
    #[allow(clippy::too_many_arguments)]
    async fn forward(
        &self,
        app_type: &AppType,
        method: &http::Method,
        provider: &Provider,
        endpoint: &str,
        body: &Value,
        headers: &axum::http::HeaderMap,
        extensions: &Extensions,
        adapter: &dyn ProviderAdapter,
        pinned: &mut ProviderExecutionPin,
        attempt_timeout: std::time::Duration,
        policy_enabled: bool,
    ) -> Result<(ProxyResponse, Option<String>, Option<String>), ProxyError> {
        let started_at = tokio::time::Instant::now();
        let is_streaming = is_streaming_request(endpoint, body, headers);
        let request_model = body
            .get("model")
            .and_then(Value::as_str)
            .filter(|model| !model.trim().is_empty())
            .unwrap_or("unknown");
        let result = self
            .forward_once(
                app_type,
                method,
                provider,
                endpoint,
                body,
                headers,
                extensions,
                adapter,
                pinned,
                attempt_timeout,
                policy_enabled,
            )
            .await;
        if let Err(error) = &result {
            self.record_failed_attempt_usage(
                app_type,
                provider,
                request_model,
                error,
                started_at.elapsed().as_millis() as u64,
                is_streaming,
            );
        }
        result
    }

    /// 转发单个请求（使用适配器）
    ///
    /// 成功时返回 `(response, claude_api_format, outbound_model)`，其中
    /// `outbound_model` 是最终发往上游的模型名（所有映射/改写之后）。
    #[allow(clippy::too_many_arguments)]
    async fn forward_once(
        &self,
        app_type: &AppType,
        method: &http::Method,
        provider: &Provider,
        endpoint: &str,
        body: &Value,
        headers: &axum::http::HeaderMap,
        extensions: &Extensions,
        adapter: &dyn ProviderAdapter,
        pinned: &mut ProviderExecutionPin,
        attempt_timeout: std::time::Duration,
        policy_enabled: bool,
    ) -> Result<(ProxyResponse, Option<String>, Option<String>), ProxyError> {
        let deadline = Self::attempt_deadline(attempt_timeout);
        // Resolve the endpoint once per provider slot. In particular, Copilot's
        // account-specific endpoint must not change between ordinary retries or
        // one-shot repairs.
        let endpoint_pinned = pinned.base_url.is_some();
        let mut base_url = pinned
            .base_url
            .clone()
            .unwrap_or(adapter.extract_base_url(provider)?);
        let is_full_url = provider
            .meta
            .as_ref()
            .and_then(|meta| meta.is_full_url)
            .unwrap_or(false)
            && !provider.is_codex_oauth()
            && !provider.is_xai_oauth();

        // GitHub Copilot API 使用 /chat/completions（无 /v1 前缀）
        let is_copilot = provider
            .meta
            .as_ref()
            .and_then(|m| m.provider_type.as_deref())
            == Some("github_copilot")
            || base_url.contains("githubcopilot.com");

        // Codex upstream conversion mode — computed early because the [1m]-suffix strip
        // below must be skipped on the Anthropic path (the marker has to survive to
        // catalog matching and to the transform's own strip+beta detection).
        let codex_responses_to_chat = matches!(app_type, AppType::Codex | AppType::GrokBuild)
            && super::providers::should_convert_codex_responses_to_chat(provider, endpoint);
        let codex_responses_to_anthropic = matches!(app_type, AppType::Codex | AppType::GrokBuild)
            && super::providers::should_convert_codex_responses_to_anthropic(provider, endpoint);
        let codex_official_auth_passthrough = matches!(app_type, AppType::Codex)
            && super::providers::is_codex_official_provider(provider);

        if codex_official_auth_passthrough {
            validate_codex_official_authorization(headers, provider)?;
        }

        // 应用模型映射（独立于格式转换）
        // Claude Desktop proxy 模式必须先把 Desktop 可见的 claude-* route
        // 映射成真实上游模型名，并且未知 route 要直接报错，不能使用默认模型兜底。
        let mapped_body = if matches!(app_type, AppType::ClaudeDesktop) {
            crate::claude_desktop_config::map_proxy_request_model(body.clone(), provider)
                .map_err(|e| ProxyError::InvalidRequest(e.to_string()))?
        } else {
            let (mapped_body, _original_model, _mapped_model) =
                super::model_mapper::apply_model_mapping(body.clone(), provider);
            mapped_body
        };

        // 与 CCH 对齐：请求前不做 thinking 主动改写（仅保留兼容入口）
        let mut mapped_body = normalize_thinking_type(mapped_body);

        // Grok Build exposes a stable client-side model profile in config.toml.
        // Route requests to the provider's real upstream model before applying
        // the optional Responses -> Chat/Anthropic bridge.
        if matches!(app_type, AppType::GrokBuild) {
            super::providers::apply_codex_upstream_model(provider, &mut mapped_body);
        }

        if is_copilot {
            mapped_body =
                super::providers::copilot_model_map::apply_copilot_model_normalization(mapped_body);
            self.apply_copilot_live_model_resolution(provider, &mut mapped_body)
                .await;
            // Strip the [1M] context marker after Copilot normalization/resolve.
            // A user's mapped value (e.g. "gpt-5.6-sol[1M]") carries [1M] as a
            // Claude Code context-capability declaration that upstream APIs reject
            // as part of the model name. The preceding normalization step already
            // rewrites claude-xxx[1M] into the "-1m" dash form Copilot accepts, and
            // the strip helper only touches the "[1m]" bracket form, so "-1m"
            // variants pass through unchanged.
            mapped_body =
                super::model_mapper::strip_one_m_suffix_for_upstream_from_body(mapped_body);
        } else if !codex_responses_to_anthropic {
            // Skip on the Codex→Anthropic path: stripping [1m] here would break both the
            // model-catalog match (apply_codex_upstream_model) and the transform's own
            // strip+`context-1m` beta detection. The marker is stripped later, on the
            // final anthropic_body.
            mapped_body =
                super::model_mapper::strip_one_m_suffix_for_upstream_from_body(mapped_body);
        }

        // --- Copilot 优化器：分类 + 请求体优化（在格式转换之前执行） ---
        // 注意：确定性 ID 也在此处计算，因为 mapped_body 在格式转换时会被 move
        //
        // 执行顺序（与 copilot-api 对齐）：
        //   1. 先在原始 body 上分类（保留 tool_result 语义，避免误判为 user）
        //   2. 再清洗孤立 tool_result（防止上游 API 报错）
        //   3. 再合并 tool_result + text（减少 premium 计费）
        let copilot_optimization = if is_copilot && self.copilot_optimizer_config.enabled {
            // 1. 在原始 body 上分类 — 必须在清洗/合并之前执行
            //    孤立 tool_result 仍保持 tool_result 类型，分类能正确识别为 agent
            let has_anthropic_beta = headers.contains_key("anthropic-beta");
            let classification = super::copilot_optimizer::classify_request(
                &mapped_body,
                has_anthropic_beta,
                self.copilot_optimizer_config.compact_detection,
                self.copilot_optimizer_config.subagent_detection,
            );

            log::debug!(
                "[Copilot] 优化器分类: initiator={}, is_warmup={}, is_compact={}, is_subagent={}",
                classification.initiator,
                classification.is_warmup,
                classification.is_compact,
                classification.is_subagent
            );

            // 2. 孤立 tool_result 清理 — 分类完成后再清洗
            //    防止上游 API 因不匹配的 tool_result 报错导致重试/重复计费
            mapped_body = super::copilot_optimizer::sanitize_orphan_tool_results(mapped_body);

            // 3. Tool result 合并 — 将 [tool_result, text] 变为 [tool_result(含text)]
            if self.copilot_optimizer_config.tool_result_merging {
                mapped_body = super::copilot_optimizer::merge_tool_results(mapped_body);
            }

            // 3.5. 主动剥离 thinking block — Copilot 走 OpenAI 兼容端点不识别该块
            //      避免上游拒绝后由 rectifier 反应式重试（首次请求已消耗 quota）
            if self.copilot_optimizer_config.strip_thinking {
                mapped_body = super::copilot_optimizer::strip_thinking_blocks(mapped_body);
            }

            // 4. Warmup 小模型降级
            if self.copilot_optimizer_config.warmup_downgrade && classification.is_warmup {
                log::info!(
                    "[Copilot] Warmup 请求降级到模型: {}",
                    self.copilot_optimizer_config.warmup_model
                );
                mapped_body["model"] =
                    serde_json::json!(&self.copilot_optimizer_config.warmup_model);
            }

            // 预计算确定性 Request ID（在 body 被 move 之前）
            // Session 提取优先级（与 session.rs extract_from_metadata 对齐）：
            //   1. metadata.user_id 中的 _session_ 后缀
            //   2. metadata.session_id（直接字段）
            //   3. raw metadata.user_id（整串 fallback）
            //   4. x-session-id header
            let metadata = body.get("metadata");
            let session_id = metadata
                .and_then(|m| m.get("user_id"))
                .and_then(|v| v.as_str())
                .and_then(super::session::parse_session_from_user_id)
                .or_else(|| {
                    metadata
                        .and_then(|m| m.get("session_id"))
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                })
                .or_else(|| {
                    metadata
                        .and_then(|m| m.get("user_id"))
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                })
                .or_else(|| {
                    headers
                        .get("x-session-id")
                        .and_then(|v| v.to_str().ok())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                })
                .unwrap_or_default();
            let det_request_id = if self.copilot_optimizer_config.deterministic_request_id {
                Some(super::copilot_optimizer::deterministic_request_id(
                    &mapped_body,
                    &session_id,
                ))
            } else {
                None
            };

            // 从 session ID 派生稳定的 interaction ID（同一主对话共享）
            let interaction_id =
                super::copilot_optimizer::deterministic_interaction_id(&session_id);

            Some((classification, det_request_id, interaction_id))
        } else {
            None
        };

        // GitHub Copilot 动态 endpoint 路由
        // 从 CopilotAuthManager 获取缓存的 API endpoint（支持企业版等非默认 endpoint）
        if is_copilot && !is_full_url && !endpoint_pinned {
            if let Some(app_handle) = &self.app_handle {
                let copilot_state = app_handle.state::<CopilotAuthState>();
                let copilot_auth = copilot_state.0.read().await;

                // 从 provider.meta 获取关联的 GitHub 账号 ID
                let account_id = provider
                    .meta
                    .as_ref()
                    .and_then(|m| m.managed_account_id_for("github_copilot"));

                let dynamic_endpoint = match &account_id {
                    Some(id) => copilot_auth.get_api_endpoint(id).await,
                    None => copilot_auth.get_default_api_endpoint().await,
                };

                // 只在动态 endpoint 与当前 base_url 不同时替换
                if dynamic_endpoint != base_url {
                    log::debug!(
                        "[Copilot] 使用动态 API endpoint: {} (原: {})",
                        dynamic_endpoint,
                        base_url
                    );
                    base_url = dynamic_endpoint;
                }
            }
        }
        if pinned.base_url.is_none() {
            pinned.base_url = Some(base_url.clone());
        }
        let resolved_claude_api_format = if adapter.name() == "Claude" {
            Some(
                self.resolve_claude_api_format(provider, &mapped_body, is_copilot)
                    .await,
            )
        } else {
            None
        };
        if adapter.name() == "Claude" {
            if let Some(api_format) = resolved_claude_api_format.as_deref() {
                super::providers::normalize_anthropic_messages_for_provider(
                    &mut mapped_body,
                    provider,
                    api_format,
                );
                self.apply_media_prevention(&mut mapped_body, provider);
            }
        }
        let needs_transform = match resolved_claude_api_format.as_deref() {
            Some(api_format) => super::providers::claude_api_format_needs_transform(api_format),
            None => adapter.needs_transform(provider),
        };
        // Codex → Anthropic: Claude Code emulation is off by default and only
        // enabled when the user explicitly turns it on in the UI, so requests can
        // pass a gateway's "Claude Code only" fingerprint check (User-Agent /
        // anthropic-beta / x-app / system prompt first line). Defaulting to off
        // avoids leaking the Claude Code fingerprint and identity prompt to
        // general-purpose gateways.
        let codex_impersonate_claude_code = codex_responses_to_anthropic
            && provider
                .meta
                .as_ref()
                .and_then(|meta| meta.impersonate_claude_code)
                == Some(true);
        let (effective_endpoint, passthrough_query) = if codex_responses_to_chat {
            rewrite_codex_responses_endpoint_to_chat(endpoint)
        } else if codex_responses_to_anthropic {
            rewrite_codex_responses_endpoint_to_anthropic(endpoint)
        } else if needs_transform && adapter.name() == "Claude" {
            let api_format = resolved_claude_api_format
                .as_deref()
                .unwrap_or_else(|| super::providers::get_claude_api_format(provider));
            rewrite_claude_transform_endpoint(endpoint, api_format, is_copilot, &mapped_body)
        } else {
            (
                endpoint.to_string(),
                split_endpoint_and_query(endpoint)
                    .1
                    .map(ToString::to_string),
            )
        };

        let codex_chat_base_is_full_endpoint =
            codex_responses_to_chat && base_url_is_full_endpoint(&base_url, "/chat/completions");

        // Defensive fallback mirroring `codex_chat_base_is_full_endpoint`: if a user pastes
        // a base URL already ending in the Anthropic `/v1/messages` endpoint but leaves the
        // "full URL" switch off, treat it as a full endpoint so we don't double-append
        // `/v1/messages` (→ `.../v1/messages/v1/messages`, a non-retryable 400). Matches the
        // exact endpoint suffix, so prefixed gateways like `.../api/v1/messages` are covered.
        let codex_anthropic_base_is_full_endpoint =
            codex_responses_to_anthropic && base_url_is_full_endpoint(&base_url, "/v1/messages");

        let url = if matches!(resolved_claude_api_format.as_deref(), Some("gemini_native")) {
            super::gemini_url::resolve_gemini_native_url(
                &base_url,
                &effective_endpoint,
                is_full_url,
            )
        } else if is_full_url
            || codex_chat_base_is_full_endpoint
            || codex_anthropic_base_is_full_endpoint
        {
            append_query_to_full_url(&base_url, passthrough_query.as_deref())
        } else {
            adapter.build_url(&base_url, &effective_endpoint)
        };

        // 记录映射后的出站模型名（此时 mapped_body 已完成接管映射 / [1m] 剥离 /
        // Copilot 归一化）。格式转换后若 body 仍带 model 字段会在下方刷新覆盖；
        // gemini_native 等模型在 URL 中的格式则保留此处的转换前真值。
        let mut outbound_model = mapped_body
            .get("model")
            .and_then(|m| m.as_str())
            .filter(|m| !m.is_empty())
            .map(str::to_string);

        // Codex→Anthropic: when the model name carries the [1m] marker, strip the
        // suffix and add the context-1m beta header.
        let mut codex_anthropic_one_m = false;

        // 转换请求体（如果需要）
        let mut request_body = if codex_responses_to_chat {
            let mut mapped_body = mapped_body;
            let explicit_prompt_cache_key = mapped_body
                .get("prompt_cache_key")
                .and_then(|value| value.as_str())
                .map(ToString::to_string);
            let restored = self
                .codex_chat_history
                .enrich_request(&mut mapped_body)
                .await;
            if restored > 0 {
                log::debug!(
                    "[Codex] Restored or enriched {restored} cached function call item(s) for Chat upstream"
                );
            }
            super::providers::apply_codex_chat_upstream_model(provider, &mut mapped_body);
            let reasoning_config =
                super::providers::resolve_codex_chat_reasoning_config(provider, &mapped_body);
            let mut chat_body = super::providers::transform_codex_chat::responses_to_chat_completions_with_reasoning(
                mapped_body,
                reasoning_config.as_ref(),
            )?;
            super::providers::inject_codex_chat_prompt_cache_key(
                provider,
                &mut chat_body,
                explicit_prompt_cache_key.as_deref(),
                self.session_client_provided
                    .then_some(self.session_id.as_str()),
            );
            chat_body
        } else if codex_responses_to_anthropic {
            let mut mapped_body = mapped_body;
            super::providers::apply_codex_upstream_model(provider, &mut mapped_body);
            // Per-provider output ceiling override. Codex does not forward its
            // `model_max_output_tokens` in the request body, so honor the value
            // configured on the provider here — it takes precedence over any
            // request-supplied `max_output_tokens` and over the default below.
            // Injecting it into the body (rather than overriding after transform)
            // lets the thinking-budget clamp size its headroom against the real
            // ceiling too. Kept per-provider to avoid a global large default that
            // would 400 on low-output-ceiling gateways.
            if let Some(max_out) = provider
                .meta
                .as_ref()
                .and_then(|meta| meta.max_output_tokens)
                .filter(|v| *v > 0)
            {
                mapped_body["max_output_tokens"] = Value::from(max_out);
            }
            // Anthropic requires max_tokens; fall back to this default only when the
            // Codex request omits max_output_tokens (rare — Codex normally sends it).
            // Kept conservative so a low-output-ceiling model or relay does not hard-400
            // on the fallback (a too-high default 400s and is non-retryable); 8192 is
            // accepted by every current Claude model and virtually all gateways. The
            // transform clamps any thinking budget below this value.
            const DEFAULT_CODEX_ANTHROPIC_MAX_TOKENS: u64 = 8192;
            let mut anthropic_body =
                super::providers::transform_codex_anthropic::responses_request_to_anthropic(
                    mapped_body,
                    DEFAULT_CODEX_ANTHROPIC_MAX_TOKENS,
                )?;
            // Handle the 1M-context marker [1m]: strip the model-name suffix (the
            // gateway doesn't recognize it) and set the flag so the beta header is
            // added. apply_codex_upstream_model may have just written back a model
            // name carrying [1m] from the provider config, so strip it once more on
            // the final body here.
            if let Some(model) = anthropic_body.get("model").and_then(|v| v.as_str()) {
                let stripped = super::model_mapper::strip_one_m_suffix_for_upstream(model);
                if stripped != model {
                    codex_anthropic_one_m = true;
                    anthropic_body["model"] = Value::String(stripped.to_string());
                }
            }
            if codex_impersonate_claude_code {
                prepend_claude_code_system_prompt(&mut anthropic_body);
            }
            // Enable Anthropic prompt caching (no beta header required). Reuse the
            // configured TTL rather than silently forcing 5m on this conversion path.
            // otherwise system/tools/history are re-sent at full price every round,
            // inflating cost and first-token latency. The injector handles the
            // string→array `system` conversion and the new-breakpoint budget.
            super::cache_injector::inject(
                &mut anthropic_body,
                &codex_anthropic_cache_config(&self.optimizer_config),
            );
            anthropic_body
        } else if needs_transform {
            if adapter.name() == "Claude" {
                let api_format = resolved_claude_api_format
                    .as_deref()
                    .unwrap_or_else(|| super::providers::get_claude_api_format(provider));
                super::providers::transform_claude_request_for_api_format(
                    mapped_body,
                    provider,
                    api_format,
                    self.session_client_provided
                        .then_some(self.session_id.as_str()),
                    Some(self.gemini_shadow.as_ref()),
                )?
            } else {
                adapter.transform_request(mapped_body, provider)?
            }
        } else {
            mapped_body
        };

        // Native Responses passthrough to a strict third-party gateway (xAI):
        // flatten Codex's private `namespace`/plugin tool declarations into
        // top-level function tools so the upstream's strict serde parser does
        // not 422 on `unknown variant "namespace"`. The Chat/Anthropic paths
        // above already unwrap namespaces, so this only fires on the native
        // passthrough. The response handler restores the flat names using a map
        // re-derived from the same request tools.
        if matches!(app_type, AppType::Codex | AppType::GrokBuild)
            && !codex_responses_to_chat
            && !codex_responses_to_anthropic
            && super::providers::provider_needs_responses_namespace_flatten(provider)
            && super::providers::transform_codex_responses_namespace::flatten_request_namespaces(
                &mut request_body,
            )?
        {
            log::debug!(
                "[Codex] Flattened namespace tools for native Responses upstream (provider={})",
                provider.id
            );
        }

        // Same native-Responses path: scrub the OpenAI-backend-private fields
        // and tool carriers (`external_web_access`, `prompt_cache_retention`,
        // `additional_tools`, `tool_search`, …) that xAI's strict serde parser
        // rejects with 400/422. Deterministic field removals only, gated on the
        // xAI OAuth path, so the prompt-cache prefix stays stable and no other
        // provider is affected. Runs after the flatten above so lifted
        // `namespace` tools survive the tool-type whitelist.
        if matches!(app_type, AppType::Codex | AppType::GrokBuild)
            && !codex_responses_to_chat
            && !codex_responses_to_anthropic
            && super::providers::provider_needs_responses_namespace_flatten(provider)
            && super::providers::transform_codex_responses_xai_sanitize::sanitize_xai_responses_request(
                &mut request_body,
            )
        {
            log::debug!(
                "[Codex] Sanitized xAI-unsupported Responses fields (provider={})",
                provider.id
            );
        }

        if matches!(app_type, AppType::Codex | AppType::GrokBuild) {
            self.apply_media_prevention(&mut request_body, provider);
        }

        // 过滤私有参数（以 `_` 开头的字段），防止内部信息泄露到上游
        // 默认使用空白名单，过滤所有 _ 前缀字段
        let mut filtered_body = prepare_upstream_request_body(request_body);
        if !is_copilot {
            if let Some(overrides) = provider
                .meta
                .as_ref()
                .and_then(|meta| meta.local_proxy_request_overrides.as_ref())
            {
                if apply_local_proxy_body_overrides(&mut filtered_body, overrides) {
                    filtered_body = prepare_upstream_request_body(filtered_body);
                }
            }
        }
        // 出站 body 定稿后刷新真值（覆盖 Codex chat 上游模型覆写、转换层模型改写）
        if let Some(m) = filtered_body
            .get("model")
            .and_then(|m| m.as_str())
            .filter(|m| !m.is_empty())
        {
            outbound_model = Some(m.to_string());
        }
        log_prompt_cache_trace(
            app_type,
            provider,
            &effective_endpoint,
            resolved_claude_api_format.as_deref(),
            &filtered_body,
            self.session_client_provided,
        );
        let request_is_streaming =
            is_streaming_request(&effective_endpoint, &filtered_body, headers);
        let force_identity_encoding = needs_transform
            || codex_responses_to_chat
            || codex_responses_to_anthropic
            || request_is_streaming;

        // Resolve credentials once per provider slot. OAuth managers may rotate their
        // default account or refresh a token while a request is in flight; re-reading
        // them during same-provider retries would violate the pinned execution context.
        let auth_was_pinned = pinned.auth_resolved;
        let auth_snapshot = if auth_was_pinned {
            pinned.auth.clone()
        } else {
            let auth = adapter.extract_auth(provider);
            pinned.auth = auth.clone();
            pinned.auth_resolved = true;
            auth
        };
        let mut codex_oauth_account_id = pinned.codex_oauth_account_id.clone();
        let mut should_send_codex_oauth_session_headers = false;

        // Keep exact authentication material only for in-memory URL/header redaction.
        // It is never included in logs.
        let mut log_secrets: Vec<String> = Vec::new();
        let mut auth_headers = if let Some(mut auth) = auth_snapshot {
            if !auth_was_pinned {
                // GitHub Copilot: resolve the selected managed account once.
                if auth.strategy == AuthStrategy::GitHubCopilot {
                    if let Some(app_handle) = &self.app_handle {
                        let copilot_state = app_handle.state::<CopilotAuthState>();
                        let copilot_auth: tokio::sync::RwLockReadGuard<'_, CopilotAuthManager> =
                            copilot_state.0.read().await;
                        let account_id = provider
                            .meta
                            .as_ref()
                            .and_then(|m| m.managed_account_id_for("github_copilot"));
                        let token_result = match &account_id {
                            Some(id) => {
                                log::debug!("[Copilot] 使用指定账号 {id} 获取 token");
                                copilot_auth.get_valid_token_for_account(id).await
                            }
                            None => {
                                log::debug!("[Copilot] 使用默认账号获取 token");
                                copilot_auth.get_valid_token().await
                            }
                        };
                        auth = AuthInfo::new(
                            token_result.map_err(|error| {
                                ProxyError::AuthError(format!("GitHub Copilot 认证失败: {error}"))
                            })?,
                            AuthStrategy::GitHubCopilot,
                        );
                        log::debug!(
                            "[Copilot] 成功获取 Copilot token (account={})",
                            account_id.as_deref().unwrap_or("default")
                        );
                    } else {
                        return Err(ProxyError::AuthError(
                            "GitHub Copilot 认证不可用（无 AppHandle）".to_string(),
                        ));
                    }
                }

                // Codex OAuth: resolve the selected managed account and token once.
                if auth.strategy == AuthStrategy::CodexOAuth {
                    if let Some(app_handle) = &self.app_handle {
                        let codex_state = app_handle.state::<CodexOAuthState>();
                        let codex_auth = &codex_state.0;
                        let account_id = provider
                            .meta
                            .as_ref()
                            .and_then(|m| m.managed_account_id_for("codex_oauth"));
                        let token_result = match &account_id {
                            Some(id) => {
                                log::debug!("[CodexOAuth] 使用指定账号 {id} 获取 token");
                                codex_auth.get_valid_token_for_account(id).await
                            }
                            None => {
                                log::debug!("[CodexOAuth] 使用默认账号获取 token");
                                codex_auth.get_valid_token().await
                            }
                        };
                        let token = token_result.map_err(|error| {
                            ProxyError::AuthError(format!("Codex OAuth 认证失败: {error}"))
                        })?;
                        auth = AuthInfo::new(token, AuthStrategy::CodexOAuth);
                        codex_oauth_account_id = match account_id {
                            Some(id) => Some(id),
                            None => codex_auth.default_account_id().await,
                        };
                        pinned.codex_oauth_account_id = codex_oauth_account_id.clone();
                        log::debug!(
                            "[CodexOAuth] 成功获取 access_token (account={})",
                            codex_oauth_account_id.as_deref().unwrap_or("default")
                        );
                    } else {
                        return Err(ProxyError::AuthError(
                            "Codex OAuth 认证不可用（无 AppHandle）".to_string(),
                        ));
                    }
                }

                // xAI OAuth: resolve the selected managed account once.
                if auth.strategy == AuthStrategy::XaiOAuth {
                    if let Some(app_handle) = &self.app_handle {
                        let xai_state = app_handle.state::<XaiOAuthState>();
                        let xai_auth: tokio::sync::RwLockReadGuard<'_, XaiOAuthManager> =
                            xai_state.0.read().await;
                        let account_id = provider
                            .meta
                            .as_ref()
                            .and_then(|meta| meta.managed_account_id_for("xai_oauth"));
                        let token_result = match &account_id {
                            Some(id) => xai_auth.get_valid_token_for_account(id).await,
                            None => xai_auth.get_valid_token().await,
                        };
                        auth = AuthInfo::new(
                            token_result.map_err(|error| {
                                ProxyError::AuthError(format!("xAI OAuth 认证失败: {error}"))
                            })?,
                            AuthStrategy::XaiOAuth,
                        );
                        log::debug!(
                            "[XaiOAuth] 成功获取 access_token (account={})",
                            account_id.as_deref().unwrap_or("default")
                        );
                    } else {
                        return Err(ProxyError::AuthError(
                            "xAI OAuth 认证不可用（无 AppHandle）".to_string(),
                        ));
                    }
                }
            }

            should_send_codex_oauth_session_headers = auth.strategy == AuthStrategy::CodexOAuth;
            pinned.auth = Some(auth.clone());
            for secret in std::iter::once(&auth.api_key).chain(auth.access_token.iter()) {
                if !secret.is_empty() && !log_secrets.contains(secret) {
                    log_secrets.push(secret.clone());
                }
            }
            adapter.get_auth_headers(&auth)?
        } else {
            pinned.auth = None;
            Vec::new()
        };

        // 注入 Codex OAuth 的 ChatGPT-Account-Id header（如果有 account_id）
        if let Some(ref account_id) = codex_oauth_account_id {
            if let Ok(hv) = http::HeaderValue::from_str(account_id) {
                auth_headers.push((http::HeaderName::from_static("chatgpt-account-id"), hv));
            }
        }

        let codex_oauth_session_headers =
            if should_send_codex_oauth_session_headers && self.session_client_provided {
                build_codex_oauth_session_headers(&self.session_id)
            } else {
                Vec::new()
            };

        // 自定义 User-Agent：与 stream_check / model_fetch 共用 parse_custom_user_agent，
        // 运行时静默忽略非法值（前端在输入处给非阻断提示，不在保存时阻断）。
        // Copilot 指纹 UA 不可覆盖。
        let custom_user_agent = if is_copilot {
            None
        } else {
            provider
                .meta
                .as_ref()
                .and_then(|meta| meta.custom_user_agent_header().ok().flatten())
        };
        // Codex→Anthropic emulation: when there is no custom UA, override Codex's
        // codex_cli_rs UA with the Claude Code UA.
        let custom_user_agent = if custom_user_agent.is_none() && codex_impersonate_claude_code {
            Some(http::HeaderValue::from_static(CLAUDE_CODE_USER_AGENT))
        } else {
            custom_user_agent
        };

        // --- Copilot 优化器：动态 header 注入 ---
        if let Some((ref classification, ref det_request_id, ref interaction_id)) =
            copilot_optimization
        {
            for (name, value) in auth_headers.iter_mut() {
                match name.as_str() {
                    "x-initiator" if self.copilot_optimizer_config.request_classification => {
                        *value = http::HeaderValue::from_static(classification.initiator);
                    }
                    "x-interaction-type" if classification.is_subagent => {
                        // 子代理请求：conversation-subagent 不计 premium interaction
                        *value = http::HeaderValue::from_static("conversation-subagent");
                    }
                    "x-request-id" | "x-agent-task-id" => {
                        if let Some(ref det_id) = det_request_id {
                            if let Ok(hv) = http::HeaderValue::from_str(det_id) {
                                *value = hv;
                            }
                        }
                    }
                    _ => {}
                }
            }

            // x-interaction-id：仅在有 session 时注入（不在 get_auth_headers 中）
            if let Some(ref iid) = interaction_id {
                if let Ok(hv) = http::HeaderValue::from_str(iid) {
                    auth_headers.push((http::HeaderName::from_static("x-interaction-id"), hv));
                }
            }

            if classification.is_subagent {
                log::info!(
                    "[Copilot] 子代理请求: x-initiator=agent, x-interaction-type=conversation-subagent"
                );
            }
        }

        // Copilot 指纹头名（由 get_auth_headers 注入，需在原始头中去重）
        let copilot_fingerprint_headers: &[&str] = if is_copilot {
            &[
                "user-agent",
                "editor-version",
                "editor-plugin-version",
                "copilot-integration-id",
                "x-github-api-version",
                "openai-intent",
                // 新增 headers
                "x-initiator",
                "x-interaction-type",
                "x-interaction-id",
                "x-vscode-user-agent-library-version",
                "x-request-id",
                "x-agent-task-id",
            ]
        } else {
            &[]
        };

        // 预计算上游 host 值（用于在原位替换 host header）
        let upstream_host = url
            .parse::<http::Uri>()
            .ok()
            .and_then(|u| u.authority().map(|a| a.to_string()));

        let should_send_anthropic_headers = adapter.name() == "Claude"
            && matches!(resolved_claude_api_format.as_deref(), Some("anthropic"));

        // 预计算 anthropic-beta 值（仅 Claude）
        let anthropic_beta_value = if should_send_anthropic_headers {
            const CLAUDE_CODE_BETA: &str = "claude-code-20250219";
            Some(if let Some(beta) = headers.get("anthropic-beta") {
                if let Ok(beta_str) = beta.to_str() {
                    if beta_str.contains(CLAUDE_CODE_BETA) {
                        beta_str.to_string()
                    } else {
                        format!("{CLAUDE_CODE_BETA},{beta_str}")
                    }
                } else {
                    CLAUDE_CODE_BETA.to_string()
                }
            } else {
                CLAUDE_CODE_BETA.to_string()
            })
        } else if codex_impersonate_claude_code || codex_anthropic_one_m {
            // Codex→Anthropic: emulation injects the claude-code marker; a [1m]
            // model injects the context-1m marker.
            let mut betas: Vec<&str> = Vec::new();
            if codex_impersonate_claude_code {
                betas.push("claude-code-20250219");
            }
            if codex_anthropic_one_m {
                betas.push("context-1m-2025-08-07");
            }
            Some(betas.join(","))
        } else {
            None
        };

        // ============================================================
        // 构建有序 HeaderMap — 内联替换，保持客户端原始顺序
        // ============================================================
        let mut ordered_headers = http::HeaderMap::new();
        let mut saw_auth = false;
        let mut saw_accept_encoding = false;
        let mut saw_accept = false;
        let mut saw_user_agent = false;
        let mut saw_anthropic_beta = false;
        let mut saw_anthropic_version = false;

        for (key, value) in headers {
            let key_str = key.as_str();

            // --- host — 原位替换为上游 host（保持客户端原始位置） ---
            if key_str.eq_ignore_ascii_case("host") {
                if let Some(ref host_val) = upstream_host {
                    if let Ok(hv) = http::HeaderValue::from_str(host_val) {
                        ordered_headers.append(key.clone(), hv);
                    }
                }
                continue;
            }

            // --- 连接 / 追踪 / CDN 类 — 无条件跳过 ---
            if matches!(
                key_str,
                "content-length"
                    | "transfer-encoding"
                    | "x-forwarded-host"
                    | "x-forwarded-port"
                    | "x-forwarded-proto"
                    | "forwarded"
                    | "cf-connecting-ip"
                    | "cf-ipcountry"
                    | "cf-ray"
                    | "cf-visitor"
                    | "true-client-ip"
                    | "fastly-client-ip"
                    | "x-azure-clientip"
                    | "x-azure-fdid"
                    | "x-azure-ref"
                    | "akamai-origin-hop"
                    | "x-akamai-config-log-detail"
                    | "x-request-id"
                    | "x-correlation-id"
                    | "x-trace-id"
                    | "x-amzn-trace-id"
                    | "x-b3-traceid"
                    | "x-b3-spanid"
                    | "x-b3-parentspanid"
                    | "x-b3-sampled"
                    | "traceparent"
                    | "tracestate"
            ) {
                continue;
            }

            // --- 认证类 — 用 adapter 提供的认证头替换（在原始位置） ---
            if key_str.eq_ignore_ascii_case("authorization")
                || key_str.eq_ignore_ascii_case("x-api-key")
                || key_str.eq_ignore_ascii_case("x-goog-api-key")
            {
                // Codex official account cards deliberately keep credentials
                // out of provider storage. `requires_openai_auth = true` makes
                // Codex send the active ChatGPT authorization, which must reach
                // the official upstream unchanged. Other credential headers
                // are still discarded.
                if codex_official_auth_passthrough && key_str.eq_ignore_ascii_case("authorization")
                {
                    saw_auth = true;
                    ordered_headers.append(key.clone(), value.clone());
                    continue;
                }
                if !saw_auth {
                    saw_auth = true;
                    for (ah_name, ah_value) in &auth_headers {
                        ordered_headers.append(ah_name.clone(), ah_value.clone());
                    }
                }
                continue;
            }

            // --- x-app — during Codex→Anthropic emulation, `cli` is injected uniformly below ---
            if codex_impersonate_claude_code && key_str.eq_ignore_ascii_case("x-app") {
                continue;
            }

            // --- Codex/OpenAI fingerprint headers — never leak to an Anthropic upstream ---
            // These are client/session identifiers from the incoming Codex request,
            // not Anthropic protocol headers. Forwarding them both leaks identity and
            // can defeat strict gateway fingerprint checks.
            // The full set lives in `is_codex_client_fingerprint_header` so it stays in one
            // place. (HeaderName is lowercased by the http crate, so a direct match is safe.)
            if codex_responses_to_anthropic && is_codex_client_fingerprint_header(key_str) {
                continue;
            }

            // --- accept — force application/json on the Codex→Anthropic path ---
            // The Codex CLI sends `Accept: text/event-stream`, whereas a native
            // Anthropic client sends `application/json` (streaming is driven by
            // the body's stream:true). Strict Anthropic gateways return 406 Not
            // Acceptable for an event-stream Accept, so normalize it here.
            if codex_responses_to_anthropic && key_str.eq_ignore_ascii_case("accept") {
                if !saw_accept {
                    saw_accept = true;
                    ordered_headers.append(
                        http::header::ACCEPT,
                        http::HeaderValue::from_static("application/json"),
                    );
                }
                continue;
            }

            // --- accept-encoding — transform / SSE 路径强制 identity，其余保留原值 ---
            if key_str.eq_ignore_ascii_case("accept-encoding") {
                if !saw_accept_encoding {
                    saw_accept_encoding = true;
                    if force_identity_encoding {
                        ordered_headers.append(
                            http::header::ACCEPT_ENCODING,
                            http::HeaderValue::from_static("identity"),
                        );
                    } else {
                        ordered_headers.append(key.clone(), value.clone());
                    }
                }
                continue;
            }

            // --- user-agent: provider-level override for local proxy routing ---
            if !is_copilot && key_str.eq_ignore_ascii_case("user-agent") {
                if !saw_user_agent {
                    saw_user_agent = true;
                    if let Some(ref ua) = custom_user_agent {
                        ordered_headers.append(http::header::USER_AGENT, ua.clone());
                    } else {
                        ordered_headers.append(key.clone(), value.clone());
                    }
                }
                continue;
            }

            // --- anthropic-beta — 用重建值替换（确保含 claude-code 标记） ---
            if key_str.eq_ignore_ascii_case("anthropic-beta") {
                if !saw_anthropic_beta {
                    saw_anthropic_beta = true;
                    if let Some(ref beta_val) = anthropic_beta_value {
                        if let Ok(hv) = http::HeaderValue::from_str(beta_val) {
                            ordered_headers.append("anthropic-beta", hv);
                        }
                    }
                }
                continue;
            }

            // --- anthropic-version — 透传客户端值 ---
            if key_str.eq_ignore_ascii_case("anthropic-version") {
                if should_send_anthropic_headers {
                    saw_anthropic_version = true;
                    ordered_headers.append(key.clone(), value.clone());
                }
                continue;
            }

            // --- Copilot 指纹头 — 跳过（由 auth_headers 提供） ---
            if copilot_fingerprint_headers
                .iter()
                .any(|h| key_str.eq_ignore_ascii_case(h))
            {
                continue;
            }

            // --- 默认：透传 ---
            ordered_headers.append(key.clone(), value.clone());
        }

        // 如果原始请求中没有认证头，在末尾追加
        if !saw_auth && !auth_headers.is_empty() {
            for (ah_name, ah_value) in &auth_headers {
                ordered_headers.append(ah_name.clone(), ah_value.clone());
            }
        }

        // transform / SSE 路径在缺失时补 identity；普通透传不主动补 accept-encoding
        if !saw_accept_encoding && force_identity_encoding {
            ordered_headers.append(
                http::header::ACCEPT_ENCODING,
                http::HeaderValue::from_static("identity"),
            );
        }

        // On the Codex→Anthropic path, add application/json when Accept is missing (matching a native Anthropic client).
        if codex_responses_to_anthropic && !saw_accept {
            ordered_headers.append(
                http::header::ACCEPT,
                http::HeaderValue::from_static("application/json"),
            );
        }

        // Codex→Anthropic emulation: inject Claude Code's x-app: cli
        if codex_impersonate_claude_code {
            ordered_headers.append("x-app", http::HeaderValue::from_static("cli"));
        }

        if !saw_user_agent {
            if let Some(ref ua) = custom_user_agent {
                ordered_headers.append(http::header::USER_AGENT, ua.clone());
            }
        }

        // 如果原始请求中没有 anthropic-beta 且有值需要添加，追加
        if !saw_anthropic_beta {
            if let Some(ref beta_val) = anthropic_beta_value {
                if let Ok(hv) = http::HeaderValue::from_str(beta_val) {
                    ordered_headers.append("anthropic-beta", hv);
                }
            }
        }

        // anthropic-version: add the default only when it is missing.
        // The Codex→Anthropic path also needs this header. Note this is independent
        // of anthropic-beta: the Claude Code-specific beta is only sent when
        // impersonation is on (handled above); on the plain Codex→Anthropic path
        // (impersonation off) anthropic-version is still required but no beta is sent.
        if (should_send_anthropic_headers || codex_responses_to_anthropic) && !saw_anthropic_version
        {
            ordered_headers.append(
                "anthropic-version",
                http::HeaderValue::from_static("2023-06-01"),
            );
        }

        // Codex OAuth 反代尽量对齐官方 Codex CLI 的会话路由信号。
        // 只发送客户端提供的 session_id；生成的 UUID 每次不同，反而会破坏前缀缓存。
        for (name, value) in codex_oauth_session_headers {
            ordered_headers.insert(name, value);
        }

        // 序列化请求体。GET/HEAD 是 idempotent/safe 方法，按 HTTP 语义不应携带 body；
        // 强行附带 JSON body 会让某些上游（如 Google Gemini 的 models.list）拒绝请求。
        let body_bytes = if matches!(method, &http::Method::GET | &http::Method::HEAD) {
            Vec::new()
        } else {
            serde_json::to_vec(&filtered_body).map_err(|e| {
                ProxyError::Internal(format!("Failed to serialize request body: {e}"))
            })?
        };

        // 确保 content-type 存在
        if !ordered_headers.contains_key(http::header::CONTENT_TYPE) {
            ordered_headers.insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/json"),
            );
        }

        apply_local_proxy_header_overrides(
            &mut ordered_headers,
            provider
                .meta
                .as_ref()
                .and_then(|meta| meta.local_proxy_request_overrides.as_ref()),
            is_copilot,
        );

        reject_proxy_placeholder_for_managed_account_upstream(&url, &ordered_headers)?;

        // 日志目标 URL 的脱敏分两种情形：
        // - 有已知密钥(log_secrets 非空)：记录脱敏后的完整 URL，剥 userinfo/query
        //   并抹掉已知密钥值，保留 host+path 便于诊断 base_url 配错路径导致的 404。
        // - 无已知密钥：凭据可能整个内嵌在 path 里且无从脱敏，只记 origin，
        //   避免默认 Info 级把形如 https://gw/<KEY>/v1 的 path 完整落盘。
        let target_for_log = if log_secrets.is_empty() {
            crate::redact_url_origin_for_log(&url)
        } else {
            crate::redact_url_for_log_with_secrets(&url, &log_secrets)
        };

        // 输出请求信息日志
        let tag = adapter.name();
        let request_model = filtered_body
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or("<none>");
        log::info!("[{tag}] >>> 请求目标: {target_for_log} (model={request_model})");
        log::debug!(
            "[{tag}] >>> 请求体已准备: bytes={}, hash={} (content omitted)",
            body_bytes.len(),
            short_value_hash(Some(&filtered_body))
        );

        let send_remaining = Self::remaining_attempt_time(deadline);
        if send_remaining.is_some_and(|remaining| remaining.is_zero()) {
            return Err(ProxyError::Timeout(
                "Provider attempt deadline exceeded before upstream send".to_string(),
            ));
        }
        let timeout = send_remaining.unwrap_or_else(|| {
            if self.non_streaming_timeout.is_zero() {
                std::time::Duration::from_secs(600)
            } else {
                self.non_streaming_timeout
            }
        });

        // 获取全局代理 URL
        let upstream_proxy_url: Option<String> = super::http_client::get_current_proxy_url();

        // SOCKS5 代理不支持 CONNECT 隧道，需要用 reqwest
        let is_socks_proxy = upstream_proxy_url
            .as_deref()
            .map(|u| u.starts_with("socks5"))
            .unwrap_or(false);

        let preserve_exact_header_case = should_preserve_exact_header_case(
            adapter.name(),
            provider,
            resolved_claude_api_format.as_deref(),
            is_copilot,
        );

        // 发送请求
        let response = if is_socks_proxy || !preserve_exact_header_case {
            // OpenAI / Copilot / Codex 类后端不依赖原始 header 大小写；走 reqwest
            // 连接池，避免 raw TCP/TLS path 每次请求都重新握手。SOCKS5 也只能走 reqwest。
            log::debug!(
                "[Forwarder] Using pooled reqwest client (preserve_exact_header_case={preserve_exact_header_case}, socks_proxy={is_socks_proxy})"
            );
            let client = super::http_client::get();
            let mut request = client.request(method.clone(), &url);
            if request_is_streaming {
                // reqwest's timeout is a total request timeout; streaming body
                // lifetime remains governed by downstream idle-timeout handling.
                request = request.timeout(std::time::Duration::from_secs(24 * 60 * 60));
            } else if let Some(remaining) = send_remaining {
                request = request.timeout(remaining);
            } else if !self.non_streaming_timeout.is_zero() {
                request = request.timeout(self.non_streaming_timeout);
            }
            for (key, value) in &ordered_headers {
                request = request.header(key, value);
            }
            let send = request.body(body_bytes).send();
            let send_result = if request_is_streaming {
                let header_timeout = send_remaining.unwrap_or(timeout);
                tokio::time::timeout(header_timeout, send)
                    .await
                    .map_err(|_| {
                        if deadline.is_some() {
                            ProxyError::Timeout(
                                "Provider attempt deadline exceeded before upstream headers"
                                    .to_string(),
                            )
                        } else {
                            ProxyError::Timeout(format!(
                                "流式响应首包超时: {}s（上游未返回响应头）",
                                header_timeout.as_secs()
                            ))
                        }
                    })?
            } else {
                send.await
            };
            let reqwest_resp = send_result.map_err(map_reqwest_send_error)?;
            ProxyResponse::Reqwest(reqwest_resp)
        } else {
            // HTTP 代理或直连：走 hyper raw write（保持 header 大小写）
            // 如果有 HTTP 代理，hyper_client 会用 CONNECT 隧道穿过代理
            let uri: http::Uri = url.parse().map_err(|e| {
                ProxyError::ForwardFailed(format!("Invalid upstream URL ({target_for_log}): {e}"))
            })?;
            super::hyper_client::send_request(
                uri,
                &target_for_log,
                method.clone(),
                ordered_headers,
                extensions.clone(),
                body_bytes,
                send_remaining.unwrap_or(timeout),
                upstream_proxy_url.as_deref(),
            )
            .await?
        };

        // 检查响应状态
        let status = response.status();

        if status.is_success() {
            let mut response = self
                .prepare_success_response_for_failover(
                    response,
                    request_is_streaming,
                    deadline,
                    policy_enabled,
                )
                .await?;
            // Streaming requests normally return SSE. If a compatible gateway
            // explicitly returns JSON instead, buffer and validate it inside the retry
            // loop as well so a 2xx Anthropic error envelope can still fail over. Do
            // not buffer unknown content types: some gateways omit the SSE header.
            if codex_responses_to_anthropic && (!request_is_streaming || response.is_json()) {
                response = self
                    .validate_codex_anthropic_success_response(response, deadline)
                    .await?;
            } else if matches!(
                resolved_claude_api_format.as_deref(),
                Some("openai_responses")
            ) {
                if !request_is_streaming || response.is_json() {
                    // Claude→Responses gateways can also return a semantic failure in an
                    // HTTP 2xx Response object. Validate buffered/JSON bodies inside the
                    // retry loop so an early failure can still select another provider.
                    response = self
                        .validate_responses_success_response(response, deadline)
                        .await?;
                } else {
                    // Delay committing the downstream stream until the upstream emits
                    // either productive output or a valid non-failure terminal event.
                    // A response.failed/error before output remains failover-safe.
                    response = self
                        .validate_responses_stream_start(response, deadline)
                        .await?;
                }
            }
            Ok((response, resolved_claude_api_format, outbound_model))
        } else {
            let status_code = status.as_u16();
            // 错误响应同样可能被上游压缩（content-encoding）。reqwest 未启用任何
            // 自动解压 feature，这里拿到的是原始字节；不解压的话，压缩过的错误体会
            // 在 from_utf8 处变成非 UTF-8 而被丢弃，隐藏掉上游的限流/鉴权等详情。
            let encoding = get_content_encoding(response.headers());
            let raw = match Self::remaining_attempt_time(deadline) {
                Some(remaining) => tokio::time::timeout(
                    remaining,
                    response.bytes_with_limit(MAX_RESPONSE_BODY_BYTES),
                )
                .await
                .map_err(|_| {
                    ProxyError::Timeout(
                        "Provider attempt deadline exceeded while reading error body".to_string(),
                    )
                })??,
                None => response.bytes_with_limit(MAX_RESPONSE_BODY_BYTES).await?,
            };
            let decoded = match encoding {
                Some(encoding) => {
                    match decompress_body_with_limit(&encoding, &raw, MAX_RESPONSE_BODY_BYTES) {
                        Ok(Some(decompressed)) => decompressed,
                        // 不支持的编码 / 解压失败 / 解压后超限：退回（已有上限的）
                        // 原始字节，尽量保留可读信息
                        _ => raw.to_vec(),
                    }
                }
                None => raw.to_vec(),
            };
            let body_text = String::from_utf8(decoded).ok();

            Err(ProxyError::UpstreamError {
                status: status_code,
                body: body_text,
            })
        }
    }

    /// 故障转移开启时，成功不能只看上游响应头。
    ///
    /// - 非流式：先把完整 body 读到内存，读超时/连接中断会回到 retry loop 尝试下一家。
    /// - 流式：至少等首个 chunk 到达，避免上游返回 200 后一直不吐 SSE 时被误记成功。
    async fn prepare_success_response_for_failover(
        &self,
        response: ProxyResponse,
        request_is_streaming: bool,
        deadline: Option<tokio::time::Instant>,
        policy_enabled: bool,
    ) -> Result<ProxyResponse, ProxyError> {
        if request_is_streaming {
            return self
                .prime_streaming_response(response, deadline, policy_enabled)
                .await;
        }

        // A provider policy enables buffering even when app-level failover timeouts
        // are disabled. Legacy zero/zero requests retain pass-through behavior.
        if deadline.is_none() && self.non_streaming_timeout.is_zero() && !policy_enabled {
            return Ok(response);
        }

        let status = response.status();
        let headers = response.headers().clone();
        let body = match Self::remaining_attempt_time(deadline) {
            Some(remaining) => tokio::time::timeout(
                remaining,
                response.bytes_with_limit(MAX_RESPONSE_BODY_BYTES),
            )
            .await
            .map_err(|_| {
                ProxyError::Timeout(
                    "Provider attempt deadline exceeded while reading response body".to_string(),
                )
            })??,
            None if policy_enabled => response.bytes_with_limit(MAX_RESPONSE_BODY_BYTES).await?,
            None => {
                let body_timeout = self.non_streaming_timeout;
                tokio::time::timeout(
                    body_timeout,
                    response.bytes_with_limit(MAX_RESPONSE_BODY_BYTES),
                )
                .await
                .map_err(|_| {
                    ProxyError::Timeout(format!(
                        "响应体读取超时: {}s（上游发完响应头后 body 未到达）",
                        body_timeout.as_secs()
                    ))
                })??
            }
        };

        Ok(ProxyResponse::buffered(status, headers, body))
    }
    async fn read_response_body_with_deadline(
        response: ProxyResponse,
        deadline: Option<tokio::time::Instant>,
        context: &str,
    ) -> Result<Bytes, ProxyError> {
        let read = response.bytes_with_limit(MAX_RESPONSE_BODY_BYTES);
        match Self::remaining_attempt_time(deadline) {
            Some(remaining) => tokio::time::timeout(remaining, read).await.map_err(|_| {
                ProxyError::Timeout(format!(
                    "Provider attempt deadline exceeded while {context}"
                ))
            })?,
            None => read.await,
        }
    }

    /// Some Anthropic-compatible gateways return an Anthropic error envelope with
    /// HTTP 2xx. Validate it inside the retry loop so the request can fail over to
    /// the next provider; the response transformer runs too late for that.
    async fn validate_codex_anthropic_success_response(
        &self,
        response: ProxyResponse,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<ProxyResponse, ProxyError> {
        let status = response.status();
        let headers = response.headers().clone();
        let encoding = get_content_encoding(&headers);
        let raw = Self::read_response_body_with_deadline(
            response,
            deadline,
            "validating Anthropic success response",
        )
        .await?;
        let decoded = match encoding {
            Some(encoding) => {
                match decompress_body_with_limit(&encoding, &raw, MAX_RESPONSE_BODY_BYTES) {
                    Ok(Some(decompressed)) => decompressed,
                    _ => raw.to_vec(),
                }
            }
            None => raw.to_vec(),
        };

        if let Some(message) = codex_anthropic_error_envelope_message(&decoded) {
            return Err(ProxyError::TransformErrorWithBody {
                message: format!("Anthropic upstream returned a 2xx error envelope: {message}"),
                body: String::from_utf8_lossy(&decoded).into_owned(),
            });
        }

        Ok(ProxyResponse::buffered(status, headers, raw))
    }

    async fn validate_responses_success_response(
        &self,
        response: ProxyResponse,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<ProxyResponse, ProxyError> {
        let status = response.status();
        let headers = response.headers().clone();
        let encoding = get_content_encoding(&headers);
        let raw = Self::read_response_body_with_deadline(
            response,
            deadline,
            "validating Responses success response",
        )
        .await?;
        let decoded = match encoding {
            Some(encoding) => {
                match decompress_body_with_limit(&encoding, &raw, MAX_RESPONSE_BODY_BYTES) {
                    Ok(Some(decompressed)) => decompressed,
                    _ => raw.to_vec(),
                }
            }
            None => raw.to_vec(),
        };

        if let Some(message) = responses_error_envelope_message(&decoded) {
            return Err(ProxyError::TransformErrorWithBody {
                message: format!("Responses upstream returned a 2xx failure: {message}"),
                body: String::from_utf8_lossy(&decoded).into_owned(),
            });
        }

        Ok(ProxyResponse::buffered(status, headers, raw))
    }

    async fn validate_responses_stream_start(
        &self,
        response: ProxyResponse,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<ProxyResponse, ProxyError> {
        const MAX_PRIME_BYTES: usize = 256 * 1024;

        let status = response.status();
        let headers = response.headers().clone();
        let mut stream = Box::pin(response.bytes_stream());
        let mut replay_chunks: Vec<Bytes> = Vec::new();
        let mut parse_buffer = String::new();
        let mut utf8_remainder = Vec::new();

        loop {
            let next = if let Some(remaining) = Self::remaining_attempt_time(deadline) {
                tokio::time::timeout(remaining, stream.next())
                    .await
                    .map_err(|_| {
                        ProxyError::Timeout(
                            "Provider attempt deadline exceeded while priming Responses stream"
                                .to_string(),
                        )
                    })?
            } else if self.streaming_first_byte_timeout.is_zero() {
                stream.next().await
            } else {
                tokio::time::timeout(self.streaming_first_byte_timeout, stream.next())
                    .await
                    .map_err(|_| {
                        ProxyError::Timeout(format!(
                            "Responses stream produced no semantic output within {}s",
                            self.streaming_first_byte_timeout.as_secs()
                        ))
                    })?
            };

            let Some(chunk) = next else {
                if let Some(outcome) = inspect_responses_json_document(&parse_buffer) {
                    outcome?;
                    let replay = futures::stream::iter(replay_chunks.into_iter().map(Ok));
                    return Ok(ProxyResponse::streamed(status, headers, replay));
                }
                if !parse_buffer.trim().is_empty() {
                    if let Some(outcome) = inspect_responses_start_event(parse_buffer.trim()) {
                        outcome?;
                        let replay = futures::stream::iter(replay_chunks.into_iter().map(Ok));
                        return Ok(ProxyResponse::streamed(status, headers, replay));
                    }
                }
                return Err(ProxyError::ForwardFailed(
                    "Responses stream ended before producing output or a terminal event"
                        .to_string(),
                ));
            };
            let chunk = chunk.map_err(|error| {
                ProxyError::ForwardFailed(format!(
                    "Failed while validating Responses stream start: {error}"
                ))
            })?;
            crate::proxy::sse::append_utf8_safe(&mut parse_buffer, &mut utf8_remainder, &chunk);
            replay_chunks.push(chunk);

            // Some compatible gateways ignore `stream:true` and return a complete
            // Responses JSON document without a JSON content-type. Recognize that
            // shape before looking for SSE delimiters; pretty-printed JSON may itself
            // contain blank lines and must stay intact.
            if let Some(outcome) = inspect_responses_json_document(&parse_buffer) {
                outcome?;
                let replay = futures::stream::iter(replay_chunks.into_iter().map(Ok)).chain(stream);
                return Ok(ProxyResponse::streamed(status, headers, replay));
            }

            while let Some(block) = crate::proxy::sse::take_sse_block(&mut parse_buffer) {
                if let Some(outcome) = inspect_responses_start_event(&block) {
                    outcome?;
                    let replay =
                        futures::stream::iter(replay_chunks.into_iter().map(Ok)).chain(stream);
                    return Ok(ProxyResponse::streamed(status, headers, replay));
                }
            }

            if replay_chunks.iter().map(Bytes::len).sum::<usize>() >= MAX_PRIME_BYTES {
                log::warn!(
                    "[Claude/Responses] semantic stream priming exceeded {MAX_PRIME_BYTES} bytes; committing buffered stream"
                );
                let replay = futures::stream::iter(replay_chunks.into_iter().map(Ok)).chain(stream);
                return Ok(ProxyResponse::streamed(status, headers, replay));
            }
        }
    }

    async fn prime_streaming_response(
        &self,
        response: ProxyResponse,
        deadline: Option<tokio::time::Instant>,
        policy_enabled: bool,
    ) -> Result<ProxyResponse, ProxyError> {
        if deadline.is_none() && self.streaming_first_byte_timeout.is_zero() && !policy_enabled {
            return Ok(response);
        }

        let status = response.status();
        let headers = response.headers().clone();
        let mut stream = Box::pin(response.bytes_stream());
        let first = if let Some(remaining) = Self::remaining_attempt_time(deadline) {
            if remaining.is_zero() {
                return Err(ProxyError::Timeout(
                    "Provider attempt deadline exceeded before first stream chunk".to_string(),
                ));
            }
            tokio::time::timeout(remaining, stream.next())
                .await
                .map_err(|_| {
                    ProxyError::Timeout(
                        "Provider attempt deadline exceeded before first stream chunk".to_string(),
                    )
                })?
        } else if self.streaming_first_byte_timeout.is_zero() && policy_enabled {
            stream.next().await
        } else {
            let timeout = self.streaming_first_byte_timeout;
            tokio::time::timeout(timeout, stream.next())
                .await
                .map_err(|_| {
                    ProxyError::Timeout(format!(
                        "流式响应首包超时: {}s（上游已返回响应头但未返回数据）",
                        timeout.as_secs()
                    ))
                })?
        };

        let Some(first) = first else {
            return Err(ProxyError::ForwardFailed(
                "流式响应在首包到达前结束".to_string(),
            ));
        };

        let first =
            first.map_err(|e| ProxyError::ForwardFailed(format!("读取流式响应首包失败: {e}")))?;

        let replay = futures::stream::once(async move { Ok(first) }).chain(stream);
        Ok(ProxyResponse::streamed(status, headers, replay))
    }

    async fn resolve_claude_api_format(
        &self,
        provider: &Provider,
        body: &Value,
        is_copilot: bool,
    ) -> String {
        if !is_copilot {
            return super::providers::get_claude_api_format(provider).to_string();
        }

        let model = body.get("model").and_then(|value| value.as_str());
        if let Some(model_id) = model {
            if self
                .is_copilot_openai_vendor_model(provider, model_id)
                .await
            {
                return "openai_responses".to_string();
            }
        }

        "openai_chat".to_string()
    }

    /// 用 Copilot live `/models` 列表确认 model ID 真实可用，找不到时按 family 降级。
    /// 命中缓存后是同步的；首次请求或 5 min 缓存过期后会触发一次 HTTP。
    async fn apply_copilot_live_model_resolution(
        &self,
        provider: &Provider,
        body: &mut serde_json::Value,
    ) {
        let Some(model_id) = body.get("model").and_then(|v| v.as_str()) else {
            return;
        };
        let model_id = model_id.to_string();

        let Some(app_handle) = &self.app_handle else {
            return;
        };
        let copilot_state = app_handle.state::<CopilotAuthState>();
        let copilot_auth = copilot_state.0.read().await;
        let account_id = provider
            .meta
            .as_ref()
            .and_then(|m| m.managed_account_id_for("github_copilot"));

        let models_result = match account_id.as_deref() {
            Some(id) => copilot_auth.fetch_models_for_account(id).await,
            None => copilot_auth.fetch_models().await,
        };

        let models = match models_result {
            Ok(m) => m,
            Err(err) => {
                log::debug!("[Copilot] live model list unavailable, skip resolution: {err}");
                return;
            }
        };

        if let Some(resolved) =
            super::providers::copilot_model_map::resolve_against_models(&model_id, &models)
        {
            log::info!("[Copilot] live-model resolve: {model_id} → {resolved}");
            body["model"] = serde_json::Value::String(resolved);
        }
    }

    async fn is_copilot_openai_vendor_model(&self, provider: &Provider, model_id: &str) -> bool {
        let Some(app_handle) = &self.app_handle else {
            log::debug!("[Copilot] AppHandle unavailable, fallback to chat/completions");
            return false;
        };

        let copilot_state = app_handle.state::<CopilotAuthState>();
        let copilot_auth = copilot_state.0.read().await;
        let account_id = provider
            .meta
            .as_ref()
            .and_then(|m| m.managed_account_id_for("github_copilot"));

        let vendor_result = match account_id.as_deref() {
            Some(id) => {
                copilot_auth
                    .get_model_vendor_for_account(id, model_id)
                    .await
            }
            None => copilot_auth.get_model_vendor(model_id).await,
        };

        match vendor_result {
            Ok(Some(vendor)) => vendor.eq_ignore_ascii_case("openai"),
            Ok(None) => {
                log::debug!(
                    "[Copilot] Model vendor unavailable for {model_id}, fallback to chat/completions"
                );
                false
            }
            Err(err) => {
                log::warn!(
                    "[Copilot] Failed to resolve model vendor for {model_id}, fallback to chat/completions: {err}"
                );
                false
            }
        }
    }

    fn categorize_proxy_error(&self, error: &ProxyError, provider: &Provider) -> ErrorCategory {
        // Authentication belongs to the Codex client for an official route.
        // Every retry would reuse the selected account's inbound Authorization
        // header against another card, so no official-route error may fail over.
        if super::providers::is_codex_official_provider(provider) {
            return ErrorCategory::NonRetryable;
        }

        // xAI OAuth mirrors the same rule for token acquisition: a local
        // AuthError means the managed account needs re-login. Failing over
        // would silently move the conversation off the selected Grok account
        // and poison the provider's health state for an account-level issue.
        if provider.is_xai_oauth() && matches!(error, ProxyError::AuthError(_)) {
            return ErrorCategory::NonRetryable;
        }

        match error {
            // 网络和上游错误：都应该尝试下一个供应商
            ProxyError::Timeout(_) => ErrorCategory::Retryable,
            ProxyError::ForwardFailed(_) => ErrorCategory::Retryable,
            ProxyError::ProviderUnhealthy(_) => ErrorCategory::Retryable,
            // 上游 HTTP 错误：按状态码分桶。
            //
            // 客户端请求自身有问题的状态码无论换哪个 provider 都会被拒绝，
            // 继续轮询只会放大错误率、污染熔断器健康度、浪费配额：
            //   400 Bad Request / 422 Unprocessable Entity   ← 请求体格式或语义错误
            //   405 Method Not Allowed / 406 Not Acceptable  ← 方法或 Accept 错误
            //   413 Payload Too Large / 414 URI Too Long     ← 客户端构造超限
            //   415 Unsupported Media Type                    ← Content-Type 错误
            //   501 Not Implemented                           ← 上游协议确实不支持
            //
            // 其他 4xx（401/403/404/408/409/429/451 等）和全部 5xx 都保留
            // Retryable —— 换一家 provider 可能持有不同的 key、配额、地域或模型映射。
            ProxyError::UpstreamError { status, .. } => match *status {
                400 | 405 | 406 | 413 | 414 | 415 | 422 | 501 => ErrorCategory::NonRetryable,
                _ => ErrorCategory::Retryable,
            },
            // Provider 级配置/转换问题：换一个 Provider 可能就能成功
            ProxyError::ConfigError(_) => ErrorCategory::Retryable,
            ProxyError::TransformError(_) | ProxyError::TransformErrorWithBody { .. } => {
                ErrorCategory::Retryable
            }
            ProxyError::AuthError(_) => ErrorCategory::Retryable,
            ProxyError::StreamIdleTimeout(_) => ErrorCategory::Retryable,
            // 无可用供应商：所有供应商都试过了，无法重试
            ProxyError::NoAvailableProvider => ErrorCategory::NonRetryable,
            // 其他错误（数据库/内部错误等）：不是换供应商能解决的问题
            _ => ErrorCategory::NonRetryable,
        }
    }
}

/// 按上游 wire format 解析非流式 usage。
fn parse_usage_value(value: &Value) -> Option<TokenUsage> {
    if value.get("usageMetadata").is_some() {
        return TokenUsage::from_gemini_response(value);
    }

    let usage = value.get("usage")?;
    if usage.get("prompt_tokens").is_some() {
        return TokenUsage::from_openai_response(value);
    }
    if usage.get("cache_read_input_tokens").is_some()
        || usage.get("cache_creation_input_tokens").is_some()
    {
        return TokenUsage::from_claude_response(value);
    }
    TokenUsage::from_codex_response_auto(value).or_else(|| TokenUsage::from_claude_response(value))
}

fn parse_failed_attempt_usage(error: &ProxyError) -> Option<TokenUsage> {
    let body = match error {
        ProxyError::UpstreamError {
            body: Some(body), ..
        }
        | ProxyError::TransformErrorWithBody { body, .. } => body,
        _ => return None,
    };
    let value: Value = serde_json::from_str(body).ok()?;
    std::iter::once(&value)
        .chain(value.get("response"))
        .chain(value.get("data"))
        .find_map(parse_usage_value)
        .filter(TokenUsage::has_billable_tokens)
}

/// 从 ProxyError 中提取错误消息，供同供应商修复判定使用。
fn extract_error_message(error: &ProxyError) -> Option<String> {
    match error {
        ProxyError::UpstreamError { body, .. } => body.clone(),
        ProxyError::TransformErrorWithBody { body, .. } => Some(body.clone()),
        _ => Some(error.to_string()),
    }
}

/// 检测 Provider 是否为 Bedrock（通过 CLAUDE_CODE_USE_BEDROCK 环境变量判断）
fn is_bedrock_provider(provider: &Provider) -> bool {
    provider
        .settings_config
        .get("env")
        .and_then(|e| e.get("CLAUDE_CODE_USE_BEDROCK"))
        .and_then(|v| v.as_str())
        .map(|v| v == "1")
        .unwrap_or(false)
}

fn build_retryable_failure_log(
    provider_name: &str,
    attempted_providers: usize,
    total_providers: usize,
    error: &ProxyError,
) -> (&'static str, String) {
    let error_summary = summarize_proxy_error(error);

    if total_providers <= 1 {
        (
            log_fwd::SINGLE_PROVIDER_FAILED,
            format!("Provider {provider_name} 请求失败: {error_summary}"),
        )
    } else {
        (
            log_fwd::PROVIDER_FAILED_RETRY,
            format!(
                "Provider {provider_name} 失败，继续尝试下一个 ({attempted_providers}/{total_providers}): {error_summary}"
            ),
        )
    }
}

fn build_terminal_failure_log(
    attempted_providers: usize,
    total_providers: usize,
    last_error: Option<&ProxyError>,
) -> Option<(&'static str, String)> {
    if total_providers <= 1 {
        return None;
    }

    let error_summary = last_error
        .map(summarize_proxy_error)
        .unwrap_or_else(|| "未知错误".to_string());

    Some((
        log_fwd::ALL_PROVIDERS_FAILED,
        format!(
            "已尝试 {attempted_providers}/{total_providers} 个 Provider，均失败。最后错误: {error_summary}"
        ),
    ))
}

fn summarize_proxy_error(error: &ProxyError) -> String {
    match error {
        ProxyError::UpstreamError { status, body } => {
            let body_summary = body
                .as_deref()
                .map(summarize_upstream_body)
                .filter(|summary| !summary.is_empty());

            match body_summary {
                Some(summary) => format!("上游 HTTP {status}: {summary}"),
                None => format!("上游 HTTP {status}"),
            }
        }
        ProxyError::Timeout(message) => {
            format!("请求超时: {}", summarize_text_for_log(message, 180))
        }
        ProxyError::ForwardFailed(message) => {
            format!("请求转发失败: {}", summarize_text_for_log(message, 180))
        }
        ProxyError::TransformError(message)
        | ProxyError::TransformErrorWithBody { message, .. } => {
            format!("响应转换失败: {}", summarize_text_for_log(message, 180))
        }
        ProxyError::ConfigError(message) => {
            format!("配置错误: {}", summarize_text_for_log(message, 180))
        }
        ProxyError::AuthError(message) => {
            format!("认证失败: {}", summarize_text_for_log(message, 180))
        }
        _ => summarize_text_for_log(&error.to_string(), 180),
    }
}

fn summarize_upstream_body(body: &str) -> String {
    if let Ok(json_body) = serde_json::from_str::<Value>(body) {
        if let Some(message) = extract_json_error_message(&json_body) {
            return summarize_text_for_log(&message, 180);
        }

        if let Ok(compact_json) = serde_json::to_string(&json_body) {
            return summarize_text_for_log(&compact_json, 180);
        }
    }

    summarize_text_for_log(body, 180)
}

fn extract_json_error_message(body: &Value) -> Option<String> {
    let candidates = [
        body.pointer("/error/message"),
        body.pointer("/message"),
        body.pointer("/detail"),
        body.pointer("/error"),
    ];

    candidates
        .into_iter()
        .flatten()
        .find_map(|value| value.as_str().map(ToString::to_string))
}

fn split_endpoint_and_query(endpoint: &str) -> (&str, Option<&str>) {
    endpoint
        .split_once('?')
        .map_or((endpoint, None), |(path, query)| (path, Some(query)))
}

fn strip_beta_query(query: Option<&str>) -> Option<String> {
    let filtered = query.map(|query| {
        query
            .split('&')
            .filter(|pair| !pair.is_empty() && !pair.starts_with("beta="))
            .collect::<Vec<_>>()
            .join("&")
    });

    match filtered.as_deref() {
        Some("") | None => None,
        Some(_) => filtered,
    }
}

fn is_claude_messages_path(path: &str) -> bool {
    matches!(path, "/v1/messages" | "/claude/v1/messages")
}

fn rewrite_codex_responses_endpoint_to_chat(endpoint: &str) -> (String, Option<String>) {
    let (_path, query) = split_endpoint_and_query(endpoint);
    let passthrough_query = query.map(ToString::to_string);
    let target_path = "/chat/completions";
    let rewritten = match passthrough_query.as_deref() {
        Some(query) if !query.is_empty() => format!("{target_path}?{query}"),
        _ => target_path.to_string(),
    };

    (rewritten, passthrough_query)
}

/// Claude Code client fingerprint (used for Codex→Anthropic emulation to pass a
/// gateway's "Claude Code only" check).
const CLAUDE_CODE_USER_AGENT: &str = "claude-cli/1.0.119 (external, cli)";
const CLAUDE_CODE_SYSTEM_IDENTITY: &str =
    "You are Claude Code, Anthropic's official CLI for Claude.";

/// Insert the Claude Code identity as the first line before the `system` field in
/// the Anthropic request body.
///
/// Anthropic subscription/OAuth plans require the first system block to be exactly
/// this identity line. After conversion `system` is a string (from Codex
/// instructions); normalize it into an array here: [identity line, original system...].
fn prepend_claude_code_system_prompt(body: &mut Value) {
    let identity = serde_json::json!({ "type": "text", "text": CLAUDE_CODE_SYSTEM_IDENTITY });
    let mut blocks: Vec<Value> = vec![identity];
    match body.get("system") {
        Some(Value::String(existing)) if !existing.is_empty() => {
            blocks.push(serde_json::json!({ "type": "text", "text": existing }));
        }
        Some(Value::Array(existing)) => {
            // Idempotent: skip re-injection if the first block is already the identity line.
            if existing
                .first()
                .and_then(|b| b.get("text"))
                .and_then(|t| t.as_str())
                == Some(CLAUDE_CODE_SYSTEM_IDENTITY)
            {
                return;
            }
            blocks.extend(existing.iter().cloned());
        }
        _ => {}
    }
    body["system"] = Value::Array(blocks);
}

/// Headers a native Claude Code client never sends but the Codex/OpenAI CLI (and its
/// stainless SDK layer) do. Dropped for every Codex→Anthropic request so the upstream sees a
/// clean Anthropic client fingerprint. Centralized here so the set stays in one place and future
/// additions can't miss a code path. `key_str` is already lowercased by the http crate.
/// Whether `base_url` already ends in `endpoint_suffix` (e.g. `/v1/messages` or
/// `/chat/completions`), ignoring surrounding whitespace, any `?query`/`#fragment`, and a
/// trailing slash. Used to avoid double-appending the endpoint when a user pastes a full
/// URL but leaves the "full URL" switch off (`.../v1/messages` → `.../v1/messages/v1/messages`,
/// a non-retryable 400). `endpoint_suffix` must be lowercase.
fn base_url_is_full_endpoint(base_url: &str, endpoint_suffix: &str) -> bool {
    let trimmed = base_url.trim();
    // Match against the path only: a `?query`/`#fragment` on a full endpoint URL must not
    // hide the suffix (`.../v1/messages?beta=true` still ends in the endpoint).
    let path = match trimmed.split_once(['?', '#']) {
        Some((head, _)) => head,
        None => trimmed,
    };
    path.trim_end_matches('/')
        .to_ascii_lowercase()
        .ends_with(endpoint_suffix)
}

fn is_codex_client_fingerprint_header(key_str: &str) -> bool {
    matches!(
        key_str,
        "originator"
            | "session_id"
            | "session-id"
            | "thread-id"
            | "conversation_id"
            | "chatgpt-account-id"
            | "x-openai-subagent"
            | "x-client-request-id"
            | "openai-beta"
            | "openai-organization"
            | "openai-project"
    ) || key_str.starts_with("x-stainless-")
        || key_str.starts_with("x-codex-")
}

fn codex_anthropic_error_envelope_message(body: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(body).ok()?;
    if value.get("type").and_then(Value::as_str) != Some("error") && value.get("error").is_none() {
        return None;
    }
    let error = value.get("error").unwrap_or(&value);
    let error_type = error.get("type").and_then(Value::as_str).unwrap_or("error");
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| error.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| error.to_string());
    Some(format!("{error_type}: {message}"))
}

fn responses_error_envelope_message(body: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let status = value.get("status").and_then(Value::as_str);
    let has_error = value.get("error").is_some_and(|error| !error.is_null());
    if !matches!(status, Some("failed" | "cancelled")) && !has_error {
        return None;
    }

    let error = value.get("error").unwrap_or(&value);
    let error_type = error
        .get("type")
        .and_then(Value::as_str)
        .or_else(|| error.get("code").and_then(Value::as_str))
        .unwrap_or_else(|| status.unwrap_or("error"));
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| error.as_str())
        .filter(|message| !message.trim().is_empty())
        .unwrap_or(match status {
            Some("cancelled") => "response generation was cancelled",
            _ => "response generation failed",
        });
    Some(format!("{error_type}: {message}"))
}

/// Prompt caching is part of the Codex→Anthropic protocol bridge rather than an
/// optional Bedrock optimizer. Codex requests do not contain Anthropic
/// `cache_control`, so keep bridge caching on by default while still honoring the
/// dedicated cache-injection switch. Injected breakpoints always use Anthropic's
/// standard 5-minute TTL.
fn codex_anthropic_cache_config(config: &OptimizerConfig) -> OptimizerConfig {
    OptimizerConfig {
        enabled: true,
        thinking_optimizer: false,
        cache_injection: config.cache_injection,
    }
}

/// A streaming request may receive a whole JSON document even when the gateway
/// omits `application/json`. `None` means either "not JSON" or "not complete yet";
/// a parsed document is safe to commit unless it is a semantic failure envelope.
fn inspect_responses_json_document(buffer: &str) -> Option<Result<(), ProxyError>> {
    let trimmed = buffer.trim();
    if !matches!(trimmed.as_bytes().first(), Some(b'{') | Some(b'[')) {
        return None;
    }
    let _: Value = serde_json::from_str(trimmed).ok()?;
    if let Some(message) = responses_error_envelope_message(trimmed.as_bytes()) {
        return Some(Err(ProxyError::TransformErrorWithBody {
            message: format!("Responses upstream returned a 2xx failure: {message}"),
            body: trimmed.to_string(),
        }));
    }
    Some(Ok(()))
}

/// Inspect one complete Responses SSE block while the response is still inside
/// the retry loop. `None` means the event is lifecycle-only and priming should
/// continue; `Some(Ok(()))` means it is safe to commit/replay the stream.
fn inspect_responses_start_event(block: &str) -> Option<Result<(), ProxyError>> {
    let mut named_event = None;
    let mut data_lines = Vec::new();
    for line in block.lines() {
        if let Some(event) = crate::proxy::sse::strip_sse_field(line, "event") {
            named_event = Some(event.trim().to_string());
        } else if let Some(data) = crate::proxy::sse::strip_sse_field(line, "data") {
            data_lines.push(data);
        }
    }
    if data_lines.is_empty() {
        return None;
    }
    let data = data_lines.join("\n");
    let value: Value = match serde_json::from_str(&data) {
        Ok(value) => value,
        Err(_) => return None,
    };
    let event = named_event
        .as_deref()
        .filter(|event| !event.is_empty())
        .or_else(|| value.get("type").and_then(Value::as_str))
        .unwrap_or("");

    let response = value.get("response").unwrap_or(&value);
    if matches!(
        response.get("status").and_then(Value::as_str),
        Some("failed" | "cancelled")
    ) || response.get("error").is_some_and(|error| !error.is_null())
    {
        let error = response.get("error").unwrap_or(response);
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .or_else(|| error.as_str())
            .unwrap_or("Responses upstream failed before output");
        let error_type = error
            .get("type")
            .and_then(Value::as_str)
            .or_else(|| error.get("code").and_then(Value::as_str))
            .or_else(|| response.get("status").and_then(Value::as_str))
            .unwrap_or("upstream_error");
        return Some(Err(ProxyError::TransformErrorWithBody {
            message: format!("Responses upstream {error_type}: {message}"),
            body: data,
        }));
    }

    match event {
        "response.failed" | "error" => {
            let error = response.get("error").unwrap_or(response);
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| error.as_str())
                .unwrap_or("Responses upstream emitted an error before output");
            let error_type = error
                .get("type")
                .and_then(Value::as_str)
                .or_else(|| error.get("code").and_then(Value::as_str))
                .unwrap_or("upstream_error");
            Some(Err(ProxyError::TransformErrorWithBody {
                message: format!("Responses upstream {error_type}: {message}"),
                body: data,
            }))
        }
        "response.created" | "response.in_progress" | "response.queued" => None,
        "" => None,
        // Productive output, incomplete, and completed terminals are all safe to
        // expose. Mid-stream failures after this point are surfaced by the converter
        // but intentionally do not switch providers.
        _ => Some(Ok(())),
    }
}

/// Rewrite Codex's `/responses` (and variants) to Anthropic's `/v1/messages`, preserving the query.
fn rewrite_codex_responses_endpoint_to_anthropic(endpoint: &str) -> (String, Option<String>) {
    let (_path, query) = split_endpoint_and_query(endpoint);
    let passthrough_query = query.map(ToString::to_string);
    let target_path = "/v1/messages";
    let rewritten = match passthrough_query.as_deref() {
        Some(query) if !query.is_empty() => format!("{target_path}?{query}"),
        _ => target_path.to_string(),
    };

    (rewritten, passthrough_query)
}

fn rewrite_claude_transform_endpoint(
    endpoint: &str,
    api_format: &str,
    is_copilot: bool,
    body: &Value,
) -> (String, Option<String>) {
    let (path, query) = split_endpoint_and_query(endpoint);
    let passthrough_query = if is_claude_messages_path(path) {
        strip_beta_query(query)
    } else {
        query.map(ToString::to_string)
    };

    if !is_claude_messages_path(path) {
        return (endpoint.to_string(), passthrough_query);
    }

    if api_format == "gemini_native" {
        let model =
            super::providers::transform_gemini::extract_gemini_model(body).unwrap_or("unknown");
        // Accept both bare ids (`gemini-2.5-pro`) and the resource-name
        // form (`models/gemini-2.5-pro`) that Gemini SDKs emit. See
        // `normalize_gemini_model_id` for rationale.
        let model = super::gemini_url::normalize_gemini_model_id(model);
        let is_stream = body
            .get("stream")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let target_path = if is_stream {
            format!("/v1beta/models/{model}:streamGenerateContent")
        } else {
            format!("/v1beta/models/{model}:generateContent")
        };

        let rewritten_query = merge_query_params(
            passthrough_query.as_deref(),
            if is_stream { Some("alt=sse") } else { None },
        );

        let rewritten = match rewritten_query.as_deref() {
            Some(query) if !query.is_empty() => format!("{target_path}?{query}"),
            _ => target_path,
        };

        return (rewritten, rewritten_query);
    }

    let target_path = if is_copilot && api_format == "openai_responses" {
        "/v1/responses"
    } else if is_copilot {
        "/chat/completions"
    } else if api_format == "openai_responses" {
        "/v1/responses"
    } else {
        "/v1/chat/completions"
    };

    let rewritten = match passthrough_query.as_deref() {
        Some(query) if !query.is_empty() => format!("{target_path}?{query}"),
        _ => target_path.to_string(),
    };

    (rewritten, passthrough_query)
}

fn merge_query_params(base_query: Option<&str>, extra_param: Option<&str>) -> Option<String> {
    let mut params: Vec<String> = base_query
        .into_iter()
        .flat_map(|query| query.split('&'))
        .filter(|pair| !pair.is_empty())
        .filter(|pair| !pair.starts_with("alt="))
        .map(ToString::to_string)
        .collect();

    if let Some(extra_param) = extra_param {
        params.push(extra_param.to_string());
    }

    if params.is_empty() {
        None
    } else {
        Some(params.join("&"))
    }
}

fn append_query_to_full_url(base_url: &str, query: Option<&str>) -> String {
    match query {
        Some(query) if !query.is_empty() => {
            if base_url.contains('?') {
                format!("{base_url}&{query}")
            } else {
                format!("{base_url}?{query}")
            }
        }
        _ => base_url.to_string(),
    }
}

fn build_codex_oauth_session_headers(
    session_id: &str,
) -> Vec<(http::HeaderName, http::HeaderValue)> {
    let session_id = session_id.trim();
    if session_id.is_empty() {
        return Vec::new();
    }

    let mut headers = Vec::new();
    if let Ok(value) = http::HeaderValue::from_str(session_id) {
        headers.push((http::HeaderName::from_static("session_id"), value.clone()));
        headers.push((http::HeaderName::from_static("x-client-request-id"), value));
    }

    let window_id = format!("{session_id}:0");
    if let Ok(value) = http::HeaderValue::from_str(&window_id) {
        headers.push((http::HeaderName::from_static("x-codex-window-id"), value));
    }

    headers
}

fn reject_proxy_placeholder_for_managed_account_upstream(
    url: &str,
    headers: &http::HeaderMap,
) -> Result<(), ProxyError> {
    if !is_managed_account_upstream_url(url) || !headers_contain_proxy_placeholder(headers) {
        return Ok(());
    }

    Err(ProxyError::AuthError(
        "Managed account proxy auth was not resolved; PROXY_MANAGED must not be sent upstream"
            .to_string(),
    ))
}

fn is_managed_account_upstream_url(url: &str) -> bool {
    let Ok(uri) = url.parse::<http::Uri>() else {
        return false;
    };

    let Some(host) = uri.host().map(str::to_ascii_lowercase) else {
        return false;
    };

    host == "githubcopilot.com"
        || host.ends_with(".githubcopilot.com")
        || (host == "chatgpt.com" && uri.path().starts_with("/backend-api/codex"))
        || (host == "api.x.ai" && uri.path().starts_with("/v1/"))
}

fn headers_contain_proxy_placeholder(headers: &http::HeaderMap) -> bool {
    headers.values().any(|value| {
        value
            .to_str()
            .map(|value| value.contains(PROXY_AUTH_PLACEHOLDER))
            .unwrap_or(false)
    })
}

fn should_preserve_exact_header_case(
    adapter_name: &str,
    provider: &Provider,
    resolved_claude_api_format: Option<&str>,
    is_copilot: bool,
) -> bool {
    if matches!(adapter_name, "Codex" | "Gemini") {
        return false;
    }

    if is_copilot || provider.is_codex_oauth() || provider.is_xai_oauth() {
        return false;
    }

    matches!(resolved_claude_api_format, None | Some("anthropic"))
}

fn is_streaming_request(endpoint: &str, body: &Value, headers: &axum::http::HeaderMap) -> bool {
    if body
        .get("stream")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
    {
        return true;
    }

    if endpoint.contains("streamGenerateContent") || endpoint.contains("alt=sse") {
        return true;
    }

    headers
        .get(axum::http::header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(|accept| accept.contains("text/event-stream"))
        .unwrap_or(false)
}

#[cfg(test)]
fn should_force_identity_encoding(
    endpoint: &str,
    body: &Value,
    headers: &axum::http::HeaderMap,
) -> bool {
    is_streaming_request(endpoint, body, headers)
}

fn is_connect_failure_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "上游连接失败",
        "tcp connect failed",
        "proxy tcp connect failed",
        "tls handshake failed",
        "proxy tls handshake failed",
        "failed to connect",
        "trying to connect",
        "connection refused",
        "connection reset",
        "network is unreachable",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}

fn map_reqwest_send_error(error: reqwest::Error) -> ProxyError {
    if error.is_timeout() {
        ProxyError::Timeout(format!("上游请求超时: {}", error.without_url()))
    } else if error.is_connect() {
        ProxyError::ForwardFailed(format!("上游连接失败: {}", error.without_url()))
    } else {
        ProxyError::ForwardFailed(format!("上游请求发送失败: {}", error.without_url()))
    }
}

fn summarize_text_for_log(text: &str, max_chars: usize) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let trimmed = normalized.trim();

    if trimmed.chars().count() <= max_chars {
        return trimmed.to_string();
    }

    let truncated: String = trimmed.chars().take(max_chars).collect();
    let truncated = truncated.trim_end();
    format!("{truncated}...")
}

fn apply_local_proxy_body_overrides(
    body: &mut Value,
    overrides: &LocalProxyRequestOverrides,
) -> bool {
    let Some(override_body) = overrides.body.as_ref() else {
        return false;
    };

    if !override_body.is_object() {
        log::warn!("[LocalProxyOverrides] Ignoring body override because it is not an object");
        return false;
    }

    merge_json_override(body, override_body)
}

fn merge_json_override(target: &mut Value, patch: &Value) -> bool {
    merge_json_override_inner(target, patch, true)
}

fn merge_json_override_inner(target: &mut Value, patch: &Value, is_top_level: bool) -> bool {
    match (target, patch) {
        (Value::Object(target_map), Value::Object(patch_map)) => {
            let mut changed = false;
            for (key, patch_value) in patch_map {
                if is_top_level && key == "stream" {
                    log::warn!(
                        "[LocalProxyOverrides] Ignoring body override for protected field: stream"
                    );
                    continue;
                }
                match target_map.get_mut(key) {
                    Some(target_value) => {
                        changed |= merge_json_override_inner(target_value, patch_value, false);
                    }
                    None => {
                        target_map.insert(key.clone(), patch_value.clone());
                        changed = true;
                    }
                }
            }
            changed
        }
        (target_value, patch_value) => {
            if target_value == patch_value {
                false
            } else {
                *target_value = patch_value.clone();
                true
            }
        }
    }
}

fn apply_local_proxy_header_overrides(
    headers: &mut http::HeaderMap,
    overrides: Option<&LocalProxyRequestOverrides>,
    is_copilot: bool,
) {
    if is_copilot {
        return;
    }

    let Some(header_overrides) = overrides.map(|overrides| &overrides.headers) else {
        return;
    };

    for (raw_name, raw_value) in header_overrides {
        let header_name = raw_name.trim().to_ascii_lowercase();
        if header_name.is_empty() {
            log::warn!("[LocalProxyOverrides] Ignoring header override with empty name");
            continue;
        }

        let Ok(name) = http::HeaderName::from_bytes(header_name.as_bytes()) else {
            log::warn!("[LocalProxyOverrides] Ignoring invalid header override name: {raw_name}");
            continue;
        };

        if is_protected_local_proxy_override_header(&name) {
            log::debug!(
                "[LocalProxyOverrides] Ignoring protected header override: {}",
                name.as_str()
            );
            continue;
        }

        let Ok(value) = http::HeaderValue::from_str(raw_value) else {
            log::warn!(
                "[LocalProxyOverrides] Ignoring invalid header override value for {}",
                name.as_str()
            );
            continue;
        };

        headers.insert(name, value);
    }
}

fn is_protected_local_proxy_override_header(name: &http::HeaderName) -> bool {
    matches!(
        name.as_str(),
        "host"
            | "content-length"
            | "transfer-encoding"
            | "connection"
            | "proxy-authorization"
            | "proxy-authenticate"
            | "te"
            | "trailer"
            | "upgrade"
            | "accept-encoding"
            | "content-type"
            | "authorization"
            | "x-api-key"
            | "x-goog-api-key"
            | "chatgpt-account-id"
            | "session_id"
            | "x-client-request-id"
            | "x-codex-window-id"
            | "x-forwarded-host"
            | "x-forwarded-port"
            | "x-forwarded-proto"
            | "forwarded"
            | "cf-connecting-ip"
            | "cf-ipcountry"
            | "cf-ray"
            | "cf-visitor"
            | "true-client-ip"
            | "fastly-client-ip"
            | "x-azure-clientip"
            | "x-azure-fdid"
            | "x-azure-ref"
            | "akamai-origin-hop"
            | "x-akamai-config-log-detail"
            | "x-request-id"
            | "x-correlation-id"
            | "x-trace-id"
            | "x-amzn-trace-id"
            | "x-b3-traceid"
            | "x-b3-spanid"
            | "x-b3-parentspanid"
            | "x-b3-sampled"
            | "traceparent"
            | "tracestate"
    )
}

fn prepare_upstream_request_body(request_body: Value) -> Value {
    canonicalize_value(filter_private_params_with_whitelist(request_body, &[]))
}

fn log_prompt_cache_trace(
    app_type: &AppType,
    provider: &Provider,
    endpoint: &str,
    api_format: Option<&str>,
    body: &Value,
    session_client_provided: bool,
) {
    if !log::log_enabled!(log::Level::Debug) {
        return;
    }

    let prompt_cache_key = body
        .get("prompt_cache_key")
        .and_then(|value| value.as_str())
        .map(|key| format!("present(len={})", key.len()))
        .unwrap_or_else(|| "absent".to_string());
    let store = body
        .get("store")
        .map(value_for_log)
        .unwrap_or_else(|| "absent".to_string());
    let stream = body
        .get("stream")
        .map(value_for_log)
        .unwrap_or_else(|| "absent".to_string());
    let cache_controls = cache_control_summary(body);

    log::debug!(
        "[CacheTrace] app={}, provider={}, endpoint={}, api_format={}, session_client_provided={}, prompt_cache_key={}, store={}, stream={}, instructions_hash={}, system_hash={}, tools_hash={}, input_hash={}, messages_hash={}, include_hash={}, cache_controls={}, body_hash={}",
        app_type.as_str(),
        provider.id,
        // Gemini 的 endpoint 带 ?key=<API_KEY>；脱敏剥掉 query 再落盘。
        crate::redact_url_for_log(endpoint),
        api_format.unwrap_or("native"),
        session_client_provided,
        prompt_cache_key,
        store,
        stream,
        short_value_hash(body.get("instructions")),
        short_value_hash(body.get("system")),
        short_value_hash(body.get("tools")),
        short_value_hash(body.get("input")),
        short_value_hash(body.get("messages")),
        short_value_hash(body.get("include")),
        cache_controls,
        short_value_hash(Some(body)),
    );
}

fn cache_control_summary(value: &Value) -> String {
    fn walk(value: &Value, count: &mut usize, ttls: &mut std::collections::BTreeSet<String>) {
        match value {
            Value::Object(object) => {
                if let Some(cache_control) = object.get("cache_control") {
                    *count += 1;
                    let ttl = cache_control
                        .get("ttl")
                        .and_then(Value::as_str)
                        .unwrap_or("default");
                    ttls.insert(ttl.to_string());
                }
                for child in object.values() {
                    walk(child, count, ttls);
                }
            }
            Value::Array(items) => {
                for child in items {
                    walk(child, count, ttls);
                }
            }
            _ => {}
        }
    }

    let mut count = 0;
    let mut ttls = std::collections::BTreeSet::new();
    walk(value, &mut count, &mut ttls);
    format!(
        "count={count},ttls={}",
        if ttls.is_empty() {
            "none".to_string()
        } else {
            ttls.into_iter().collect::<Vec<_>>().join("|")
        }
    )
}

fn value_for_log(value: &Value) -> String {
    match value {
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Null => "null".to_string(),
        Value::Array(values) => format!("array(len={})", values.len()),
        Value::Object(values) => format!("object(len={})", values.len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use crate::error::AppError;
    use crate::provider::LocalProxyRequestOverrides;
    use axum::http::header::{HeaderValue, ACCEPT};
    use axum::http::HeaderMap;
    use bytes::Bytes;
    use http::StatusCode;
    use serde_json::json;
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn test_provider_with_type(provider_type: Option<&str>) -> Provider {
        Provider {
            id: "provider-1".to_string(),
            name: "Provider 1".to_string(),
            settings_config: json!({}),
            website_url: None,
            category: None,
            created_at: None,
            sort_index: None,
            notes: None,
            meta: provider_type.map(|value| crate::provider::ProviderMeta {
                provider_type: Some(value.to_string()),
                ..Default::default()
            }),
            icon: None,
            icon_color: None,
            in_failover_queue: false,
        }
    }

    fn test_forwarder(
        non_streaming_timeout: Duration,
        streaming_first_byte_timeout: Duration,
    ) -> RequestForwarder {
        let db = Arc::new(Database::memory().expect("memory db"));

        RequestForwarder {
            router: Arc::new(ProviderRouter::new(db.clone())),
            db: db.clone(),
            usage_logging_enabled: true,
            status: Arc::new(RwLock::new(ProxyStatus::default())),
            current_providers: Arc::new(RwLock::new(HashMap::new())),
            gemini_shadow: Arc::new(GeminiShadowStore::new()),
            codex_chat_history: Arc::new(CodexChatHistoryStore::default()),
            failover_manager: Arc::new(FailoverSwitchManager::new(db)),
            app_handle: None,
            current_provider_id_at_start: String::new(),
            session_id: String::new(),
            session_client_provided: false,
            rectifier_config: RectifierConfig::default(),
            optimizer_config: OptimizerConfig::default(),
            copilot_optimizer_config: CopilotOptimizerConfig::default(),
            non_streaming_timeout,
            streaming_first_byte_timeout,
            max_attempts: 1,
        }
    }
    async fn spawn_usage_error_server(
        response_body: Value,
        response_count: usize,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let response_body = response_body.to_string();
        let handle = tokio::spawn(async move {
            for _ in 0..response_count {
                let (mut socket, _) = listener.accept().await.expect("accept test request");
                let mut request = [0u8; 8192];
                socket.read(&mut request).await.expect("read test request");
                let response = format!(
                    "HTTP/1.1 503 Service Unavailable\r\n\
                     Content-Type: application/json\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                socket
                    .write_all(response.as_bytes())
                    .await
                    .expect("write test response");
                socket.shutdown().await.expect("close test response");
            }
        });
        (format!("http://{address}"), handle)
    }

    #[test]
    fn failed_attempt_usage_parses_nested_responses_payload() {
        let error = ProxyError::TransformErrorWithBody {
            message: "Responses upstream server_error: boom".to_string(),
            body: json!({
                "type": "response.failed",
                "response": {
                    "id": "resp_failed",
                    "model": "gpt-5.6",
                    "status": "failed",
                    "usage": {
                        "input_tokens": 120,
                        "output_tokens": 8,
                        "input_tokens_details": { "cached_tokens": 20 }
                    },
                    "error": { "type": "server_error", "message": "boom" }
                }
            })
            .to_string(),
        };

        let usage = parse_failed_attempt_usage(&error).expect("failed response usage");
        assert_eq!(usage.input_tokens, 120);
        assert_eq!(usage.output_tokens, 8);
        assert_eq!(usage.cache_read_tokens, 20);
        assert_eq!(usage.model.as_deref(), Some("gpt-5.6"));
        assert_eq!(usage.message_id.as_deref(), Some("resp_failed"));
    }

    #[tokio::test]
    async fn same_provider_retries_log_each_failed_attempt_usage() -> Result<(), AppError> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let response = json!({
            "type": "error",
            "error": { "type": "overloaded_error", "message": "retry later" },
            "id": "msg_failed",
            "model": "claude-test",
            "usage": { "input_tokens": 11, "output_tokens": 2 }
        });
        let (base_url, server) = spawn_usage_error_server(response, 2).await;
        let mut provider = test_provider_with_type(None);
        provider.settings_config = json!({
            "env": {
                "ANTHROPIC_BASE_URL": base_url,
                "ANTHROPIC_AUTH_TOKEN": "test-key"
            }
        });
        let forwarder = test_forwarder(Duration::from_secs(5), Duration::from_secs(5));
        let adapter = get_adapter(&AppType::Claude).expect("Claude adapter");
        let mut permit = ProviderAttemptPermit::new(
            forwarder.router.clone(),
            &provider.id,
            AppType::Claude.as_str(),
            false,
        );
        let result = forwarder
            .execute_provider_slot(
                &AppType::Claude,
                http::Method::POST,
                "/v1/messages",
                &json!({
                    "model": "claude-test",
                    "stream": false,
                    "max_tokens": 8,
                    "messages": [{ "role": "user", "content": "hi" }]
                }),
                &HeaderMap::new(),
                &Extensions::new(),
                &provider,
                adapter.as_ref(),
                ProviderRetryPolicy {
                    retry_count: 1,
                    per_attempt_timeout_seconds: 5,
                    retry_wait_seconds: 0,
                    unlimited_retries: false,
                },
                &mut permit,
            )
            .await;

        assert!(matches!(
            result,
            Err(ProviderSlotFailure {
                error: ProxyError::UpstreamError { status: 503, .. },
                ..
            })
        ));
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("same-provider retry did not issue two requests")
            .expect("test server completed");

        let conn = crate::database::lock_conn!(forwarder.db.conn);
        let totals: (i64, i64, i64, f64) = conn.query_row(
            "SELECT COUNT(*), SUM(input_tokens), SUM(output_tokens),
                    SUM(CAST(total_cost_usd AS REAL))
             FROM proxy_request_logs
             WHERE request_id LIKE 'provider-attempt:%'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(totals, (2, 22, 4, 0.0));
        Ok(())
    }

    #[test]
    fn finite_provider_attempt_indices_match_retry_count() {
        let policy = crate::provider::ProviderRetryPolicy {
            retry_count: 2,
            ..Default::default()
        };

        assert_eq!(
            RequestForwarder::provider_attempt_indices(policy).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn unlimited_provider_attempt_indices_continue_without_finite_budget() {
        let policy = crate::provider::ProviderRetryPolicy {
            unlimited_retries: true,
            ..Default::default()
        };

        assert_eq!(
            RequestForwarder::provider_attempt_indices(policy)
                .take(4)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
    }

    #[test]
    fn single_provider_retryable_log_uses_single_provider_code() {
        let error = ProxyError::UpstreamError {
            status: 429,
            body: Some(r#"{"error":{"message":"rate limit exceeded"}}"#.to_string()),
        };

        let (code, message) = build_retryable_failure_log("PackyCode-response", 1, 1, &error);

        assert_eq!(code, log_fwd::SINGLE_PROVIDER_FAILED);
        assert!(message.contains("Provider PackyCode-response 请求失败"));
        assert!(message.contains("上游 HTTP 429"));
        // 上游错误消息保留(截断)，用于诊断失败原因。
        assert!(message.contains("rate limit exceeded"));
        assert!(!message.contains("切换下一个"));
    }

    #[test]
    fn multi_provider_retryable_log_keeps_failover_wording() {
        let error = ProxyError::Timeout("upstream timed out after 30s".to_string());

        let (code, message) = build_retryable_failure_log("primary", 1, 3, &error);

        assert_eq!(code, log_fwd::PROVIDER_FAILED_RETRY);
        assert!(message.contains("继续尝试下一个 (1/3)"));
        assert!(message.contains("请求超时"));
    }

    #[test]
    fn single_provider_has_no_terminal_all_failed_log() {
        assert!(build_terminal_failure_log(1, 1, None).is_none());
    }

    #[test]
    fn multi_provider_terminal_log_contains_last_error_summary() {
        let error = ProxyError::ForwardFailed("connection reset by peer".to_string());

        let (code, message) =
            build_terminal_failure_log(2, 2, Some(&error)).expect("expected terminal log");

        assert_eq!(code, log_fwd::ALL_PROVIDERS_FAILED);
        assert!(message.contains("已尝试 2/2 个 Provider，均失败"));
        assert!(message.contains("connection reset by peer"));
    }

    #[test]
    fn summarize_text_for_log_collapses_whitespace_and_truncates() {
        let summary = summarize_text_for_log("line1\n\n line2   line3", 12);

        assert_eq!(summary, "line1 line2...");
    }

    #[test]
    fn canonical_json_sorts_object_keys_for_cache_trace_hashes() {
        let left = json!({
            "tools": [
                {
                    "parameters": {
                        "properties": {
                            "b": {"type": "string"},
                            "a": {"type": "number"}
                        },
                        "type": "object"
                    },
                    "name": "lookup"
                }
            ]
        });
        let right = json!({
            "tools": [
                {
                    "name": "lookup",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "a": {"type": "number"},
                            "b": {"type": "string"}
                        }
                    }
                }
            ]
        });

        assert_eq!(
            crate::proxy::json_canonical::canonical_json_string(&left),
            crate::proxy::json_canonical::canonical_json_string(&right)
        );
        assert_eq!(
            short_value_hash(Some(&left)),
            short_value_hash(Some(&right))
        );
    }

    #[test]
    fn prepare_upstream_request_body_filters_private_fields_and_canonicalizes_order() {
        let body = json!({
            "z": 1,
            "_internal": "drop",
            "tools": [
                {
                    "name": "lookup",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "_id": {
                                "_private_note": "drop",
                                "type": "string"
                            },
                            "b": {"type": "number"},
                            "a": {"type": "string"}
                        }
                    }
                }
            ],
            "a": 2
        });

        let prepared = prepare_upstream_request_body(body);

        assert!(prepared.get("_internal").is_none());
        assert!(prepared["tools"][0]["parameters"]["properties"]
            .get("_id")
            .is_some());
        assert!(prepared["tools"][0]["parameters"]["properties"]["_id"]
            .get("_private_note")
            .is_none());
        assert_eq!(
            serde_json::to_string(&prepared).unwrap(),
            r#"{"a":2,"tools":[{"name":"lookup","parameters":{"properties":{"_id":{"type":"string"},"a":{"type":"string"},"b":{"type":"number"}},"type":"object"}}],"z":1}"#
        );
    }

    #[test]
    fn local_proxy_body_overrides_deep_merge_final_body_without_stream() {
        let mut body = json!({
            "model": "before",
            "stream": false,
            "metadata": {
                "keep": true,
                "temperature": 1
            },
            "messages": [{ "role": "user", "content": "hello" }]
        });
        let overrides = LocalProxyRequestOverrides {
            headers: HashMap::new(),
            body: Some(json!({
                "model": "after",
                "stream": true,
                "metadata": {
                    "temperature": 0.2,
                    "top_p": 0.9
                },
                "messages": []
            })),
        };

        assert!(apply_local_proxy_body_overrides(&mut body, &overrides));

        assert_eq!(body["model"], "after");
        assert_eq!(body["stream"], false);
        assert_eq!(body["metadata"]["keep"], true);
        assert_eq!(body["metadata"]["temperature"], 0.2);
        assert_eq!(body["metadata"]["top_p"], 0.9);
        assert_eq!(body["messages"], json!([]));
    }

    #[test]
    fn local_proxy_header_overrides_replace_allowed_headers_only() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::USER_AGENT,
            http::HeaderValue::from_static("original"),
        );
        headers.insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_static("Bearer good"),
        );
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );

        let overrides = LocalProxyRequestOverrides {
            headers: HashMap::from([
                ("User-Agent".to_string(), "custom".to_string()),
                ("X-Test".to_string(), "ok".to_string()),
                ("Authorization".to_string(), "Bearer bad".to_string()),
                ("Content-Type".to_string(), "text/plain".to_string()),
                ("X-Bad".to_string(), "bad\nvalue".to_string()),
            ]),
            body: None,
        };

        apply_local_proxy_header_overrides(&mut headers, Some(&overrides), false);

        assert_eq!(
            headers
                .get(http::header::USER_AGENT)
                .and_then(|value| value.to_str().ok()),
            Some("custom")
        );
        assert_eq!(
            headers
                .get(http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer good")
        );
        assert_eq!(
            headers
                .get(http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
        assert_eq!(
            headers.get("x-test").and_then(|value| value.to_str().ok()),
            Some("ok")
        );
        assert!(headers.get("x-bad").is_none());
    }

    #[test]
    fn local_proxy_header_overrides_are_skipped_for_copilot() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::USER_AGENT,
            http::HeaderValue::from_static("copilot"),
        );
        let overrides = LocalProxyRequestOverrides {
            headers: HashMap::from([("User-Agent".to_string(), "custom".to_string())]),
            body: None,
        };

        apply_local_proxy_header_overrides(&mut headers, Some(&overrides), true);

        assert_eq!(
            headers
                .get(http::header::USER_AGENT)
                .and_then(|value| value.to_str().ok()),
            Some("copilot")
        );
    }

    #[tokio::test]
    async fn non_streaming_success_is_buffered_before_marking_provider_successful() {
        let forwarder = test_forwarder(Duration::from_secs(1), Duration::from_secs(1));
        let response = ProxyResponse::streamed(
            StatusCode::OK,
            HeaderMap::new(),
            futures::stream::once(async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok::<Bytes, std::io::Error>(Bytes::from_static(b"{\"ok\":true}"))
            }),
        );

        let prepared = forwarder
            .prepare_success_response_for_failover(response, false, None, false)
            .await
            .expect("response should be buffered");

        assert_eq!(
            prepared
                .bytes_with_limit(MAX_RESPONSE_BODY_BYTES)
                .await
                .unwrap(),
            Bytes::from_static(b"{\"ok\":true}")
        );
    }

    #[tokio::test]
    async fn non_streaming_body_read_error_is_retryable_before_success_record() {
        let forwarder = test_forwarder(Duration::from_secs(1), Duration::from_secs(1));
        let response = ProxyResponse::streamed(
            StatusCode::OK,
            HeaderMap::new(),
            futures::stream::once(async {
                Err::<Bytes, std::io::Error>(std::io::Error::other("body boom"))
            }),
        );

        let err = match forwarder
            .prepare_success_response_for_failover(response, false, None, false)
            .await
        {
            Ok(_) => panic!("body read errors should fail the attempt"),
            Err(err) => err,
        };

        assert!(matches!(err, ProxyError::ForwardFailed(_)));
    }

    #[tokio::test]
    async fn streaming_success_primes_first_chunk_and_replays_it() {
        let forwarder = test_forwarder(Duration::from_secs(1), Duration::from_secs(1));
        let response = ProxyResponse::streamed(
            StatusCode::OK,
            HeaderMap::new(),
            futures::stream::iter(vec![
                Ok::<Bytes, std::io::Error>(Bytes::from_static(b"first")),
                Ok::<Bytes, std::io::Error>(Bytes::from_static(b"second")),
            ]),
        );

        let prepared = forwarder
            .prepare_success_response_for_failover(response, true, None, false)
            .await
            .expect("stream should be primed");

        assert_eq!(
            prepared
                .bytes_with_limit(MAX_RESPONSE_BODY_BYTES)
                .await
                .unwrap(),
            Bytes::from_static(b"firstsecond")
        );
    }

    #[tokio::test]
    async fn streaming_first_chunk_error_is_retryable_before_success_record() {
        let forwarder = test_forwarder(Duration::from_secs(1), Duration::from_secs(1));
        let response = ProxyResponse::streamed(
            StatusCode::OK,
            HeaderMap::new(),
            futures::stream::once(async {
                Err::<Bytes, std::io::Error>(std::io::Error::other("first chunk boom"))
            }),
        );

        let err = match forwarder
            .prepare_success_response_for_failover(response, true, None, false)
            .await
        {
            Ok(_) => panic!("first chunk errors should fail the attempt"),
            Err(err) => err,
        };

        assert!(matches!(err, ProxyError::ForwardFailed(_)));
    }

    #[test]
    fn codex_oauth_session_headers_match_codex_cache_identity() {
        let headers = build_codex_oauth_session_headers("session-123");
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.insert(name, value);
        }

        assert_eq!(
            map.get("session_id"),
            Some(&HeaderValue::from_static("session-123"))
        );
        assert_eq!(
            map.get("x-client-request-id"),
            Some(&HeaderValue::from_static("session-123"))
        );
        assert_eq!(
            map.get("x-codex-window-id"),
            Some(&HeaderValue::from_static("session-123:0"))
        );
    }

    #[test]
    fn managed_account_upstream_rejects_proxy_managed_placeholder_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer PROXY_MANAGED"),
        );

        let err = reject_proxy_placeholder_for_managed_account_upstream(
            "https://api.githubcopilot.com/chat/completions",
            &headers,
        )
        .expect_err("placeholder should be rejected before upstream");

        assert!(matches!(
            err,
            ProxyError::AuthError(message) if message.contains("PROXY_MANAGED")
        ));

        let xai_err = reject_proxy_placeholder_for_managed_account_upstream(
            "https://api.x.ai/v1/responses",
            &headers,
        )
        .expect_err("xAI placeholder should be rejected before upstream");
        assert!(matches!(
            xai_err,
            ProxyError::AuthError(message) if message.contains("PROXY_MANAGED")
        ));
    }

    #[test]
    fn codex_oauth_upstream_rejects_proxy_managed_placeholder_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer PROXY_MANAGED"),
        );

        let err = reject_proxy_placeholder_for_managed_account_upstream(
            "https://chatgpt.com/backend-api/codex/responses",
            &headers,
        )
        .expect_err("placeholder should be rejected before upstream");

        assert!(matches!(
            err,
            ProxyError::AuthError(message) if message.contains("PROXY_MANAGED")
        ));
    }

    #[test]
    fn non_managed_upstream_allows_proxy_managed_placeholder_guard() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer PROXY_MANAGED"),
        );

        reject_proxy_placeholder_for_managed_account_upstream(
            "https://api.example.com/v1/messages",
            &headers,
        )
        .expect("guard is scoped to managed-account upstreams");
    }

    #[test]
    fn exact_header_case_preserved_for_native_claude_only() {
        let provider = test_provider_with_type(None);

        assert!(should_preserve_exact_header_case(
            "Claude",
            &provider,
            Some("anthropic"),
            false
        ));
        assert!(!should_preserve_exact_header_case(
            "Claude",
            &provider,
            Some("openai_responses"),
            false
        ));
        assert!(!should_preserve_exact_header_case(
            "Codex", &provider, None, false
        ));
        assert!(!should_preserve_exact_header_case(
            "Gemini", &provider, None, false
        ));
    }

    #[test]
    fn exact_header_case_skipped_for_codex_oauth_and_copilot() {
        let codex_oauth = test_provider_with_type(Some("codex_oauth"));
        let copilot = test_provider_with_type(Some("github_copilot"));

        assert!(!should_preserve_exact_header_case(
            "Claude",
            &codex_oauth,
            Some("openai_responses"),
            false
        ));
        assert!(!should_preserve_exact_header_case(
            "Claude",
            &copilot,
            Some("openai_chat"),
            true
        ));
    }

    #[test]
    fn rewrite_claude_transform_endpoint_strips_beta_for_chat_completions() {
        let (endpoint, passthrough_query) = rewrite_claude_transform_endpoint(
            "/v1/messages?beta=true&foo=bar",
            "openai_chat",
            false,
            &json!({ "model": "gpt-5.4" }),
        );

        assert_eq!(endpoint, "/v1/chat/completions?foo=bar");
        assert_eq!(passthrough_query.as_deref(), Some("foo=bar"));
    }

    #[test]
    fn rewrite_claude_transform_endpoint_strips_beta_for_responses() {
        let (endpoint, passthrough_query) = rewrite_claude_transform_endpoint(
            "/claude/v1/messages?beta=true&x-id=1",
            "openai_responses",
            false,
            &json!({ "model": "gpt-5.4" }),
        );

        assert_eq!(endpoint, "/v1/responses?x-id=1");
        assert_eq!(passthrough_query.as_deref(), Some("x-id=1"));
    }

    #[test]
    fn rewrite_codex_responses_endpoint_to_chat_preserves_query() {
        let (endpoint, passthrough_query) =
            rewrite_codex_responses_endpoint_to_chat("/v1/responses?foo=bar");

        assert_eq!(endpoint, "/chat/completions?foo=bar");
        assert_eq!(passthrough_query.as_deref(), Some("foo=bar"));
    }

    #[test]
    fn prepend_claude_code_system_prompt_from_string() {
        let mut body = json!({ "system": "You are a Codex agent." });
        prepend_claude_code_system_prompt(&mut body);
        let system = body["system"].as_array().unwrap();
        assert_eq!(system[0]["text"], CLAUDE_CODE_SYSTEM_IDENTITY);
        assert_eq!(system[1]["text"], "You are a Codex agent.");
    }

    #[test]
    fn prepend_claude_code_system_prompt_when_absent() {
        let mut body = json!({});
        prepend_claude_code_system_prompt(&mut body);
        let system = body["system"].as_array().unwrap();
        assert_eq!(system.len(), 1);
        assert_eq!(system[0]["text"], CLAUDE_CODE_SYSTEM_IDENTITY);
    }

    #[test]
    fn prepend_claude_code_system_prompt_is_idempotent() {
        let mut body = json!({ "system": "orig" });
        prepend_claude_code_system_prompt(&mut body);
        prepend_claude_code_system_prompt(&mut body);
        let system = body["system"].as_array().unwrap();
        assert_eq!(system.len(), 2);
        assert_eq!(system[0]["text"], CLAUDE_CODE_SYSTEM_IDENTITY);
        assert_eq!(system[1]["text"], "orig");
    }

    #[test]
    fn rewrite_codex_responses_endpoint_to_anthropic_preserves_query() {
        let (endpoint, passthrough_query) =
            rewrite_codex_responses_endpoint_to_anthropic("/responses?x=1");
        assert_eq!(endpoint, "/v1/messages?x=1");
        assert_eq!(passthrough_query.as_deref(), Some("x=1"));

        let (endpoint, _) = rewrite_codex_responses_endpoint_to_anthropic("/v1/responses");
        assert_eq!(endpoint, "/v1/messages");
    }

    #[test]
    fn codex_anthropic_full_endpoint_guard_avoids_double_messages() {
        // On the Codex→Anthropic path a base URL already ending in `/v1/messages` (switch
        // off) must be treated as a full endpoint by the real `base_url_is_full_endpoint`.

        // Without the guard, build_url would concatenate the pasted endpoint with the
        // rewritten `/v1/messages` target, producing a broken double suffix.
        use super::super::providers::ProviderAdapter;
        let doubled = super::super::providers::CodexAdapter::new()
            .build_url("https://host.example/v1/messages", "/v1/messages");
        assert_eq!(doubled, "https://host.example/v1/messages/v1/messages");

        // With the guard, the pasted URL is used verbatim (plus preserved query). Includes
        // query/fragment/whitespace suffixes, which must not hide the endpoint (fix: a base
        // like `.../v1/messages?beta=true` previously evaded the suffix check).
        for base in [
            "https://host.example/v1/messages",
            "https://host.example/v1/messages/",
            "https://host.example/api/v1/messages", // prefixed gateway
            "https://host.example/v1/messages?beta=true",
            "https://host.example/v1/messages/?beta=true",
            "https://host.example/v1/messages#frag",
            "  https://host.example/v1/messages  ",
        ] {
            assert!(
                base_url_is_full_endpoint(base, "/v1/messages"),
                "expected full-endpoint match: {base:?}"
            );
        }
        assert_eq!(
            append_query_to_full_url("https://host.example/v1/messages", Some("x=1")),
            "https://host.example/v1/messages?x=1"
        );
        // A base URL that already carries its own query is preserved verbatim (no double
        // `/v1/messages`, query kept).
        assert_eq!(
            append_query_to_full_url("https://host.example/v1/messages?beta=true", None),
            "https://host.example/v1/messages?beta=true"
        );

        // A non-endpoint base (origin/prefix) must NOT match, so build_url still appends.
        assert!(!base_url_is_full_endpoint(
            "https://host.example",
            "/v1/messages"
        ));
        assert!(!base_url_is_full_endpoint(
            "https://host.example/v1",
            "/v1/messages"
        ));
        // The shared helper also backs the Chat path's `/chat/completions` guard.
        assert!(base_url_is_full_endpoint(
            "https://host.example/v1/chat/completions?api-version=2024",
            "/chat/completions"
        ));
    }

    #[test]
    fn codex_client_fingerprint_headers_are_dropped_for_anthropic_upstreams() {
        // Codex/OpenAI fingerprints a native Claude Code client never sends → must drop.
        for header in [
            "originator",
            "session_id",
            "session-id",
            "thread-id",
            "conversation_id",
            "chatgpt-account-id",
            "x-openai-subagent",
            "x-client-request-id",
            "x-codex-window-id",
            "openai-beta",
            "openai-organization",
            "openai-project",
            "x-stainless-lang",
            "x-stainless-runtime",
            "x-codex-turn-id",
        ] {
            assert!(
                is_codex_client_fingerprint_header(header),
                "expected {header} to be dropped while impersonating Claude Code"
            );
        }

        // Headers a real Claude Code client sends (or that the forwarder rebuilds) must
        // NOT be caught by the denylist.
        for header in [
            "anthropic-version",
            "anthropic-beta",
            "user-agent",
            "accept",
            "content-type",
            "x-app",
        ] {
            assert!(
                !is_codex_client_fingerprint_header(header),
                "{header} must be preserved while impersonating Claude Code"
            );
        }
    }

    #[test]
    fn codex_anthropic_2xx_error_envelope_is_detected_for_failover() {
        let body = br#"{"type":"error","error":{"type":"overloaded_error","message":"busy"}}"#;
        assert_eq!(
            codex_anthropic_error_envelope_message(body).as_deref(),
            Some("overloaded_error: busy")
        );
        assert!(
            codex_anthropic_error_envelope_message(br#"{"type":"message","content":[]}"#).is_none()
        );
    }

    #[test]
    fn responses_2xx_failure_is_detected_for_failover() {
        assert_eq!(
            responses_error_envelope_message(
                br#"{"status":"failed","error":{"type":"server_error","message":"busy"},"output":[]}"#
            )
            .as_deref(),
            Some("server_error: busy")
        );
        assert_eq!(
            responses_error_envelope_message(br#"{"status":"cancelled","output":[]}"#).as_deref(),
            Some("cancelled: response generation was cancelled")
        );
        assert!(responses_error_envelope_message(
            br#"{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[]}"#
        )
        .is_none());
        assert!(responses_error_envelope_message(
            br#"{"status":"completed","error":null,"output":[]}"#
        )
        .is_none());
    }

    #[test]
    fn responses_stream_start_semantic_failure_is_retryable() {
        let created = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}"
        );
        assert!(inspect_responses_start_event(created).is_none());

        let failed = concat!(
            "event: response.failed\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"server_error\",\"message\":\"boom\"}}}"
        );
        assert!(matches!(
            inspect_responses_start_event(failed),
            Some(Err(ProxyError::TransformErrorWithBody { message, body }))
                if message.contains("boom") && body.contains("response.failed")
        ));

        let delta = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}"
        );
        assert!(matches!(inspect_responses_start_event(delta), Some(Ok(()))));
    }

    #[test]
    fn responses_stream_start_accepts_unlabelled_whole_json() {
        assert!(matches!(
            inspect_responses_json_document(
                r#"{
                    "status": "completed",

                    "output": []
                }"#
            ),
            Some(Ok(()))
        ));
        assert!(inspect_responses_json_document(r#"{"status":"completed""#).is_none());

        let failed = inspect_responses_json_document(
            r#"{"status":"failed","error":{"message":"backend unavailable"}}"#,
        );
        assert!(matches!(
            failed,
            Some(Err(ProxyError::TransformErrorWithBody { message, body }))
                if message.contains("backend unavailable") && body.contains("\"status\":\"failed\"")
        ));
    }

    #[test]
    fn codex_anthropic_cache_is_default_on_but_honors_sub_switch() {
        let default = codex_anthropic_cache_config(&OptimizerConfig::default());
        assert!(default.enabled);
        assert!(default.cache_injection);

        let disabled = codex_anthropic_cache_config(&OptimizerConfig {
            cache_injection: false,
            ..OptimizerConfig::default()
        });
        assert!(disabled.enabled);
        assert!(!disabled.cache_injection);
    }

    #[test]
    fn invalid_client_history_is_not_retryable() {
        let forwarder = test_forwarder(Duration::ZERO, Duration::ZERO);
        let provider = test_provider_with_type(None);
        assert_eq!(
            forwarder.categorize_proxy_error(
                &ProxyError::InvalidRequest("invalid historical tool arguments".to_string()),
                &provider,
            ),
            ErrorCategory::NonRetryable
        );
    }

    #[test]
    fn official_codex_failures_are_not_retryable() {
        let forwarder = test_forwarder(Duration::ZERO, Duration::ZERO);
        let mut provider = test_provider_with_type(None);
        provider.id = "codex-official".to_string();
        provider.category = Some("official".to_string());

        for error in [
            ProxyError::AuthError("restart Codex".to_string()),
            ProxyError::UpstreamError {
                status: 401,
                body: None,
            },
            ProxyError::UpstreamError {
                status: 403,
                body: None,
            },
            ProxyError::UpstreamError {
                status: 429,
                body: None,
            },
            ProxyError::Timeout("timeout".to_string()),
        ] {
            assert_eq!(
                forwarder.categorize_proxy_error(&error, &provider),
                ErrorCategory::NonRetryable
            );
        }
    }

    #[test]
    fn xai_oauth_token_auth_failures_are_not_retryable() {
        let forwarder = test_forwarder(Duration::ZERO, Duration::ZERO);
        let provider = test_provider_with_type(Some("xai_oauth"));

        // 本地取 token 失败 = 账号级问题（需重新登录），failover 无济于事
        assert_eq!(
            forwarder.categorize_proxy_error(
                &ProxyError::AuthError("xAI OAuth 认证失败".to_string()),
                &provider,
            ),
            ErrorCategory::NonRetryable
        );
        // 上游 401/403 保持 Retryable：换 provider 可能持有可用的 key
        assert_eq!(
            forwarder.categorize_proxy_error(
                &ProxyError::UpstreamError {
                    status: 401,
                    body: None,
                },
                &provider,
            ),
            ErrorCategory::Retryable
        );
    }

    #[test]
    fn official_codex_rejects_stale_proxy_placeholder_with_restart_hint() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer PROXY_MANAGED"),
        );
        let mut provider = test_provider_with_type(None);
        provider.id = "codex-official".to_string();
        provider.category = Some("official".to_string());
        let error = validate_codex_official_authorization(&headers, &provider)
            .expect_err("stale placeholder must be rejected");
        assert!(matches!(error, ProxyError::AuthError(message) if message.contains("重启 Codex")));
    }

    #[test]
    fn managed_codex_official_rejects_a_different_session_account() {
        let mut provider = test_provider_with_type(Some("codex_oauth"));
        provider.category = Some("official".to_string());
        provider.meta.as_mut().expect("provider meta").auth_binding =
            Some(crate::provider::AuthBinding {
                source: crate::provider::AuthBindingSource::ManagedAccount,
                auth_provider: Some("codex_oauth".to_string()),
                account_id: Some("account-b".to_string()),
            });

        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer account-a-token"),
        );
        headers.insert("chatgpt-account-id", HeaderValue::from_static("account-a"));
        let error = validate_codex_official_authorization(&headers, &provider)
            .expect_err("a stale Codex session must not cross the account boundary");
        assert!(matches!(error, ProxyError::AuthError(message) if message.contains("重启 Codex")));

        headers.insert("chatgpt-account-id", HeaderValue::from_static("account-b"));
        validate_codex_official_authorization(&headers, &provider)
            .expect("the selected account may pass through");
    }

    #[test]
    fn rewrite_codex_responses_compact_endpoint_to_chat_preserves_query() {
        let (endpoint, passthrough_query) =
            rewrite_codex_responses_endpoint_to_chat("/v1/responses/compact?foo=bar");

        assert_eq!(endpoint, "/chat/completions?foo=bar");
        assert_eq!(passthrough_query.as_deref(), Some("foo=bar"));
    }

    #[test]
    fn rewrite_claude_transform_endpoint_uses_copilot_path() {
        let (endpoint, passthrough_query) = rewrite_claude_transform_endpoint(
            "/v1/messages?beta=true&x-id=1",
            "anthropic",
            true,
            &json!({ "model": "claude-sonnet-4-6" }),
        );

        assert_eq!(endpoint, "/chat/completions?x-id=1");
        assert_eq!(passthrough_query.as_deref(), Some("x-id=1"));
    }

    #[test]
    fn rewrite_claude_transform_endpoint_uses_copilot_responses_path() {
        let (endpoint, passthrough_query) = rewrite_claude_transform_endpoint(
            "/v1/messages?beta=true&x-id=1",
            "openai_responses",
            true,
            &json!({ "model": "gpt-5.4" }),
        );

        assert_eq!(endpoint, "/v1/responses?x-id=1");
        assert_eq!(passthrough_query.as_deref(), Some("x-id=1"));
    }

    #[test]
    fn rewrite_claude_transform_endpoint_maps_gemini_generate_content() {
        let (endpoint, passthrough_query) = rewrite_claude_transform_endpoint(
            "/v1/messages?beta=true&x-id=1",
            "gemini_native",
            false,
            &json!({ "model": "gemini-2.5-pro" }),
        );

        assert_eq!(
            endpoint,
            "/v1beta/models/gemini-2.5-pro:generateContent?x-id=1"
        );
        assert_eq!(passthrough_query.as_deref(), Some("x-id=1"));
    }

    /// Regression: body.model arriving as the resource-name form
    /// `models/gemini-2.5-pro` must not produce a doubled
    /// `/v1beta/models/models/...` path.
    #[test]
    fn rewrite_claude_transform_endpoint_strips_gemini_model_resource_prefix() {
        let (endpoint, _) = rewrite_claude_transform_endpoint(
            "/v1/messages",
            "gemini_native",
            false,
            &json!({ "model": "models/gemini-2.5-pro" }),
        );

        assert_eq!(endpoint, "/v1beta/models/gemini-2.5-pro:generateContent");
    }

    #[test]
    fn rewrite_claude_transform_endpoint_maps_gemini_streaming() {
        let (endpoint, passthrough_query) = rewrite_claude_transform_endpoint(
            "/v1/messages?beta=true",
            "gemini_native",
            false,
            &json!({ "model": "gemini-2.5-flash", "stream": true }),
        );

        assert_eq!(
            endpoint,
            "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse"
        );
        assert_eq!(passthrough_query.as_deref(), Some("alt=sse"));
    }

    #[test]
    fn append_query_to_full_url_preserves_existing_query_string() {
        let url = append_query_to_full_url("https://relay.example/api?foo=bar", Some("x-id=1"));

        assert_eq!(url, "https://relay.example/api?foo=bar&x-id=1");
    }

    #[test]
    fn build_gemini_native_url_uses_origin_when_base_ends_with_v1beta() {
        let url = crate::proxy::gemini_url::build_gemini_native_url(
            "https://generativelanguage.googleapis.com/v1beta",
            "/v1beta/models/gemini-2.5-pro:generateContent",
        );

        assert_eq!(
            url,
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent"
        );
    }

    #[test]
    fn build_gemini_native_url_uses_origin_when_base_already_contains_models_prefix() {
        let url = crate::proxy::gemini_url::build_gemini_native_url(
            "https://generativelanguage.googleapis.com/v1beta/models",
            "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse",
        );

        assert_eq!(
            url,
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse"
        );
    }

    #[test]
    fn resolve_gemini_native_url_keeps_opaque_full_url_as_is() {
        let url = crate::proxy::gemini_url::resolve_gemini_native_url(
            "https://relay.example/custom/generate-content",
            "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse",
            true,
        );

        assert_eq!(url, "https://relay.example/custom/generate-content?alt=sse");
    }

    #[test]
    fn force_identity_for_stream_flag_requests() {
        let headers = HeaderMap::new();

        assert!(should_force_identity_encoding(
            "/v1/responses",
            &json!({ "stream": true }),
            &headers
        ));
    }

    #[test]
    fn force_identity_for_gemini_stream_endpoints() {
        let headers = HeaderMap::new();

        assert!(should_force_identity_encoding(
            "/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse",
            &json!({ "model": "gemini-2.5-pro" }),
            &headers
        ));
    }

    #[test]
    fn streaming_request_detects_gemini_sse_without_body_stream_flag() {
        let headers = HeaderMap::new();

        assert!(is_streaming_request(
            "/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse",
            &json!({ "model": "gemini-2.5-pro" }),
            &headers
        ));
    }

    #[test]
    fn force_identity_for_sse_accept_header() {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));

        assert!(should_force_identity_encoding(
            "/v1/responses",
            &json!({ "model": "gpt-5" }),
            &headers
        ));
    }

    #[test]
    fn non_streaming_requests_allow_automatic_compression() {
        let headers = HeaderMap::new();

        assert!(!should_force_identity_encoding(
            "/v1/responses",
            &json!({ "model": "gpt-5" }),
            &headers
        ));
    }

    // ==================== Copilot 动态 endpoint 路由相关测试 ====================

    /// 验证 is_copilot 检测逻辑：通过 provider_type 判断
    #[test]
    fn copilot_detection_via_provider_type() {
        use crate::provider::{Provider, ProviderMeta};

        let provider = Provider {
            id: "test".to_string(),
            name: "Test Copilot".to_string(),
            settings_config: serde_json::json!({}),
            website_url: None,
            category: None,
            created_at: None,
            sort_index: None,
            notes: None,
            meta: Some(ProviderMeta {
                provider_type: Some("github_copilot".to_string()),
                ..Default::default()
            }),
            icon: None,
            icon_color: None,
            in_failover_queue: false,
        };

        let is_copilot = provider
            .meta
            .as_ref()
            .and_then(|m| m.provider_type.as_deref())
            == Some("github_copilot");

        assert!(is_copilot, "应该通过 provider_type 检测为 Copilot");
    }

    /// 验证 is_copilot 检测逻辑：通过 base_url 判断
    #[test]
    fn copilot_detection_via_base_url() {
        let base_url = "https://api.githubcopilot.com";
        let is_copilot = base_url.contains("githubcopilot.com");
        assert!(is_copilot, "应该通过 base_url 检测为 Copilot");

        let non_copilot_url = "https://api.anthropic.com";
        let is_not_copilot = non_copilot_url.contains("githubcopilot.com");
        assert!(!is_not_copilot, "非 Copilot URL 不应被检测为 Copilot");
    }

    /// 验证企业版 endpoint（不包含 githubcopilot.com）场景下 is_copilot 仍然正确
    #[test]
    fn copilot_detection_for_enterprise_endpoint() {
        use crate::provider::{Provider, ProviderMeta};

        // 企业版场景：provider_type 是 github_copilot，但 base_url 可能是企业内部域名
        let provider = Provider {
            id: "enterprise".to_string(),
            name: "Enterprise Copilot".to_string(),
            settings_config: serde_json::json!({}),
            website_url: None,
            category: None,
            created_at: None,
            sort_index: None,
            notes: None,
            meta: Some(ProviderMeta {
                provider_type: Some("github_copilot".to_string()),
                ..Default::default()
            }),
            icon: None,
            icon_color: None,
            in_failover_queue: false,
        };

        let enterprise_base_url = "https://copilot-api.corp.example.com";

        // is_copilot 应该通过 provider_type 检测成功，即使 base_url 不包含 githubcopilot.com
        let is_copilot = provider
            .meta
            .as_ref()
            .and_then(|m| m.provider_type.as_deref())
            == Some("github_copilot")
            || enterprise_base_url.contains("githubcopilot.com");

        assert!(
            is_copilot,
            "企业版 Copilot 应该通过 provider_type 被正确检测"
        );
    }

    /// 验证动态 endpoint 替换条件
    #[test]
    fn dynamic_endpoint_replacement_conditions() {
        // 条件：is_copilot && !is_full_url
        let test_cases = [
            (true, false, true, "Copilot + 非 full_url 应该替换"),
            (true, true, false, "Copilot + full_url 不应替换"),
            (false, false, false, "非 Copilot 不应替换"),
            (false, true, false, "非 Copilot + full_url 不应替换"),
        ];

        for (is_copilot, is_full_url, should_replace, desc) in test_cases {
            let will_replace = is_copilot && !is_full_url;
            assert_eq!(will_replace, should_replace, "{desc}");
        }
    }

    // ===== P3: forwarder 层 media 开关回归测试 =====
    // 验证 gate 在 forwarder 这一层的"接线"，而非 media_sanitizer 纯函数本身。

    fn forwarder_with_rectifier(config: RectifierConfig) -> RequestForwarder {
        let mut fwd = test_forwarder(Duration::from_secs(1), Duration::from_secs(1));
        fwd.rectifier_config = config;
        fwd
    }

    fn provider_with_settings(settings_config: Value) -> Provider {
        let mut p = test_provider_with_type(Some("anthropic"));
        p.settings_config = settings_config;
        p
    }

    fn body_with_image(model: &str) -> Value {
        json!({
            "model": model,
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "image", "source": { "type": "base64", "media_type": "image/png", "data": "abc" } }
                ]
            }]
        })
    }

    fn body_with_codex_input_image(model: &str) -> Value {
        json!({
            "model": model,
            "input": [{
                "role": "user",
                "content": [
                    { "type": "input_image", "image_url": "data:image/png;base64,abc" }
                ]
            }]
        })
    }

    fn body_with_codex_tool_output_image(stringified: bool) -> Value {
        let output = json!({
            "content": [{
                "type": "input_image",
                "image_url": "data:image/png;base64,TOOL_OUTPUT_SENTINEL"
            }]
        });
        json!({
            "model": "any-model",
            "input": [{
                "type": "function_call_output",
                "call_id": "call_1",
                "output": if stringified {
                    Value::String(output.to_string())
                } else {
                    output
                }
            }]
        })
    }

    fn body_with_stringified_chat_tool_image() -> Value {
        let content = json!({
            "content": [{
                "type": "image",
                "mimeType": "image/png",
                "data": "CHAT_TOOL_SENTINEL"
            }]
        })
        .to_string();
        json!({
            "model": "any-model",
            "messages": [{
                "role": "tool",
                "tool_call_id": "call_1",
                "content": content
            }]
        })
    }

    fn body_with_gemini_image() -> Value {
        json!({
            "contents": [{
                "role": "user",
                "parts": [{
                    "inlineData": {
                        "mimeType": "image/png",
                        "data": "GEMINI_SENTINEL"
                    }
                }]
            }]
        })
    }

    fn image_unsupported_error() -> ProxyError {
        ProxyError::UpstreamError {
            status: 400,
            body: Some(
                r#"{"error":{"message":"This model does not support image input"}}"#.to_string(),
            ),
        }
    }
    #[test]
    fn prevention_replaces_when_all_switches_on_and_model_in_heuristic_list() {
        let fwd = forwarder_with_rectifier(RectifierConfig::default());
        let provider = provider_with_settings(json!({}));
        let mut body = body_with_image("deepseek-v4-pro");

        let replaced = fwd.apply_media_prevention(&mut body, &provider);

        assert_eq!(replaced, 1, "默认全开 + 名单内模型应预替换");
        assert_eq!(body["messages"][0]["content"][0]["type"], "text");
    }

    #[test]
    fn prevention_skipped_when_media_fallback_off() {
        // 关闭 request_media_fallback：即使名单命中也不预替换。
        let fwd = forwarder_with_rectifier(RectifierConfig {
            request_media_fallback: false,
            ..RectifierConfig::default()
        });
        let provider = provider_with_settings(json!({}));
        let mut body = body_with_image("deepseek-v4-pro");

        let replaced = fwd.apply_media_prevention(&mut body, &provider);

        assert_eq!(replaced, 0);
        assert_eq!(body["messages"][0]["content"][0]["type"], "image");
    }

    #[test]
    fn prevention_skipped_when_master_switch_off() {
        let fwd = forwarder_with_rectifier(RectifierConfig {
            enabled: false,
            ..RectifierConfig::default()
        });
        let provider = provider_with_settings(json!({}));
        let mut body = body_with_image("deepseek-v4-pro");

        assert_eq!(fwd.apply_media_prevention(&mut body, &provider), 0);
        assert_eq!(body["messages"][0]["content"][0]["type"], "image");
    }

    #[test]
    fn prevention_heuristic_off_skips_list_but_keeps_explicit_text_only() {
        // 关闭 request_media_heuristic：名单预测失效，但显式声明 text-only 仍预替换。
        let fwd = forwarder_with_rectifier(RectifierConfig {
            request_media_heuristic: false,
            ..RectifierConfig::default()
        });

        // (a) 名单内模型、无显式声明 → 不再预替换
        let bare_provider = provider_with_settings(json!({}));
        let mut list_body = body_with_image("deepseek-v4-pro");
        assert_eq!(
            fwd.apply_media_prevention(&mut list_body, &bare_provider),
            0,
            "heuristic 关闭后名单模型不应被预替换"
        );
        assert_eq!(list_body["messages"][0]["content"][0]["type"], "image");

        // (b) 显式声明 text-only → 仍预替换（声明驱动，不受 heuristic 开关影响）
        let declared_provider = provider_with_settings(json!({
            "models": [ { "id": "some-text-model", "input": ["text"] } ]
        }));
        let mut declared_body = body_with_image("some-text-model");
        assert_eq!(
            fwd.apply_media_prevention(&mut declared_body, &declared_provider),
            1,
            "显式 text-only 即使关闭 heuristic 也应预替换"
        );
        assert_eq!(declared_body["messages"][0]["content"][0]["type"], "text");
    }

    #[test]
    fn reactive_triggers_when_all_switches_on() {
        let fwd = forwarder_with_rectifier(RectifierConfig::default());
        let body = body_with_image("any-model");
        assert!(fwd.media_retry_should_trigger("Claude", false, &body, &image_unsupported_error()));
    }

    #[test]
    fn reactive_triggers_for_codex_image_url_deserialize_errors() {
        let fwd = forwarder_with_rectifier(RectifierConfig::default());
        let body = body_with_codex_input_image("deepseek-v4-flash");
        let error = ProxyError::UpstreamError {
            status: 400,
            body: Some(
                r#"{"error":{"message":"Failed to deserialize the JSON body into the target type: messages[11]: unknown variant image_url, expected text"}}"#
                    .to_string(),
            ),
        };

        assert!(fwd.media_retry_should_trigger("Codex", false, &body, &error));
    }

    #[test]
    fn reactive_triggers_for_structured_and_stringified_codex_tool_images() {
        let fwd = forwarder_with_rectifier(RectifierConfig::default());

        for stringified in [false, true] {
            let body = body_with_codex_tool_output_image(stringified);
            assert!(
                fwd.media_retry_should_trigger("Codex", false, &body, &image_unsupported_error()),
                "tool-output image should trigger retry (stringified={stringified})"
            );
        }
    }

    #[test]
    fn reactive_triggers_for_chat_tool_and_gemini_images() {
        let fwd = forwarder_with_rectifier(RectifierConfig::default());

        assert!(fwd.media_retry_should_trigger(
            "Claude",
            false,
            &body_with_stringified_chat_tool_image(),
            &image_unsupported_error()
        ));
        assert!(fwd.media_retry_should_trigger(
            "Claude",
            false,
            &body_with_gemini_image(),
            &image_unsupported_error()
        ));
    }

    #[test]
    fn reactive_does_not_treat_context_limit_as_image_rejection() {
        let fwd = forwarder_with_rectifier(RectifierConfig::default());
        let body = body_with_codex_tool_output_image(false);
        let context_error = ProxyError::UpstreamError {
            status: 400,
            body: Some(r#"{"error":{"message":"maximum context length exceeded"}}"#.to_string()),
        };

        assert!(!fwd.media_retry_should_trigger("Codex", false, &body, &context_error));
    }

    #[test]
    fn reactive_skipped_when_media_fallback_off() {
        // 关闭 request_media_fallback：上游报图片错误也不触发兜底重试。
        let fwd = forwarder_with_rectifier(RectifierConfig {
            request_media_fallback: false,
            ..RectifierConfig::default()
        });
        let body = body_with_image("any-model");
        assert!(!fwd.media_retry_should_trigger(
            "Claude",
            false,
            &body,
            &image_unsupported_error()
        ));
    }

    #[test]
    fn reactive_skipped_when_master_switch_off() {
        let fwd = forwarder_with_rectifier(RectifierConfig {
            enabled: false,
            ..RectifierConfig::default()
        });
        let body = body_with_image("any-model");
        assert!(!fwd.media_retry_should_trigger(
            "Claude",
            false,
            &body,
            &image_unsupported_error()
        ));
    }

    #[test]
    fn reactive_unaffected_by_heuristic_switch() {
        // 关闭 request_media_heuristic 不影响反应式兜底——它是上游实测错误后的恢复，不是预测。
        let fwd = forwarder_with_rectifier(RectifierConfig {
            request_media_heuristic: false,
            ..RectifierConfig::default()
        });
        let body = body_with_image("any-model");
        assert!(fwd.media_retry_should_trigger("Claude", false, &body, &image_unsupported_error()));
    }
}
