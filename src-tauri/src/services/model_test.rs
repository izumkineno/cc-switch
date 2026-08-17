//! Backend model-test requests and response parsing.
//!
//! This module deliberately keeps model tests separate from the proxy path.  A
//! test sends one fixed, non-streaming `hi` request to the provider selected by
//! the caller, and never changes the active provider or failover state.

use crate::app_config::AppType;
use crate::provider::{AuthBindingSource, Provider};
use crate::proxy::providers::{get_adapter, AuthInfo, AuthStrategy, ProviderAdapter};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, CONTENT_TYPE, USER_AGENT};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use futures::StreamExt;
use std::time::Duration;

const MAX_SNIPPET_CHARS: usize = 512;
const MAX_ERROR_CHARS: usize = 240;
const MAX_MODEL_ID_CHARS: usize = 256;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// Wire protocol used by a provider model test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestProtocol {
    Anthropic,
    OpenAiResponses,
    OpenAiChat,
    Gemini,
}

/// Candidate returned by the live model discovery command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCandidate {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub source: String,
}

/// A bounded, user-safe failure from one model-test attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestFailure {
    pub category: String,
    pub message: String,
    pub retryable: bool,
}

impl RequestFailure {
    fn new(category: &str, message: &str, retryable: bool) -> Self {
        Self {
            category: category.to_string(),
            message: bound_message(message, MAX_ERROR_CHARS),
            retryable,
        }
    }
}

/// Return the number of attempts for `max_retries` retries after the first call.
pub const fn attempt_count(max_retries: u32) -> u32 {
    max_retries.saturating_add(1)
}

/// Resolve the upstream wire protocol from the persisted provider format.
pub fn protocol_for(app_type: &AppType, provider: &Provider) -> TestProtocol {
    let configured_format = provider
        .meta
        .as_ref()
        .and_then(|meta| meta.api_format.as_deref())
        .or_else(|| provider.settings_config.get("api_format").and_then(Value::as_str))
        .or_else(|| provider.settings_config.get("apiFormat").and_then(Value::as_str));

    match app_type {
        AppType::Claude => match configured_format {
            Some(format) if format.eq_ignore_ascii_case("openai_chat") => TestProtocol::OpenAiChat,
            Some(format) if format.eq_ignore_ascii_case("openai_responses") => {
                TestProtocol::OpenAiResponses
            }
            Some(format) if format.eq_ignore_ascii_case("gemini_native") => TestProtocol::Gemini,
            _ => TestProtocol::Anthropic,
        },
        AppType::Gemini => TestProtocol::Gemini,
        AppType::Codex | AppType::GrokBuild => {
            if configured_format
                .is_some_and(|format| format.eq_ignore_ascii_case("anthropic"))
            {
                TestProtocol::Anthropic
            } else if configured_format
                .is_some_and(|format| format.eq_ignore_ascii_case("openai_chat"))
                || provider
                    .settings_config
                    .get("config")
                    .and_then(Value::as_str)
                    .is_some_and(|config| {
                        config.contains("api_backend = \"chat\"")
                            || config.contains("api_backend = 'chat'")
                            || config.contains("wire_api = \"chat\"")
                            || config.contains("wire_api = 'chat'")
                    })
            {
                TestProtocol::OpenAiChat
            } else {
                TestProtocol::OpenAiResponses
            }
        }
        // The command rejects these app types before reaching this function.
        _ => TestProtocol::OpenAiResponses,
    }
}

/// Fail closed for providers that are not user-managed, direct custom cards.
pub fn check_eligibility(app_type: &AppType, provider: &Provider) -> Result<(), String> {
    if !matches!(
        app_type,
        AppType::Claude | AppType::Codex | AppType::Gemini | AppType::GrokBuild
    ) {
        return Err("Model tests are not supported for this app".to_string());
    }

    if provider
        .category
        .as_deref()
        .is_some_and(|category| !matches!(category, "custom" | "third_party"))
    {
        return Err("Model tests require a custom or third-party provider".to_string());
    }

    if provider.uses_managed_account_auth() {
        return Err("Managed OAuth providers cannot be model-tested".to_string());
    }

    if provider
        .meta
        .as_ref()
        .and_then(|meta| meta.auth_binding.as_ref())
        .is_some_and(|binding| binding.source == AuthBindingSource::ManagedAccount)
    {
        return Err("Managed OAuth providers cannot be model-tested".to_string());
    }

    if has_rejected_marker(provider) {
        return Err("Official, OAuth, and native providers cannot be model-tested".to_string());
    }

    Ok(())
}

fn has_rejected_marker(provider: &Provider) -> bool {
    let marker = |raw: Option<&str>| {
        raw.map(|value| {
            let value = value.trim().to_ascii_lowercase();
            value.contains("oauth")
                || value.contains("copilot")
                || value == "native"
                || value == "gemini_cli"
                || value == "gemini-cli"
                || value == "managed"
        })
        .unwrap_or(false)
    };

    if let Some(meta) = provider.meta.as_ref() {
        if marker(meta.provider_type.as_deref()) {
            return true;
        }
        if meta.extra.iter().any(|(key, value)| {
            let key_marker = key.to_ascii_lowercase();
            (key_marker.contains("oauth")
                || key_marker.contains("native")
                || key_marker.contains("managed"))
                && value.as_bool().unwrap_or(false)
        }) {
            return true;
        }
    }

    ["providerType", "authMode", "auth_mode", "provider_type"]
        .iter()
        .any(|key| marker(provider.settings_config.get(key).and_then(Value::as_str)))
        || ["native", "isNative", "managed", "isManaged", "oauth"]
            .iter()
            .any(|key| provider
                .settings_config
                .get(key)
                .and_then(Value::as_bool)
                .unwrap_or(false))
}

/// Extract non-empty assistant text from a known non-streaming response shape.
pub fn extract_response_text(protocol: TestProtocol, value: &Value) -> Option<String> {
    let text = match protocol {
        TestProtocol::Anthropic => value
            .get("content")
            .and_then(Value::as_array)
            .and_then(|blocks| {
                blocks.iter().find_map(|block| {
                    block
                        .get("text")
                        .and_then(Value::as_str)
                        .or_else(|| block.as_str())
                })
            }),
        TestProtocol::OpenAiChat => value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
            .and_then(|choice| choice.get("message"))
            .and_then(|message| extract_content_text(message)),
        TestProtocol::OpenAiResponses => value
            .get("output_text")
            .and_then(Value::as_str)
            .or_else(|| {
                value
                    .get("output")
                    .and_then(Value::as_array)
                    .and_then(|items| {
                        items.iter().find_map(|item| {
                            item.get("content")
                                .and_then(|content| extract_content_text(content))
                                .or_else(|| item.get("text").and_then(Value::as_str))
                        })
                    })
            }),
        TestProtocol::Gemini => value
            .get("candidates")
            .and_then(Value::as_array)
            .and_then(|candidates| candidates.first())
            .and_then(|candidate| candidate.get("content"))
            .and_then(|content| content.get("parts"))
            .and_then(Value::as_array)
            .and_then(|parts| parts.iter().find_map(|part| part.get("text").and_then(Value::as_str))),
    }?;

    let text = text.trim();
    (!text.is_empty()).then(|| bound_message(text, MAX_SNIPPET_CHARS))
}

fn extract_content_text(value: &Value) -> Option<&str> {
    if let Some(text) = value.as_str() {
        return Some(text);
    }
    if let Some(text) = value.get("text").and_then(Value::as_str) {
        return Some(text);
    }
    value.as_array().and_then(|parts| {
        parts.iter().find_map(|part| {
            part.get("text")
                .and_then(Value::as_str)
                .or_else(|| part.as_str())
        })
    })
}

/// Execute one direct, non-streaming request.
pub async fn request_once(
    app_type: &AppType,
    provider: &Provider,
    model_id: &str,
    timeout_secs: u64,
) -> Result<String, RequestFailure> {
    validate_model_id(model_id)?;
    let protocol = protocol_for(app_type, provider);
    let connection = resolve_connection(app_type, provider)?;
    let endpoint = match protocol {
        TestProtocol::Anthropic => "/v1/messages".to_string(),
        TestProtocol::OpenAiResponses => "/responses".to_string(),
        TestProtocol::OpenAiChat => "/chat/completions".to_string(),
        TestProtocol::Gemini => format!("models/{model_id}:generateContent"),
    };
    let url = if provider
        .meta
        .as_ref()
        .and_then(|meta| meta.is_full_url)
        .unwrap_or(false)
    {
        connection.base_url.clone()
    } else {
        connection.adapter.build_url(&connection.base_url, &endpoint)
    };

    let body = match protocol {
        TestProtocol::Anthropic => json!({
            "model": model_id,
            "max_tokens": 32,
            "messages": [{"role": "user", "content": "hi"}],
            "stream": false
        }),
        TestProtocol::OpenAiResponses => json!({
            "model": model_id,
            "input": "hi",
            "max_output_tokens": 32,
            "stream": false
        }),
        TestProtocol::OpenAiChat => json!({
            "model": model_id,
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 32,
            "stream": false
        }),
        TestProtocol::Gemini => json!({
            "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
            "generationConfig": {"maxOutputTokens": 32}
        }),
    };

    let mut headers = connection.headers;
    if protocol == TestProtocol::Anthropic {
        headers.insert(
            HeaderName::from_static("anthropic-version"),
            HeaderValue::from_static("2023-06-01"),
        );
    }

    let response = crate::proxy::http_client::get()
        .post(url)
        .headers(headers)
        .timeout(Duration::from_secs(timeout_secs))
        .json(&body)
        .send()
        .await
        .map_err(map_request_error)?;

    let status = response.status();
    if !status.is_success() {
        let status_code = status.as_u16();
        return Err(if status.is_server_error() {
            RequestFailure::new("5xx", &format!("HTTP {status_code}"), true)
        } else {
            RequestFailure::new("http", &format!("HTTP {status_code}"), false)
        });
    }

    let bytes = read_response_body_limited(response).await?;
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| RequestFailure::new("malformed", "Provider returned malformed JSON", true))?;
    extract_response_text(protocol, &value)
        .ok_or_else(|| RequestFailure::new("malformed", "Provider returned no assistant text", true))
}

async fn read_response_body_limited(
    response: reqwest::Response,
) -> Result<Vec<u8>, RequestFailure> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(RequestFailure::new(
            "response_too_large",
            "Provider response is too large",
            false,
        ));
    }

    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            if error.is_timeout() {
                RequestFailure::new("timeout", "Response read timed out", true)
            } else {
                RequestFailure::new("read", "Response could not be read", true)
            }
        })?;
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(RequestFailure::new(
                "response_too_large",
                "Provider response is too large",
                false,
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Discover candidates using the existing OpenAI-compatible model fetcher, or
/// Gemini's native `models` listing when the app uses Gemini.
pub async fn refresh_candidates(
    app_type: &AppType,
    provider: &Provider,
) -> Result<Vec<ModelCandidate>, String> {
    let connection = resolve_connection(app_type, provider).map_err(|failure| failure.message)?;
    let user_agent = provider
        .meta
        .as_ref()
        .and_then(|meta| meta.custom_user_agent_header().ok().flatten());

    if *app_type == AppType::Gemini {
        return fetch_gemini_candidates(&connection, user_agent).await;
    }

    let api_format = if connection.auth.strategy == AuthStrategy::Anthropic {
        Some("anthropic-messages")
    } else {
        None
    };
    let models_url = provider.meta.as_ref().and_then(|meta| {
        meta.extra
            .get("modelsUrl")
            .or_else(|| meta.extra.get("models_url"))
            .and_then(Value::as_str)
    });
    let models = crate::services::model_fetch::fetch_models(
        &connection.base_url,
        &connection.auth.api_key,
        provider
            .meta
            .as_ref()
            .and_then(|meta| meta.is_full_url)
            .unwrap_or(false),
        models_url,
        user_agent,
        api_format,
        Some(&connection.custom_headers),
    )
    .await?;

    Ok(models
        .into_iter()
        .filter(|model| !model.id.trim().is_empty())
        .map(|model| ModelCandidate {
            id: model.id,
            label: model.owned_by.filter(|owner| !owner.trim().is_empty()),
            source: "live".to_string(),
        })
        .collect())
}

struct Connection {
    adapter: Box<dyn ProviderAdapter>,
    base_url: String,
    auth: AuthInfo,
    headers: HeaderMap,
    custom_headers: BTreeMap<String, String>,
}

fn resolve_connection(
    app_type: &AppType,
    provider: &Provider,
) -> Result<Connection, RequestFailure> {
    let adapter = get_adapter(app_type)
        .ok_or_else(|| RequestFailure::new("configuration", "Provider adapter unavailable", false))?;
    let base_url = adapter
        .extract_base_url(provider)
        .map_err(|_| RequestFailure::new("configuration", "Provider base URL is unavailable", false))?;
    if base_url.trim().is_empty() {
        return Err(RequestFailure::new(
            "configuration",
            "Provider base URL is unavailable",
            false,
        ));
    }
    let auth = adapter
        .extract_auth(provider)
        .ok_or_else(|| RequestFailure::new("configuration", "Provider credentials are unavailable", false))?;
    if auth.api_key.trim().is_empty() {
        return Err(RequestFailure::new(
            "configuration",
            "Provider credentials are unavailable",
            false,
        ));
    }
    let auth_pairs = adapter
        .get_auth_headers(&auth)
        .map_err(|_| RequestFailure::new("configuration", "Provider credentials are invalid", false))?;
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    for (name, value) in auth_pairs {
        headers.insert(name, value);
    }

    let custom_headers = provider
        .meta
        .as_ref()
        .and_then(|meta| meta.local_proxy_request_overrides.as_ref())
        .map(|overrides| {
            overrides
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    for (name, value) in &custom_headers {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
            RequestFailure::new("configuration", "Provider request header is invalid", false)
        })?;
        let value = HeaderValue::from_str(value).map_err(|_| {
            RequestFailure::new("configuration", "Provider request header is invalid", false)
        })?;
        headers.insert(name, value);
    }
    if let Some(user_agent) = provider
        .meta
        .as_ref()
        .and_then(|meta| meta.custom_user_agent_header().ok().flatten())
    {
        headers.insert(USER_AGENT, user_agent);
    }

    Ok(Connection {
        adapter,
        base_url: base_url.trim_end_matches('/').to_string(),
        auth,
        headers,
        custom_headers,
    })
}

async fn fetch_gemini_candidates(
    connection: &Connection,
    user_agent: Option<HeaderValue>,
) -> Result<Vec<ModelCandidate>, String> {
    let url = connection.adapter.build_url(&connection.base_url, "models");
    let mut headers = connection.headers.clone();
    if let Some(user_agent) = user_agent {
        headers.insert(USER_AGENT, user_agent);
    }
    let response = crate::proxy::http_client::get()
        .get(url)
        .headers(headers)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|_| "Failed to fetch Gemini models".to_string())?;
    if !response.status().is_success() {
        return Err(format!("HTTP {} while fetching Gemini models", response.status().as_u16()));
    }
    let body: Value = response
        .json()
        .await
        .map_err(|_| "Gemini model list was malformed".to_string())?;
    let mut candidates = body
        .get("models")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|model| {
            let raw_name = model.get("name").and_then(Value::as_str)?.trim();
            let id = raw_name.strip_prefix("models/").unwrap_or(raw_name);
            if id.is_empty() {
                return None;
            }
            let supports_generation = model
                .get("supportedGenerationMethods")
                .and_then(Value::as_array)
                .map(|methods| {
                    methods.iter().any(|method| {
                        method.as_str() == Some("generateContent")
                    })
                })
                .unwrap_or(true);
            supports_generation.then(|| ModelCandidate {
                id: id.to_string(),
                label: model
                    .get("displayName")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                source: "live".to_string(),
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.id.cmp(&right.id));
    candidates.dedup_by(|left, right| left.id == right.id);
    Ok(candidates)
}

fn validate_model_id(model_id: &str) -> Result<(), RequestFailure> {
    let trimmed = model_id.trim();
    if trimmed.is_empty()
        || trimmed.chars().count() > MAX_MODEL_ID_CHARS
        || trimmed.chars().any(|ch| ch.is_control())
        || trimmed.contains(['?', '#'])
    {
        return Err(RequestFailure::new(
            "configuration",
            "Model id is invalid",
            false,
        ));
    }
    Ok(())
}

fn map_request_error(error: reqwest::Error) -> RequestFailure {
    if error.is_timeout() {
        RequestFailure::new("timeout", "Request timed out", true)
    } else if error.is_connect() {
        RequestFailure::new("connect", "Connection failed", true)
    } else if error.is_body() {
        RequestFailure::new("read", "Response could not be read", true)
    } else {
        RequestFailure::new("network", "Request failed", false)
    }
}

fn bound_message(value: &str, max_chars: usize) -> String {
    value
        .chars()
        .filter(|ch| !ch.is_control() || matches!(ch, '\n' | '\r' | '\t'))
        .take(max_chars)
        .collect::<String>()
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{Provider, ProviderMeta};

    fn provider(category: &str) -> Provider {
        let mut provider = Provider::with_id(
            "custom".to_string(),
            "Custom".to_string(),
            json!({"env": {"ANTHROPIC_BASE_URL": "https://example.test", "ANTHROPIC_API_KEY": "secret"}}),
            None,
        );
        provider.category = Some(category.to_string());
        provider
    }

    #[test]
    fn eligibility_is_fail_closed_for_category_and_markers() {
        assert!(check_eligibility(&AppType::Claude, &provider("custom")).is_ok());
        assert!(check_eligibility(&AppType::Claude, &provider("official")).is_err());

        let mut oauth = provider("custom");
        oauth.meta = Some(ProviderMeta {
            provider_type: Some("codex_oauth".to_string()),
            ..Default::default()
        });
        assert!(check_eligibility(&AppType::Claude, &oauth).is_err());

        let mut native = provider("third_party");
        native.settings_config["native"] = Value::Bool(true);
        assert!(check_eligibility(&AppType::Gemini, &native).is_err());
    }

    #[test]
    fn extracts_non_empty_text_for_supported_response_shapes() {
        assert_eq!(
            extract_response_text(
                TestProtocol::Anthropic,
                &json!({"content": [{"type": "text", "text": " hello "}]})
            ),
            Some("hello".to_string())
        );
        assert_eq!(
            extract_response_text(
                TestProtocol::OpenAiChat,
                &json!({"choices": [{"message": {"content": "chat"}}]})
            ),
            Some("chat".to_string())
        );
        assert_eq!(
            extract_response_text(TestProtocol::OpenAiResponses, &json!({"output_text": "response"})),
            Some("response".to_string())
        );
        assert_eq!(
            extract_response_text(
                TestProtocol::Gemini,
                &json!({"candidates": [{"content": {"parts": [{"text": "gemini"}]}}]})
            ),
            Some("gemini".to_string())
        );
        assert_eq!(extract_response_text(TestProtocol::Anthropic, &json!({"content": []})), None);
    }

    #[test]
    fn maps_retries_to_attempts_without_overflow() {
        assert_eq!(attempt_count(0), 1);
        assert_eq!(attempt_count(3), 4);
        assert_eq!(attempt_count(u32::MAX), u32::MAX);
    }
}
