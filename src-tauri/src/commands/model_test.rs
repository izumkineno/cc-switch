//! Tauri commands for direct provider model tests.

use crate::app_config::AppType;
use crate::services::model_test::{self, ModelCandidate, RequestFailure};
use crate::store::AppState;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tauri::State;
use tokio::sync::watch;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelTestStatus {
    Running,
    Retrying,
    Succeeded,
    Failed,
    Cancelled,
}

impl ModelTestStatus {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled
        )
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelTestResult {
    pub status: ModelTestStatus,
    pub provider_id: String,
    pub model_id: String,
    pub attempts: u32,
    pub retries_used: u32,
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_snippet: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelTestStart {
    pub operation_id: String,
}

struct OperationEntry {
    result: ModelTestResult,
    cancel: watch::Sender<bool>,
}

/// Shared operation state.  Terminal entries are removed when their result is
/// polled, so an abandoned test cannot grow the map indefinitely.
#[derive(Clone, Default)]
pub struct ModelTestState {
    operations: Arc<Mutex<HashMap<String, OperationEntry>>>,
}

#[tauri::command(rename_all = "camelCase")]
pub async fn refresh_provider_model_candidates(
    app_type: String,
    provider_id: String,
    state: State<'_, AppState>,
) -> Result<Vec<ModelCandidate>, String> {
    let app = parse_supported_app(&app_type)?;
    let provider = state
        .db
        .get_provider_by_id(&provider_id, app.as_str())
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "Provider not found".to_string())?;
    model_test::check_eligibility(&app, &provider)?;
    model_test::refresh_candidates(&app, &provider).await
}

#[tauri::command(rename_all = "camelCase")]
pub async fn start_provider_model_test(
    app_type: String,
    provider_id: String,
    model_id: String,
    state: State<'_, AppState>,
    model_test_state: State<'_, ModelTestState>,
) -> Result<ModelTestStart, String> {
    let app = parse_supported_app(&app_type)?;
    let provider = state
        .db
        .get_provider_by_id(&provider_id, app.as_str())
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "Provider not found".to_string())?;
    model_test::check_eligibility(&app, &provider)?;
    if model_id.trim().is_empty() {
        return Err("Model id is required".to_string());
    }

    // Read the persisted policy before spawning so the worker has an immutable
    // snapshot and cannot observe a half-updated settings row.
    let config = state
        .db
        .get_stream_check_config()
        .map_err(|error| error.to_string())?;
    let operation_id = Uuid::new_v4().to_string();
    let (cancel, cancel_rx) = watch::channel(false);
    let result = ModelTestResult {
        status: ModelTestStatus::Running,
        provider_id: provider_id.clone(),
        model_id: model_id.clone(),
        attempts: 0,
        retries_used: 0,
        elapsed_ms: 0,
        response_snippet: None,
        error_category: None,
        message: None,
    };

    {
        let mut operations = model_test_state
            .operations
            .lock()
            .map_err(|_| "Model test state is unavailable".to_string())?;
        operations.insert(
            operation_id.clone(),
            OperationEntry { result, cancel },
        );
    }

    let operations = model_test_state.operations.clone();
    let operation_for_task = operation_id.clone();
    tauri::async_runtime::spawn(async move {
        run_model_test(
            operations,
            operation_for_task,
            app,
            provider,
            model_id,
            config.timeout_secs,
            config.max_retries,
            cancel_rx,
        )
        .await;
    });

    Ok(ModelTestStart { operation_id })
}

#[tauri::command(rename_all = "camelCase")]
pub fn get_provider_model_test_result(
    operation_id: String,
    state: State<'_, ModelTestState>,
) -> Result<ModelTestResult, String> {
    let mut operations = state
        .operations
        .lock()
        .map_err(|_| "Model test state is unavailable".to_string())?;
    let result = operations
        .get(&operation_id)
        .map(|entry| entry.result.clone())
        .ok_or_else(|| "Model test operation not found".to_string())?;
    if result.status.is_terminal() {
        operations.remove(&operation_id);
    }
    Ok(result)
}

#[tauri::command(rename_all = "camelCase")]
pub fn cancel_provider_model_test(
    operation_id: String,
    state: State<'_, ModelTestState>,
) -> Result<(), String> {
    let operations = state
        .operations
        .lock()
        .map_err(|_| "Model test state is unavailable".to_string())?;
    let entry = operations
        .get(&operation_id)
        .ok_or_else(|| "Model test operation not found".to_string())?;
    if entry.result.status.is_terminal() {
        return Ok(());
    }
    entry
        .cancel
        .send(true)
        .map_err(|_| "Model test operation is no longer running".to_string())?;
    drop(operations);

    // Make cancellation observable immediately; the worker repeats this state
    // after the in-flight request's select branch exits.
    let mut operations = state
        .operations
        .lock()
        .map_err(|_| "Model test state is unavailable".to_string())?;
    if let Some(entry) = operations.get_mut(&operation_id) {
        entry.result.status = ModelTestStatus::Cancelled;
        entry.result.message = Some("Model test cancelled".to_string());
        entry.result.error_category = Some("cancelled".to_string());
        entry.result.elapsed_ms = entry.result.elapsed_ms.max(0);
    }
    Ok(())
}

async fn run_model_test(
    operations: Arc<Mutex<HashMap<String, OperationEntry>>>,
    operation_id: String,
    app: AppType,
    provider: crate::provider::Provider,
    model_id: String,
    timeout_secs: u64,
    max_retries: u32,
    mut cancel_rx: watch::Receiver<bool>,
) {
    let started = Instant::now();
    let total_attempts = model_test::attempt_count(max_retries);

    for attempt_index in 0..total_attempts {
        if is_cancelled(&cancel_rx) {
            store_cancelled(&operations, &operation_id, started.elapsed().as_millis() as u64);
            return;
        }

        let attempt = attempt_index.saturating_add(1);
        update_result(&operations, &operation_id, |result| {
            result.status = if attempt_index == 0 {
                ModelTestStatus::Running
            } else {
                ModelTestStatus::Retrying
            };
            result.attempts = attempt;
            result.retries_used = attempt_index;
            result.elapsed_ms = started.elapsed().as_millis() as u64;
            result.error_category = None;
            result.message = None;
        });

        let request = tokio::select! {
            biased;
            _ = cancel_rx.changed() => {
                store_cancelled(&operations, &operation_id, started.elapsed().as_millis() as u64);
                return;
            }
            result = model_test::request_once(&app, &provider, &model_id, timeout_secs) => result,
        };

        match request {
            Ok(response) => {
                if is_cancelled(&cancel_rx) {
                    store_cancelled(&operations, &operation_id, started.elapsed().as_millis() as u64);
                    return;
                }
                update_result(&operations, &operation_id, |result| {
                    result.status = ModelTestStatus::Succeeded;
                    result.attempts = attempt;
                    result.retries_used = attempt_index;
                    result.elapsed_ms = started.elapsed().as_millis() as u64;
                    result.response_snippet = Some(response);
                    result.error_category = None;
                    result.message = Some("Model responded successfully".to_string());
                });
                return;
            }
            Err(failure) => {
                if is_cancelled(&cancel_rx) {
                    store_cancelled(&operations, &operation_id, started.elapsed().as_millis() as u64);
                    return;
                }
                let should_retry = failure.retryable && attempt_index < max_retries;
                if should_retry {
                    update_result(&operations, &operation_id, |result| {
                        result.status = ModelTestStatus::Retrying;
                        result.attempts = attempt;
                        result.retries_used = attempt_index;
                        result.elapsed_ms = started.elapsed().as_millis() as u64;
                        result.error_category = Some(failure.category.clone());
                        result.message = Some(failure.message.clone());
                    });
                    continue;
                }
                update_result(&operations, &operation_id, |result| {
                    result.status = ModelTestStatus::Failed;
                    result.attempts = attempt;
                    result.retries_used = attempt_index;
                    result.elapsed_ms = started.elapsed().as_millis() as u64;
                    result.error_category = Some(failure.category.clone());
                    result.message = Some(failure.message.clone());
                });
                return;
            }
        }
    }
}

fn parse_supported_app(value: &str) -> Result<AppType, String> {
    let app = value.parse::<AppType>().map_err(|error| error.to_string())?;
    if matches!(
        app,
        AppType::Claude | AppType::Codex | AppType::Gemini | AppType::GrokBuild
    ) {
        Ok(app)
    } else {
        Err("Model tests are not supported for this app".to_string())
    }
}

fn is_cancelled(cancel_rx: &watch::Receiver<bool>) -> bool {
    *cancel_rx.borrow()
}

fn update_result(
    operations: &Arc<Mutex<HashMap<String, OperationEntry>>>,
    operation_id: &str,
    update: impl FnOnce(&mut ModelTestResult),
) {
    if let Ok(mut operations) = operations.lock() {
        if let Some(entry) = operations.get_mut(operation_id) {
            if !matches!(entry.result.status, ModelTestStatus::Cancelled) {
                update(&mut entry.result);
            }
        }
    }
}

fn store_cancelled(
    operations: &Arc<Mutex<HashMap<String, OperationEntry>>>,
    operation_id: &str,
    elapsed_ms: u64,
) {
    update_result(operations, operation_id, |result| {
        result.status = ModelTestStatus::Cancelled;
        result.elapsed_ms = elapsed_ms;
        result.error_category = Some("cancelled".to_string());
        result.message = Some("Model test cancelled".to_string());
    });
}

#[allow(dead_code)]
fn _request_failure_is_bounded(failure: &RequestFailure) -> bool {
    failure.message.chars().count() <= 240
}
