import { invoke } from "@tauri-apps/api/core";
import type { AppId } from "./types";

export type ProviderModelTestApp = Extract<
  AppId,
  "claude" | "codex" | "gemini" | "grokbuild"
>;

export type ProviderModelCandidateSource = "configured" | "live";

export interface ProviderModelCandidate {
  id: string;
  label?: string;
  source: ProviderModelCandidateSource;
}

export type LiveProviderModelCandidate = ProviderModelCandidate & {
  source: "live";
};

export interface ProviderModelTestStartResult {
  operationId: string;
}

export type ProviderModelTestStatus =
  | "running"
  | "retrying"
  | "succeeded"
  | "failed"
  | "cancelled";

export type ProviderModelTestErrorCategory =
  | "timeout"
  | "connect"
  | "read"
  | "server"
  | "malformed"
  | "invalid"
  | "cancelled"
  | "unknown";

export interface ProviderModelTestResult {
  status: ProviderModelTestStatus;
  providerId: string;
  modelId: string;
  attempts: number;
  retriesUsed: number;
  elapsedMs: number;
  responseSnippet?: string;
  errorCategory?: ProviderModelTestErrorCategory;
  message?: string;
}

/** Fetches live model candidates. Credentials are resolved by the Rust command. */
export async function refreshProviderModelCandidates(
  appType: ProviderModelTestApp,
  providerId: string,
): Promise<LiveProviderModelCandidate[]> {
  return invoke<LiveProviderModelCandidate[]>(
    "refresh_provider_model_candidates",
    { appType, providerId },
  );
}

/** Starts a fixed, non-streaming `hi` request for the selected provider/model. */
export async function startProviderModelTest(
  appType: ProviderModelTestApp,
  providerId: string,
  modelId: string,
): Promise<ProviderModelTestStartResult> {
  return invoke<ProviderModelTestStartResult>("start_provider_model_test", {
    appType,
    providerId,
    modelId,
  });
}

export async function getProviderModelTestResult(
  operationId: string,
): Promise<ProviderModelTestResult> {
  return invoke<ProviderModelTestResult>("get_provider_model_test_result", {
    operationId,
  });
}

export async function cancelProviderModelTest(
  operationId: string,
): Promise<void> {
  await invoke<void>("cancel_provider_model_test", { operationId });
}

export const MODEL_TEST_SUPPORTED_APPS: readonly ProviderModelTestApp[] = [
  "claude",
  "codex",
  "gemini",
  "grokbuild",
];
