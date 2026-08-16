import type { ProviderRetryPolicy } from "@/types";

export const PROVIDER_RETRY_POLICY_DEFAULTS: ProviderRetryPolicy = {
  retryCount: 0,
  perAttemptTimeoutSeconds: 0,
  retryWaitSeconds: 0,
  unlimitedRetries: false,
};

export type ProviderRetryPolicyNumericField = Exclude<
  keyof ProviderRetryPolicy,
  "unlimitedRetries"
>;

export const PROVIDER_RETRY_POLICY_LIMITS = {
  retryCount: { min: 0, max: 10 },
  perAttemptTimeoutSeconds: { min: 0, max: 1200 },
  retryWaitSeconds: { min: 0, max: 60 },
} as const satisfies Record<
  ProviderRetryPolicyNumericField,
  { min: number; max: number }
>;

export const PROVIDER_RETRY_POLICY_SUPPORTED_APPS = [
  "claude",
  "codex",
  "gemini",
  "grokbuild",
] as const;

export type ProviderRetryPolicySupportedApp =
  (typeof PROVIDER_RETRY_POLICY_SUPPORTED_APPS)[number];

export type ProviderRetryPolicyDraft = {
  [Field in ProviderRetryPolicyNumericField]: string;
} & Pick<ProviderRetryPolicy, "unlimitedRetries">;

export type ProviderRetryPolicyField = ProviderRetryPolicyNumericField;

export type ProviderRetryPolicyErrorCode = "required" | "integer" | "range";

export type ProviderRetryPolicyErrors = Partial<
  Record<ProviderRetryPolicyField, ProviderRetryPolicyErrorCode>
>;

export interface ProviderRetryPolicyParseResult {
  success: boolean;
  valid: boolean;
  policy?: ProviderRetryPolicy;
  errors: ProviderRetryPolicyErrors;
}

const providerRetryPolicyFields = Object.keys(
  PROVIDER_RETRY_POLICY_LIMITS,
) as ProviderRetryPolicyField[];

const isValidInteger = (
  value: unknown,
  min: number,
  max: number,
): value is number =>
  typeof value === "number" &&
  Number.isSafeInteger(value) &&
  value >= min &&
  value <= max;

/**
 * Normalize persisted metadata only. Invalid or missing values become the
 * documented disabled value; submission uses the strict draft parser below.
 */
export function normalizeProviderRetryPolicy(
  value?: Partial<ProviderRetryPolicy> | null,
): ProviderRetryPolicy {
  const source = value ?? {};
  return {
    retryCount: isValidInteger(
      source.retryCount,
      PROVIDER_RETRY_POLICY_LIMITS.retryCount.min,
      PROVIDER_RETRY_POLICY_LIMITS.retryCount.max,
    )
      ? source.retryCount
      : PROVIDER_RETRY_POLICY_DEFAULTS.retryCount,
    perAttemptTimeoutSeconds: isValidInteger(
      source.perAttemptTimeoutSeconds,
      PROVIDER_RETRY_POLICY_LIMITS.perAttemptTimeoutSeconds.min,
      PROVIDER_RETRY_POLICY_LIMITS.perAttemptTimeoutSeconds.max,
    )
      ? source.perAttemptTimeoutSeconds
      : PROVIDER_RETRY_POLICY_DEFAULTS.perAttemptTimeoutSeconds,
    retryWaitSeconds: isValidInteger(
      source.retryWaitSeconds,
      PROVIDER_RETRY_POLICY_LIMITS.retryWaitSeconds.min,
      PROVIDER_RETRY_POLICY_LIMITS.retryWaitSeconds.max,
    )
      ? source.retryWaitSeconds
      : PROVIDER_RETRY_POLICY_DEFAULTS.retryWaitSeconds,
    unlimitedRetries:
      typeof source.unlimitedRetries === "boolean"
        ? source.unlimitedRetries
        : PROVIDER_RETRY_POLICY_DEFAULTS.unlimitedRetries,
  };
}

export function providerRetryPolicyToDraft(
  value?: Partial<ProviderRetryPolicy> | null,
): ProviderRetryPolicyDraft {
  const normalized = normalizeProviderRetryPolicy(value);
  return {
    retryCount: String(normalized.retryCount),
    perAttemptTimeoutSeconds: String(normalized.perAttemptTimeoutSeconds),
    retryWaitSeconds: String(normalized.retryWaitSeconds),
    unlimitedRetries: normalized.unlimitedRetries,
  };
}

export const createDefaultProviderRetryPolicyDraft =
  (): ProviderRetryPolicyDraft =>
    providerRetryPolicyToDraft(PROVIDER_RETRY_POLICY_DEFAULTS);

const validateField = (
  rawValue: unknown,
  field: ProviderRetryPolicyField,
): ProviderRetryPolicyErrorCode | undefined => {
  const raw = typeof rawValue === "string" ? rawValue.trim() : "";
  if (!raw) return "required";
  if (!/^\d+$/.test(raw)) return "integer";

  const parsed = Number(raw);
  const { min, max } = PROVIDER_RETRY_POLICY_LIMITS[field];
  return isValidInteger(parsed, min, max) ? undefined : "range";
};

export function validateProviderRetryPolicyDraft(
  draft: Partial<ProviderRetryPolicyDraft> | null | undefined,
): ProviderRetryPolicyErrors {
  const errors: ProviderRetryPolicyErrors = {};
  for (const field of providerRetryPolicyFields) {
    const error = validateField(draft?.[field], field);
    if (error) errors[field] = error;
  }
  return errors;
}

/** Validate either controlled string values or serialized numeric values. */
export function validateProviderRetryPolicy(
  value:
    | Partial<ProviderRetryPolicyDraft>
    | Partial<ProviderRetryPolicy>
    | null
    | undefined,
): ProviderRetryPolicyErrors {
  const draft: Partial<ProviderRetryPolicyDraft> = {};
  for (const field of providerRetryPolicyFields) {
    const raw = value?.[field];
    draft[field] =
      typeof raw === "string" ? raw : raw == null ? "" : String(raw);
  }
  return validateProviderRetryPolicyDraft(draft);
}

export function parseProviderRetryPolicy(
  draft: Partial<ProviderRetryPolicyDraft> | null | undefined,
): ProviderRetryPolicyParseResult {
  const errors = validateProviderRetryPolicyDraft(draft);
  if (Object.keys(errors).length > 0) {
    return { success: false, valid: false, errors };
  }

  const policy = {
    retryCount: Number(draft?.retryCount),
    perAttemptTimeoutSeconds: Number(draft?.perAttemptTimeoutSeconds),
    retryWaitSeconds: Number(draft?.retryWaitSeconds),
    unlimitedRetries: draft?.unlimitedRetries === true,
  } satisfies ProviderRetryPolicy;
  return { success: true, valid: true, policy, errors: {} };
}

export const parseProviderRetryPolicyDraft = parseProviderRetryPolicy;

export function isProviderRetryPolicySupportedApp(
  appId: string,
): appId is ProviderRetryPolicySupportedApp {
  return (PROVIDER_RETRY_POLICY_SUPPORTED_APPS as readonly string[]).includes(
    appId,
  );
}

export const supportsProviderRetryPolicy = isProviderRetryPolicySupportedApp;
export const getProviderRetryPolicyDraft = providerRetryPolicyToDraft;
