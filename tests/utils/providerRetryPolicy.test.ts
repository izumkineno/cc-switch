import { describe, expect, it } from "vitest";
import {
  createDefaultProviderRetryPolicyDraft,
  isProviderRetryPolicySupportedApp,
  normalizeProviderRetryPolicy,
  parseProviderRetryPolicy,
  validateProviderRetryPolicyDraft,
} from "@/utils/providerRetryPolicy";

describe("providerRetryPolicy", () => {
  it("normalizes missing and out-of-range metadata to the disabled defaults", () => {
    expect(
      normalizeProviderRetryPolicy({
        retryCount: 2,
        perAttemptTimeoutSeconds: 1201,
        retryWaitSeconds: -1,
      }),
    ).toEqual({
      retryCount: 2,
      perAttemptTimeoutSeconds: 0,
      retryWaitSeconds: 0,
      unlimitedRetries: false,
    });
    expect(normalizeProviderRetryPolicy()).toEqual({
      retryCount: 0,
      perAttemptTimeoutSeconds: 0,
      retryWaitSeconds: 0,
      unlimitedRetries: false,
    });
  });

  it("parses controlled string values into a numeric policy", () => {
    const result = parseProviderRetryPolicy({
      retryCount: "3",
      perAttemptTimeoutSeconds: "120",
      retryWaitSeconds: "5",
      unlimitedRetries: true,
    });

    expect(result).toEqual({
      success: true,
      valid: true,
      policy: {
        retryCount: 3,
        perAttemptTimeoutSeconds: 120,
        retryWaitSeconds: 5,
        unlimitedRetries: true,
      },
      errors: {},
    });
  });

  it("reports required, integer, and range errors for drafts", () => {
    const errors = validateProviderRetryPolicyDraft({
      retryCount: "",
      perAttemptTimeoutSeconds: "1.5",
      retryWaitSeconds: "61",
    });

    expect(errors).toEqual({
      retryCount: "required",
      perAttemptTimeoutSeconds: "integer",
      retryWaitSeconds: "range",
    });
    expect(
      parseProviderRetryPolicy(createDefaultProviderRetryPolicyDraft()),
    ).toMatchObject({
      success: true,
      valid: true,
    });
  });

  it("limits controls to the four supported additive apps", () => {
    expect(
      ["claude", "codex", "gemini", "grokbuild"].every(
        isProviderRetryPolicySupportedApp,
      ),
    ).toBe(true);
    expect(isProviderRetryPolicySupportedApp("claude-desktop")).toBe(false);
  });
});
