import { describe, expect, it } from "vitest";
import type { Provider } from "@/types";
import { isProviderModelTestEligible } from "@/utils/providerModelCandidates";

function provider(overrides: Partial<Provider> = {}): Provider {
  return {
    id: "legacy-provider",
    name: "Legacy provider",
    settingsConfig: {},
    ...overrides,
  };
}

describe("isProviderModelTestEligible", () => {
  it("allows legacy direct providers whose category predates the nullable column", () => {
    expect(
      isProviderModelTestEligible("codex", provider({ category: undefined })),
    ).toBe(true);
  });

  it("still rejects explicitly unsupported categories and managed providers", () => {
    expect(
      isProviderModelTestEligible("codex", provider({ category: "official" })),
    ).toBe(false);
    expect(
      isProviderModelTestEligible(
        "codex",
        provider({
          category: undefined,
          meta: { providerType: "codex_oauth" },
        }),
      ),
    ).toBe(false);
  });
});
