import { parse as parseToml } from "smol-toml";
import type { AppId } from "@/lib/api";
import type {
  ProviderModelCandidate,
  ProviderModelTestApp,
} from "@/lib/api/model-test";
import type { Provider } from "@/types";
import { parseGrokBuildConfig } from "@/utils/grokBuildConfig";

const MODEL_TEST_APPS = new Set<ProviderModelTestApp>([
  "claude",
  "codex",
  "gemini",
  "grokbuild",
]);

const asRecord = (value: unknown): Record<string, unknown> | undefined =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;

const asText = (value: unknown): string | undefined => {
  if (typeof value !== "string" && typeof value !== "number") return undefined;
  const text = String(value).trim();
  return text ? text : undefined;
};

const addCandidate = (
  candidates: ProviderModelCandidate[],
  value: unknown,
  label?: unknown,
) => {
  const id = asText(value);
  if (!id || candidates.some((candidate) => candidate.id === id)) return;
  const candidateLabel = asText(label);
  candidates.push(
    candidateLabel
      ? { id, label: candidateLabel, source: "configured" }
      : { id, source: "configured" },
  );
};

const parseEnv = (value: unknown): Record<string, unknown> => {
  const object = asRecord(value);
  if (object) return object;
  if (typeof value !== "string") return {};

  const env: Record<string, unknown> = {};
  for (const line of value.split(/\r?\n/)) {
    const match = line.match(/^\s*(?:export\s+)?([A-Z][A-Z0-9_]*)\s*=\s*(.*?)\s*$/);
    if (!match) continue;
    const raw = match[2].trim();
    env[match[1]] = raw.replace(/^(["'])(.*)\1$/, "$2");
  }
  return env;
};

const addEnvModels = (
  candidates: ProviderModelCandidate[],
  settings: Record<string, unknown>,
  keys: readonly string[],
) => {
  const env = {
    ...parseEnv(settings.env),
    ...parseEnv(settings.environment),
    ...parseEnv(settings),
  };
  for (const key of keys) addCandidate(candidates, env[key]);
};

const addValueOrValues = (
  candidates: ProviderModelCandidate[],
  value: unknown,
) => {
  if (Array.isArray(value)) {
    for (const item of value) {
      const itemRecord = asRecord(item);
      if (itemRecord) {
        addCandidate(
          candidates,
          itemRecord.id ?? itemRecord.model ?? itemRecord.name,
          itemRecord.label ?? itemRecord.display_name ?? itemRecord.displayName,
        );
      } else {
        addCandidate(candidates, item);
      }
    }
    return;
  }
  const record = asRecord(value);
  if (record) {
    for (const [id, item] of Object.entries(record)) {
      const itemRecord = asRecord(item);
      if (itemRecord) {
        addCandidate(
          candidates,
          itemRecord.id ?? itemRecord.model ?? id,
          itemRecord.label ?? itemRecord.name ?? itemRecord.display_name,
        );
      } else {
        addCandidate(candidates, id, item);
      }
    }
    return;
  }
  addCandidate(candidates, value);
};

const parseJsonValue = (value: unknown): unknown => {
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return undefined;
  }
};

const readCodexCatalog = (
  candidates: ProviderModelCandidate[],
  value: unknown,
) => {
  const parsed = parseJsonValue(value);
  if (parsed === undefined) return;
  const record = asRecord(parsed);
  if (record) {
    const entries = record.models ?? record.catalog ?? record.items;
    if (entries !== undefined) {
      addValueOrValues(candidates, entries);
      return;
    }
  }
  addValueOrValues(candidates, parsed);
};

const extractCodexCandidates = (settings: Record<string, unknown>) => {
  const candidates: ProviderModelCandidate[] = [];
  const configValue = settings.config;
  const parsedConfig =
    typeof configValue === "string"
      ? (() => {
          try {
            return asRecord(parseToml(configValue));
          } catch {
            return undefined;
          }
        })()
      : asRecord(configValue);

  const config = parsedConfig ?? {};
  addCandidate(candidates, config.model);
  addCandidate(candidates, settings.model);

  const catalogValues = [
    settings.modelCatalog,
    settings.model_catalog,
    settings.modelCatalogJson,
    settings.model_catalog_json,
    config.modelCatalog,
    config.model_catalog,
    config.model_catalog_json,
  ];
  for (const catalog of catalogValues) readCodexCatalog(candidates, catalog);

  // Some providers keep a JSON catalog in a common config snippet.
  const snippets = [settings.commonConfig, settings.common_config, config.common_config];
  for (const snippet of snippets) {
    const snippetRecord = asRecord(parseJsonValue(snippet));
    if (snippetRecord) {
      readCodexCatalog(
        candidates,
        snippetRecord.model_catalog_json ?? snippetRecord.model_catalog,
      );
    }
  }
  return candidates;
};

const extractClaudeCandidates = (settings: Record<string, unknown>) => {
  const candidates: ProviderModelCandidate[] = [];
  addEnvModels(candidates, settings, [
    "ANTHROPIC_MODEL",
    "ANTHROPIC_SMALL_FAST_MODEL",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL_NAME",
    "ANTHROPIC_DEFAULT_SONNET_MODEL",
    "ANTHROPIC_DEFAULT_OPUS_MODEL",
    "ANTHROPIC_SUBAGENT_MODEL",
    "CLAUDE_CODE_SUBAGENT_MODEL",
  ]);
  addCandidate(candidates, settings.model);
  addCandidate(candidates, settings.modelId);
  return candidates;
};

const extractGeminiCandidates = (settings: Record<string, unknown>) => {
  const candidates: ProviderModelCandidate[] = [];
  addEnvModels(candidates, settings, ["GEMINI_MODEL"]);
  addCandidate(candidates, settings.model);
  addCandidate(candidates, settings.modelId);
  return candidates;
};

const extractGrokBuildCandidates = (settings: Record<string, unknown>) => {
  const candidates: ProviderModelCandidate[] = [];
  if (typeof settings.config === "string") {
    const config = parseGrokBuildConfig(settings.config);
    addCandidate(candidates, config.upstreamModel);
  }
  addCandidate(candidates, settings.upstreamModel ?? settings.upstream_model);
  return candidates;
};

/** Returns model IDs stored in a provider's local configuration, in display order. */
export function extractProviderConfiguredModelCandidates(
  appType: ProviderModelTestApp,
  provider: Provider,
): ProviderModelCandidate[];
export function extractProviderConfiguredModelCandidates(
  provider: Provider,
  appType: ProviderModelTestApp,
): ProviderModelCandidate[];
export function extractProviderConfiguredModelCandidates(
  first: ProviderModelTestApp | Provider,
  second: Provider | ProviderModelTestApp,
): ProviderModelCandidate[] {
  const appType = (typeof first === "string" ? first : second) as ProviderModelTestApp;
  const provider = (typeof first === "string" ? second : first) as Provider;
  if (!MODEL_TEST_APPS.has(appType)) return [];
  const settings = asRecord(provider.settingsConfig) ?? {};

  switch (appType) {
    case "claude":
      return extractClaudeCandidates(settings);
    case "codex":
      return extractCodexCandidates(settings);
    case "gemini":
      return extractGeminiCandidates(settings);
    case "grokbuild":
      return extractGrokBuildCandidates(settings);
    default:
      return [];
  }
}

export const extractConfiguredProviderModelCandidates =
  extractProviderConfiguredModelCandidates;

/** Merges configured first and live candidates by exact model ID. */
export function mergeProviderModelCandidates(
  configured: readonly ProviderModelCandidate[],
  live: readonly ProviderModelCandidate[] = [],
): ProviderModelCandidate[] {
  const merged: ProviderModelCandidate[] = [];
  const seen = new Set<string>();
  for (const candidate of [...configured, ...live]) {
    const id = asText(candidate?.id);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    merged.push({
      id,
      ...(asText(candidate.label) ? { label: asText(candidate.label) } : {}),
      source: candidate.source === "live" ? "live" : "configured",
    });
  }
  return merged;
}

export function buildProviderModelCandidates(
  appType: ProviderModelTestApp,
  provider: Provider,
  live: readonly ProviderModelCandidate[] = [],
): ProviderModelCandidate[] {
  return mergeProviderModelCandidates(
    extractProviderConfiguredModelCandidates(appType, provider),
    live,
  );
}

export function isProviderModelTestEligible(
  appType: AppId,
  provider: Provider,
): appType is ProviderModelTestApp {
  if (!MODEL_TEST_APPS.has(appType as ProviderModelTestApp)) return false;
  // Older locally-created providers predate the category column and load with
  // `category === undefined`; their direct credentials still identify them as
  // custom providers. Keep them visible unless an explicit excluded category
  // or managed-auth marker is present.
  if (
    provider.category !== undefined &&
    provider.category !== "custom" &&
    provider.category !== "third_party"
  ) {
    return false;
  }
  const authBinding = provider.meta?.authBinding;
  if (
    authBinding?.source === "managed_account" ||
    /oauth|copilot/i.test(authBinding?.authProvider ?? "")
  ) {
    return false;
  }
  const settings = asRecord(provider.settingsConfig) ?? {};
  const providerType =
    provider.meta?.providerType ??
    settings.providerType ??
    settings.provider_type ??
    settings.authMode ??
    settings.auth_mode;
  if (
    typeof providerType === "string" &&
    /oauth|copilot|native|managed|gemini[_-]?cli/i.test(providerType)
  ) {
    return false;
  }
  return !["native", "isNative", "managed", "isManaged", "oauth"].some(
    (key) => settings[key] === true,
  );
}
