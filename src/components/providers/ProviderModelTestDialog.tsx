import { useCallback, useEffect, useMemo, useState } from "react";
import {
  AlertTriangle,
  CheckCircle2,
  Loader2,
  RefreshCw,
  X,
  XCircle,
} from "lucide-react";
import { useTranslation } from "react-i18next";
import type { AppId } from "@/lib/api";
import {
  refreshProviderModelCandidates,
  type ProviderModelCandidate,
  type ProviderModelTestApp,
} from "@/lib/api/model-test";
import type { Provider } from "@/types";
import {
  extractProviderConfiguredModelCandidates,
  isProviderModelTestEligible,
  mergeProviderModelCandidates,
} from "@/utils/providerModelCandidates";
import { useProviderModelTest } from "@/hooks/useProviderModelTest";
import {
  useGlobalProxyUrl,
  useSetUpstreamProxyEnabled,
  useUpstreamProxyStatus,
} from "@/hooks/useGlobalProxy";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";

interface ProviderModelTestDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  appType: AppId;
  provider: Provider | null;
}

const isSupportedApp = (appType: AppId): appType is ProviderModelTestApp =>
  appType === "claude" ||
  appType === "codex" ||
  appType === "gemini" ||
  appType === "grokbuild";

function sanitizeError(value: unknown): string {
  const text = value instanceof Error ? value.message : String(value ?? "");
  return text
    .replace(
      /([?&](?:key|token|api_key|apikey|authorization|secret)=)[^&\s]+/gi,
      "$1[redacted]",
    )
    .replace(/(bearer\s+)[^\s]+/gi, "$1[redacted]")
    .replace(/https?:\/\/[^\s]+/gi, "[endpoint redacted]")
    .trim()
    .slice(0, 320);
}

export function ProviderModelTestDialog({
  open,
  onOpenChange,
  appType,
  provider,
}: ProviderModelTestDialogProps) {
  const { t } = useTranslation();
  const supportedApp = isSupportedApp(appType);
  const modelTestApp = supportedApp ? appType : "claude";
  const configuredCandidates = useMemo(
    () =>
      provider && supportedApp
        ? extractProviderConfiguredModelCandidates(modelTestApp, provider)
        : [],
    [modelTestApp, provider, supportedApp],
  );
  const [liveCandidates, setLiveCandidates] = useState<
    ProviderModelCandidate[]
  >([]);
  const [selectedModel, setSelectedModel] = useState("");
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const {
    result,
    error: testError,
    isStarting,
    isCancelling,
    isTesting,
    startTest,
    cancelTest,
    reset,
  } = useProviderModelTest(modelTestApp);
  const { data: savedProxyUrl, isLoading: isProxyConfigLoading } =
    useGlobalProxyUrl(open);
  const {
    data: upstreamProxyStatus,
    isLoading: isProxyStatusLoading,
    refetch: refetchUpstreamProxyStatus,
  } = useUpstreamProxyStatus(open);
  const {
    mutate: setUpstreamProxyEnabled,
    isPending: isProxyTogglePending,
    error: proxyToggleError,
    reset: resetProxyToggle,
  } = useSetUpstreamProxyEnabled();

  const candidates = useMemo(
    () => mergeProviderModelCandidates(configuredCandidates, liveCandidates),
    [configuredCandidates, liveCandidates],
  );

  const refreshCandidates = useCallback(async () => {
    if (!provider || !supportedApp) return;
    setIsRefreshing(true);
    setRefreshError(null);
    try {
      const live = await refreshProviderModelCandidates(
        modelTestApp,
        provider.id,
      );
      setLiveCandidates(live);
    } catch (error) {
      // Keep configured candidates usable when the live catalog is unavailable.
      setRefreshError(sanitizeError(error));
      setLiveCandidates([]);
    } finally {
      setIsRefreshing(false);
    }
  }, [modelTestApp, provider, supportedApp]);

  useEffect(() => {
    if (!open || !provider || !supportedApp) {
      setLiveCandidates([]);
      setRefreshError(null);
      setSelectedModel("");
      reset();
      return;
    }
    setSelectedModel("");
    setLiveCandidates([]);
    void refreshCandidates();
  }, [open, provider?.id, refreshCandidates, reset, supportedApp]);

  useEffect(() => {
    if (!selectedModel && candidates[0]) setSelectedModel(candidates[0].id);
  }, [candidates, selectedModel]);

  useEffect(() => {
    if (open) {
      void refetchUpstreamProxyStatus();
    } else {
      resetProxyToggle();
    }
  }, [open, refetchUpstreamProxyStatus, resetProxyToggle]);

  const eligible =
    supportedApp && provider
      ? isProviderModelTestEligible(modelTestApp, provider)
      : false;
  const canTest = eligible && Boolean(selectedModel) && !isTesting;
  const proxyConfigured = Boolean(savedProxyUrl?.trim());
  const proxyEnabled =
    proxyConfigured && (upstreamProxyStatus?.enabled ?? false);
  const proxyStateLoading = isProxyConfigLoading || isProxyStatusLoading;
  const proxyToggleDisabled =
    proxyStateLoading ||
    isProxyTogglePending ||
    isTesting ||
    isStarting ||
    isRefreshing ||
    !proxyConfigured;
  const proxyDescription = proxyStateLoading
    ? t("common.loading", { defaultValue: "Loading..." })
    : !proxyConfigured
      ? t("provider.modelTest.proxyUnavailable", {
          defaultValue:
            "Configure a global outbound proxy in Settings > Routing first.",
        })
      : proxyEnabled
        ? t("provider.modelTest.proxyEnabled", {
            defaultValue:
              "Model tests use the proxy configured in Settings > Routing.",
          })
        : t("provider.modelTest.proxyDisabled", {
            defaultValue:
              "Model tests connect directly. The saved proxy address is kept.",
          });

  const handleOpenChange = (nextOpen: boolean) => {
    if (!nextOpen && isTesting) void cancelTest();
    onOpenChange(nextOpen);
  };

  const handleTest = () => {
    if (!provider || !canTest) return;
    void startTest(provider.id, selectedModel);
  };

  const status = result?.status;
  const busy = isStarting || isRefreshing;

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="max-w-lg" zIndex="top">
        <DialogHeader className="relative pr-14">
          <DialogTitle>
            {t("provider.modelTest.title", { defaultValue: "Test model" })}
          </DialogTitle>
          <DialogDescription>
            {provider?.name ?? "Provider"} ·{" "}
            {t("provider.modelTest.description", {
              defaultValue:
                "Send a fixed non-streaming hi request to this provider.",
            })}
          </DialogDescription>
          <DialogClose asChild>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="absolute right-4 top-4 h-8 w-8"
              aria-label={t("common.close", { defaultValue: "Close" })}
              title={t("common.close", { defaultValue: "Close" })}
            >
              <X className="h-4 w-4" />
            </Button>
          </DialogClose>
        </DialogHeader>

        <div className="min-h-0 flex-1 overflow-y-auto px-6 py-5">
          <div className="space-y-4">
            <div className="rounded-md border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-900 dark:text-amber-200">
              <div className="flex items-start gap-2">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                <div className="space-y-1">
                  <p className="font-medium">
                    {t("provider.modelTest.billingWarning", {
                      defaultValue:
                        "This sends one billable request to the selected provider.",
                    })}
                  </p>
                  <p className="text-xs opacity-90">
                    {t("provider.modelTest.hiDisclosure", {
                      defaultValue:
                        'The request body is fixed to "hi" and does not stream.',
                    })}
                  </p>
                </div>
              </div>
            </div>

            <div className="rounded-md border bg-muted/30 p-3">
              <div className="flex items-center justify-between gap-4">
                <div className="min-w-0 space-y-1">
                  <label
                    className="text-sm font-medium"
                    htmlFor="provider-model-test-global-proxy"
                  >
                    {t("provider.modelTest.proxyToggle", {
                      defaultValue: "Global outbound proxy",
                    })}
                  </label>
                  <p
                    id="provider-model-test-global-proxy-description"
                    className="text-xs text-muted-foreground"
                  >
                    {proxyDescription}
                  </p>
                </div>
                <div className="flex shrink-0 items-center gap-2">
                  {isProxyTogglePending && (
                    <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                  )}
                  <Switch
                    id="provider-model-test-global-proxy"
                    checked={proxyEnabled}
                    onCheckedChange={setUpstreamProxyEnabled}
                    disabled={proxyToggleDisabled}
                    aria-describedby="provider-model-test-global-proxy-description"
                    aria-label={t("provider.modelTest.proxyToggle", {
                      defaultValue: "Global outbound proxy",
                    })}
                  />
                </div>
              </div>
              {proxyToggleError && (
                <p role="alert" className="mt-2 text-xs text-destructive">
                  {t("provider.modelTest.proxyToggleFailed", {
                    error: sanitizeError(proxyToggleError),
                    defaultValue: "Failed to switch the global outbound proxy.",
                  })}
                </p>
              )}
            </div>

            {!eligible && (
              <div
                role="alert"
                className="rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm"
              >
                {t("provider.modelTest.unavailable", {
                  defaultValue:
                    "Model testing is not available for this provider.",
                })}
              </div>
            )}

            <div className="space-y-2">
              <div className="flex items-center justify-between gap-2">
                <label
                  className="text-sm font-medium"
                  htmlFor="provider-model-test-select"
                >
                  {t("provider.modelTest.model", { defaultValue: "Model" })}
                </label>
                <Button
                  type="button"
                  size="sm"
                  variant="ghost"
                  onClick={() => void refreshCandidates()}
                  disabled={busy || !eligible}
                  className="h-7 px-2 text-xs"
                >
                  <RefreshCw
                    className={
                      isRefreshing ? "h-3.5 w-3.5 animate-spin" : "h-3.5 w-3.5"
                    }
                  />
                  {t("provider.modelTest.refresh", { defaultValue: "Refresh" })}
                </Button>
              </div>
              <Select
                value={selectedModel}
                onValueChange={setSelectedModel}
                disabled={!eligible || candidates.length === 0 || isTesting}
              >
                <SelectTrigger
                  id="provider-model-test-select"
                  aria-label={t("provider.modelTest.model", {
                    defaultValue: "Model",
                  })}
                >
                  <SelectValue
                    placeholder={t("provider.modelTest.noCandidates", {
                      defaultValue: "No configured or live models",
                    })}
                  />
                </SelectTrigger>
                <SelectContent className="z-[130]">
                  {candidates.map((candidate) => (
                    <SelectItem key={candidate.id} value={candidate.id}>
                      <span className="flex items-center gap-2">
                        <span className="font-mono text-xs">
                          {candidate.id}
                        </span>
                        <span className="text-xs text-muted-foreground">
                          {candidate.source === "configured"
                            ? t("provider.modelTest.configured", {
                                defaultValue: "configured",
                              })
                            : t("provider.modelTest.live", {
                                defaultValue: "live",
                              })}
                        </span>
                        {candidate.label &&
                          candidate.label !== candidate.id && (
                            <span className="text-xs text-muted-foreground">
                              {candidate.label}
                            </span>
                          )}
                      </span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {candidates.length === 0 && (
                <p className="text-xs text-muted-foreground">
                  {t("provider.modelTest.noCandidatesHint", {
                    defaultValue:
                      "Add a model to this provider before testing it.",
                  })}
                </p>
              )}
              {refreshError && (
                <p
                  role="alert"
                  className="text-xs text-amber-700 dark:text-amber-300"
                >
                  {t("provider.modelTest.refreshFailed", {
                    defaultValue:
                      "Live model refresh failed; configured models are still available.",
                  })}{" "}
                  {refreshError}
                </p>
              )}
            </div>

            {(isStarting || status === "running" || status === "retrying") && (
              <div className="flex items-center gap-2 rounded-md border p-3 text-sm">
                <Loader2 className="h-4 w-4 animate-spin" />
                {status === "retrying"
                  ? t("provider.modelTest.retrying", {
                      defaultValue: "Retrying…",
                    })
                  : t("provider.modelTest.running", {
                      defaultValue: "Testing…",
                    })}
                {result?.retriesUsed !== undefined && (
                  <span className="text-xs text-muted-foreground">
                    {t("provider.modelTest.retries", {
                      count: result.retriesUsed,
                      defaultValue: `Retries: ${result.retriesUsed}`,
                    })}
                  </span>
                )}
              </div>
            )}

            {status === "succeeded" && (
              <div className="rounded-md border border-emerald-500/30 bg-emerald-500/10 p-3 text-sm">
                <div className="flex items-center gap-2 font-medium text-emerald-700 dark:text-emerald-300">
                  <CheckCircle2 className="h-4 w-4" />
                  {t("provider.modelTest.success", {
                    defaultValue: "Model test succeeded",
                  })}
                  {result?.elapsedMs !== undefined && (
                    <span className="font-normal">{result.elapsedMs} ms</span>
                  )}
                </div>
                {result?.responseSnippet && (
                  <p className="mt-2 break-words text-xs text-emerald-900 dark:text-emerald-100">
                    {result.responseSnippet}
                  </p>
                )}
              </div>
            )}

            {(status === "failed" || status === "cancelled" || testError) && (
              <div
                role="alert"
                className="rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm"
              >
                <div className="flex items-center gap-2 font-medium text-destructive">
                  <XCircle className="h-4 w-4" />
                  {status === "cancelled"
                    ? t("provider.modelTest.cancelled", {
                        defaultValue: "Test cancelled",
                      })
                    : t("provider.modelTest.failed", {
                        defaultValue: "Model test failed",
                      })}
                </div>
                <p className="mt-1 break-words text-xs">
                  {sanitizeError(result?.message ?? testError)}
                </p>
                {result?.errorCategory && (
                  <p className="mt-1 text-xs text-muted-foreground">
                    {result.errorCategory}
                  </p>
                )}
              </div>
            )}
          </div>
        </div>

        <DialogFooter>
          {isTesting && (
            <Button
              type="button"
              variant="outline"
              onClick={() => void cancelTest()}
              disabled={isCancelling}
            >
              {isCancelling && <Loader2 className="h-4 w-4 animate-spin" />}
              {t("provider.modelTest.cancel", { defaultValue: "Cancel" })}
            </Button>
          )}
          <Button
            type="button"
            onClick={handleTest}
            disabled={!canTest || busy}
          >
            {isStarting && <Loader2 className="h-4 w-4 animate-spin" />}
            {t("provider.modelTest.test", { defaultValue: "Test" })}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
