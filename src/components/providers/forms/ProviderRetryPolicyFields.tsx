import { useEffect, useState } from "react";
import { ChevronDown, ChevronRight, RefreshCw } from "lucide-react";
import { useTranslation } from "react-i18next";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { cn } from "@/lib/utils";
import {
  PROVIDER_RETRY_POLICY_DEFAULTS,
  PROVIDER_RETRY_POLICY_LIMITS,
  type ProviderRetryPolicyDraft,
  type ProviderRetryPolicyErrors,
  type ProviderRetryPolicyField,
} from "@/utils/providerRetryPolicy";

export interface ProviderRetryPolicyFieldsProps {
  value: ProviderRetryPolicyDraft;
  onChange: (value: ProviderRetryPolicyDraft) => void;
  errors?: ProviderRetryPolicyErrors;
  idPrefix?: string;
}

const FIELD_CONFIG: Array<{
  field: ProviderRetryPolicyField;
  labelKey: string;
  hintKey: string;
}> = [
  {
    field: "retryCount",
    labelKey: "retryCountLabel",
    hintKey: "retryCountHint",
  },
  {
    field: "perAttemptTimeoutSeconds",
    labelKey: "perAttemptTimeoutLabel",
    hintKey: "perAttemptTimeoutHint",
  },
  {
    field: "retryWaitSeconds",
    labelKey: "retryWaitLabel",
    hintKey: "retryWaitHint",
  },
];

function hasConfiguredRetryPolicy(value: ProviderRetryPolicyDraft) {
  return (
    value.unlimitedRetries ||
    FIELD_CONFIG.some(
      ({ field }) =>
        value[field] !== String(PROVIDER_RETRY_POLICY_DEFAULTS[field]),
    )
  );
}

export function ProviderRetryPolicyFields({
  value,
  onChange,
  errors = {},
  idPrefix = "provider-retry-policy",
}: ProviderRetryPolicyFieldsProps) {
  const { t } = useTranslation();
  const [isOpen, setIsOpen] = useState(() => hasConfiguredRetryPolicy(value));
  const hasErrors = Object.keys(errors).length > 0;

  useEffect(() => {
    if (hasConfiguredRetryPolicy(value) || hasErrors) setIsOpen(true);
  }, [hasErrors, value]);

  const updateField = (field: ProviderRetryPolicyField, nextValue: string) => {
    onChange({ ...value, [field]: nextValue });
  };

  const getErrorMessage = (field: ProviderRetryPolicyField) => {
    const error = errors[field];
    if (!error) return undefined;
    if (error === "required") {
      return t("providerRetryPolicy.errors.required", {
        defaultValue: "Enter a value.",
      });
    }
    if (error === "integer") {
      return t("providerRetryPolicy.errors.integer", {
        defaultValue: "Enter a non-negative whole number.",
      });
    }
    const { min, max } = PROVIDER_RETRY_POLICY_LIMITS[field];
    return t(`providerRetryPolicy.errors.${field}Range`, {
      min,
      max,
      defaultValue: `Enter a value from ${min} to ${max}.`,
    });
  };

  return (
    <div className="rounded-lg border border-border/50 bg-muted/20">
      <button
        type="button"
        className="flex w-full items-center justify-between p-4 text-left transition-colors hover:bg-muted/30"
        onClick={() => setIsOpen((open) => !open)}
        aria-expanded={isOpen}
      >
        <div className="flex items-center gap-3">
          <RefreshCw className="h-4 w-4 text-muted-foreground" />
          <div>
            <span className="font-medium">
              {t("providerRetryPolicy.title", {
                defaultValue: "Supplier retry",
              })}
            </span>
            <p className="mt-1 text-xs text-muted-foreground">
              {t("providerRetryPolicy.description", {
                defaultValue:
                  "Retry eligible failures against the same supplier before existing failover.",
              })}
            </p>
          </div>
        </div>
        {isOpen ? (
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-4 w-4 text-muted-foreground" />
        )}
      </button>
      <div
        className={cn(
          "overflow-hidden transition-all duration-200",
          isOpen ? "max-h-[900px] opacity-100" : "max-h-0 opacity-0",
        )}
      >
        <div className="space-y-4 border-t border-border/50 p-4">
          <p className="text-sm text-muted-foreground">
            {t("providerRetryPolicy.semantics", {
              defaultValue:
                "Retry count means retries after the first attempt unless unlimited retries is enabled. All values use seconds where shown; zero disables that setting.",
            })}
          </p>
          <div className="flex items-center justify-between gap-4 rounded-md border border-border/50 bg-background/40 p-3">
            <div className="space-y-1">
              <Label htmlFor={`${idPrefix}-unlimitedRetries`}>
                {t("providerRetryPolicy.unlimitedRetriesLabel", {
                  defaultValue: "Unlimited retries",
                })}
              </Label>
              <p
                id={`${idPrefix}-unlimitedRetries-hint`}
                className="text-xs text-muted-foreground"
              >
                {t("providerRetryPolicy.unlimitedRetriesHint", {
                  defaultValue:
                    "Keep retrying eligible failures until success, a non-retryable error, or cancellation. The retry count is ignored.",
                })}
              </p>
            </div>
            <Switch
              id={`${idPrefix}-unlimitedRetries`}
              checked={value.unlimitedRetries}
              onCheckedChange={(checked) =>
                onChange({ ...value, unlimitedRetries: checked })
              }
              aria-describedby={`${idPrefix}-unlimitedRetries-hint`}
            />
          </div>
          <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
            {FIELD_CONFIG.map(({ field, labelKey, hintKey }) => {
              const inputId = `${idPrefix}-${field}`;
              const errorMessage = getErrorMessage(field);
              const { min, max } = PROVIDER_RETRY_POLICY_LIMITS[field];
              return (
                <div className="space-y-2" key={field}>
                  <Label htmlFor={inputId}>
                    {t(`providerRetryPolicy.${labelKey}`, {
                      defaultValue:
                        field === "retryCount"
                          ? "Retries after first attempt"
                          : field === "perAttemptTimeoutSeconds"
                            ? "Per-attempt timeout"
                            : "Wait between retries",
                    })}
                  </Label>
                  <div className="flex items-center gap-2">
                    <Input
                      id={inputId}
                      type="text"
                      inputMode="numeric"
                      disabled={
                        field === "retryCount" && value.unlimitedRetries
                      }
                      value={value[field]}
                      onChange={(event) =>
                        updateField(field, event.target.value)
                      }
                      aria-invalid={Boolean(errorMessage)}
                      aria-describedby={`${inputId}-hint${errorMessage ? ` ${inputId}-error` : ""}`}
                      placeholder="0"
                    />
                    {field !== "retryCount" && (
                      <span className="shrink-0 text-sm text-muted-foreground">
                        {t("providerRetryPolicy.seconds", {
                          defaultValue: "seconds",
                        })}
                      </span>
                    )}
                  </div>
                  <p
                    id={`${inputId}-hint`}
                    className="text-xs text-muted-foreground"
                  >
                    {t(`providerRetryPolicy.${hintKey}`, {
                      min,
                      max,
                      defaultValue:
                        field === "retryCount"
                          ? `0 disables same-supplier retries; allowed range ${min}–${max}.`
                          : `0 disables this setting; allowed range ${min}–${max} seconds.`,
                    })}
                  </p>
                  {errorMessage && (
                    <p
                      id={`${inputId}-error`}
                      className="text-xs text-destructive"
                      role="alert"
                    >
                      {errorMessage}
                    </p>
                  )}
                </div>
              );
            })}
          </div>
          <p className="text-xs text-muted-foreground">
            {t("providerRetryPolicy.disabledHint", {
              defaultValue:
                "When unlimited retries is off and all three numeric values are zero, supplier retry is disabled and one initial attempt still runs.",
            })}
          </p>
          <p className="text-xs text-amber-700 dark:text-amber-300">
            {t("providerRetryPolicy.failoverWarning", {
              defaultValue:
                "With app failover enabled and unlimited retries off, maximum upstream attempts multiply: (app failover retries + 1) × (supplier retries + 1). Unlimited retries has no finite attempt cap.",
            })}
          </p>
        </div>
      </div>
    </div>
  );
}
