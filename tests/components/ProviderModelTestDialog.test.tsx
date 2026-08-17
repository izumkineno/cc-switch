import type { ReactNode } from "react";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ProviderModelTestDialog } from "@/components/providers/ProviderModelTestDialog";
import type { Provider } from "@/types";

const mocks = vi.hoisted(() => ({
  refreshCandidates: vi.fn(),
  startTest: vi.fn(),
  cancelTest: vi.fn(),
  resetTest: vi.fn(),
  refetchProxyStatus: vi.fn(),
  toggleProxy: vi.fn(),
  resetProxyToggle: vi.fn(),
  proxyConfigEnabled: [] as boolean[],
  proxyStatusEnabled: [] as boolean[],
  proxy: {
    savedUrl: "http://127.0.0.1:7890" as string | null,
    status: {
      enabled: true,
      proxyUrl: "http://127.0.0.1:7890",
    },
    configLoading: false,
    statusLoading: false,
    togglePending: false,
    toggleError: null as Error | null,
  },
  modelTest: {
    result: null,
    error: null as string | null,
    isStarting: false,
    isCancelling: false,
    isTesting: false,
  },
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string) => key,
  }),
}));

vi.mock("@/lib/api/model-test", () => ({
  refreshProviderModelCandidates: mocks.refreshCandidates,
}));

vi.mock("@/hooks/useProviderModelTest", () => ({
  useProviderModelTest: () => ({
    ...mocks.modelTest,
    startTest: mocks.startTest,
    cancelTest: mocks.cancelTest,
    reset: mocks.resetTest,
  }),
}));

vi.mock("@/hooks/useGlobalProxy", () => ({
  useGlobalProxyUrl: (enabled: boolean) => {
    mocks.proxyConfigEnabled.push(enabled);
    return {
      data: mocks.proxy.savedUrl,
      isLoading: mocks.proxy.configLoading,
    };
  },
  useUpstreamProxyStatus: (enabled: boolean) => {
    mocks.proxyStatusEnabled.push(enabled);
    return {
      data: mocks.proxy.status,
      isLoading: mocks.proxy.statusLoading,
      refetch: mocks.refetchProxyStatus,
    };
  },
  useSetUpstreamProxyEnabled: () => ({
    mutate: mocks.toggleProxy,
    isPending: mocks.proxy.togglePending,
    error: mocks.proxy.toggleError,
    reset: mocks.resetProxyToggle,
  }),
}));

type WrapperProps = { children: ReactNode; [key: string]: unknown };

vi.mock("@/components/ui/dialog", () => ({
  Dialog: ({ open, children }: { open: boolean; children: ReactNode }) =>
    open ? <div data-testid="model-test-dialog">{children}</div> : null,
  DialogClose: ({ children }: WrapperProps) => <>{children}</>,
  DialogContent: ({ children }: WrapperProps) => <div>{children}</div>,
  DialogDescription: ({ children }: WrapperProps) => <p>{children}</p>,
  DialogFooter: ({ children }: WrapperProps) => <div>{children}</div>,
  DialogHeader: ({ children }: WrapperProps) => <div>{children}</div>,
  DialogTitle: ({ children }: WrapperProps) => <h2>{children}</h2>,
}));

const provider: Provider = {
  id: "provider-1",
  name: "Acme Provider",
  category: "custom",
  settingsConfig: {
    env: {
      ANTHROPIC_MODEL: "claude-test-model",
    },
  },
};

function renderDialog(open = true) {
  return render(
    <ProviderModelTestDialog
      open={open}
      onOpenChange={vi.fn()}
      appType="claude"
      provider={provider}
    />,
  );
}

describe("ProviderModelTestDialog global proxy control", () => {
  beforeEach(() => {
    mocks.refreshCandidates.mockReset().mockResolvedValue([]);
    mocks.startTest.mockReset();
    mocks.cancelTest.mockReset().mockResolvedValue(undefined);
    mocks.resetTest.mockReset();
    mocks.refetchProxyStatus.mockReset().mockResolvedValue(undefined);
    mocks.toggleProxy.mockReset();
    mocks.resetProxyToggle.mockReset();
    mocks.proxyConfigEnabled.length = 0;
    mocks.proxyStatusEnabled.length = 0;
    mocks.proxy.savedUrl = "http://127.0.0.1:7890";
    mocks.proxy.status = {
      enabled: true,
      proxyUrl: "http://127.0.0.1:7890",
    };
    mocks.proxy.configLoading = false;
    mocks.proxy.statusLoading = false;
    mocks.proxy.togglePending = false;
    mocks.proxy.toggleError = null;
    mocks.modelTest.result = null;
    mocks.modelTest.error = null;
    mocks.modelTest.isStarting = false;
    mocks.modelTest.isCancelling = false;
    mocks.modelTest.isTesting = false;
  });

  it("loads proxy state only while open and refreshes runtime status", async () => {
    renderDialog(false);

    expect(mocks.proxyConfigEnabled).toContain(false);
    expect(mocks.proxyStatusEnabled).toContain(false);
    expect(mocks.refetchProxyStatus).not.toHaveBeenCalled();

    renderDialog(true);

    expect(mocks.proxyConfigEnabled).toContain(true);
    expect(mocks.proxyStatusEnabled).toContain(true);
    await waitFor(() =>
      expect(mocks.refetchProxyStatus).toHaveBeenCalledTimes(1),
    );
    expect(
      screen.getByRole("switch", { name: "provider.modelTest.proxyToggle" }),
    ).toBeChecked();
  });

  it("toggles runtime proxy usage without changing saved configuration", async () => {
    const user = userEvent.setup();
    renderDialog();

    const toggle = screen.getByRole("switch", {
      name: "provider.modelTest.proxyToggle",
    });
    await waitFor(() => expect(toggle).toBeEnabled());
    await user.click(toggle);

    expect(mocks.toggleProxy.mock.calls[0]?.[0]).toBe(false);
    expect(mocks.proxy.savedUrl).toBe("http://127.0.0.1:7890");
  });

  it("disables the switch when no saved proxy is available", async () => {
    mocks.proxy.savedUrl = null;
    mocks.proxy.status = {
      enabled: true,
      proxyUrl: "http://127.0.0.1:7890",
    };

    renderDialog();

    const toggle = screen.getByRole("switch", {
      name: "provider.modelTest.proxyToggle",
    });
    await waitFor(() => expect(toggle).toBeDisabled());
    expect(toggle).not.toBeChecked();
    expect(
      screen.getByText("provider.modelTest.proxyUnavailable"),
    ).toBeInTheDocument();
  });

  it("keeps the proxy switch disabled while a model test is active", async () => {
    mocks.modelTest.isTesting = true;

    renderDialog();

    const toggle = screen.getByRole("switch", {
      name: "provider.modelTest.proxyToggle",
    });
    await waitFor(() => expect(toggle).toBeDisabled());
  });

  it("shows a sanitized proxy toggle error", async () => {
    mocks.proxy.toggleError = new Error(
      "proxy switch failed: https://user:secret@example.com",
    );

    renderDialog();

    await waitFor(() =>
      expect(
        screen.getByText("provider.modelTest.proxyToggleFailed"),
      ).toBeInTheDocument(),
    );
    expect(screen.queryByText(/user:secret/)).not.toBeInTheDocument();
  });
});
