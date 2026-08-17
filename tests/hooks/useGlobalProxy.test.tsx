import { renderHook, act, waitFor } from "@testing-library/react";
import { QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  useGlobalProxyUrl,
  useSetUpstreamProxyEnabled,
} from "@/hooks/useGlobalProxy";
import { createTestQueryClient } from "../utils/testQueryClient";

const apiMocks = vi.hoisted(() => ({
  getGlobalProxyUrl: vi.fn(),
  setGlobalProxyUrl: vi.fn(),
  testProxyUrl: vi.fn(),
  getUpstreamProxyStatus: vi.fn(),
  setUpstreamProxyEnabled: vi.fn(),
  scanLocalProxies: vi.fn(),
}));

vi.mock("@/lib/api/globalProxy", () => ({
  getGlobalProxyUrl: apiMocks.getGlobalProxyUrl,
  setGlobalProxyUrl: apiMocks.setGlobalProxyUrl,
  testProxyUrl: apiMocks.testProxyUrl,
  getUpstreamProxyStatus: apiMocks.getUpstreamProxyStatus,
  setUpstreamProxyEnabled: apiMocks.setUpstreamProxyEnabled,
  scanLocalProxies: apiMocks.scanLocalProxies,
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const wrapper = ({ children }: { children: ReactNode }) => {
  const client = createTestQueryClient();
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
};

describe("useGlobalProxy runtime toggle", () => {
  beforeEach(() => {
    apiMocks.getGlobalProxyUrl.mockReset();
    apiMocks.setGlobalProxyUrl.mockReset();
    apiMocks.testProxyUrl.mockReset();
    apiMocks.getUpstreamProxyStatus.mockReset();
    apiMocks.setUpstreamProxyEnabled.mockReset();
    apiMocks.scanLocalProxies.mockReset();
  });

  it("does not fetch the saved URL when disabled", async () => {
    const { result } = renderHook(() => useGlobalProxyUrl(false), { wrapper });

    expect(result.current.fetchStatus).toBe("idle");
    expect(apiMocks.getGlobalProxyUrl).not.toHaveBeenCalled();
    await waitFor(() => expect(result.current.fetchStatus).toBe("idle"));
  });

  it("updates the runtime status cache after a successful toggle", async () => {
    const nextStatus = {
      enabled: false,
      proxyUrl: null,
    };
    apiMocks.setUpstreamProxyEnabled.mockResolvedValue(nextStatus);
    const client = createTestQueryClient();
    client.setQueryData(["upstreamProxyStatus"], {
      enabled: true,
      proxyUrl: "http://127.0.0.1:7890",
    });
    const testWrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );

    const { result } = renderHook(() => useSetUpstreamProxyEnabled(), {
      wrapper: testWrapper,
    });

    await act(async () => {
      await result.current.mutateAsync(false);
    });

    expect(apiMocks.setUpstreamProxyEnabled.mock.calls[0]?.[0]).toBe(false);
    expect(client.getQueryData(["upstreamProxyStatus"])).toEqual(nextStatus);
  });
});
