import { useCallback, useEffect, useRef, useState } from "react";
import type {
  ProviderModelTestResult,
  ProviderModelTestStatus,
  ProviderModelTestApp,
} from "@/lib/api/model-test";
import {
  cancelProviderModelTest,
  getProviderModelTestResult,
  startProviderModelTest,
} from "@/lib/api/model-test";

const POLL_INTERVAL_MS = 250;
const TERMINAL_STATUSES = new Set<ProviderModelTestStatus>([
  "succeeded",
  "failed",
  "cancelled",
]);

export interface ProviderModelTestState {
  operationId: string | null;
  providerId: string | null;
  modelId: string | null;
  result: ProviderModelTestResult | null;
  isStarting: boolean;
  isCancelling: boolean;
  error: string | null;
}

const INITIAL_STATE: ProviderModelTestState = {
  operationId: null,
  providerId: null,
  modelId: null,
  result: null,
  isStarting: false,
  isCancelling: false,
  error: null,
};

/** Owns one model-test operation and ignores stale results from older operations. */
export function useProviderModelTest(appType: ProviderModelTestApp) {
  const [state, setState] = useState<ProviderModelTestState>(INITIAL_STATE);
  const generationRef = useRef(0);
  const operationRef = useRef<string | null>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const clearTimer = useCallback(() => {
    if (timerRef.current !== null) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }
  }, []);

  const poll = useCallback(
    (operationId: string, generation: number) => {
      clearTimer();
      timerRef.current = setTimeout(async () => {
        if (
          generationRef.current !== generation ||
          operationRef.current !== operationId
        ) {
          return;
        }
        try {
          const result = await getProviderModelTestResult(operationId);
          if (
            generationRef.current !== generation ||
            operationRef.current !== operationId
          ) {
            return;
          }
          setState((current) => ({ ...current, result, error: null }));
          if (TERMINAL_STATUSES.has(result.status)) {
            operationRef.current = null;
            return;
          }
          poll(operationId, generation);
        } catch (error) {
          if (
            generationRef.current !== generation ||
            operationRef.current !== operationId
          ) {
            return;
          }
          setState((current) => ({
            ...current,
            error: error instanceof Error ? error.message : String(error),
          }));
          // A transient read failure should not orphan the operation.
          poll(operationId, generation);
        }
      }, POLL_INTERVAL_MS);
    },
    [clearTimer],
  );

  const startTest = useCallback(
    async (providerId: string, modelId: string) => {
      clearTimer();
      generationRef.current += 1;
      const generation = generationRef.current;
      operationRef.current = null;
      setState({
        operationId: null,
        providerId,
        modelId,
        result: null,
        isStarting: true,
        isCancelling: false,
        error: null,
      });
      try {
        const started = await startProviderModelTest(appType, providerId, modelId);
        if (generationRef.current !== generation) {
          void cancelProviderModelTest(started.operationId).catch(
            () => undefined,
          );
          return null;
        }
        operationRef.current = started.operationId;
        setState((current) => ({
          ...current,
          operationId: started.operationId,
          isStarting: false,
        }));
        poll(started.operationId, generation);
        return started.operationId;
      } catch (error) {
        if (generationRef.current === generation) {
          setState((current) => ({
            ...current,
            isStarting: false,
            error: error instanceof Error ? error.message : String(error),
          }));
        }
        return null;
      }
    },
    [appType, clearTimer, poll],
  );

  const cancelTest = useCallback(async () => {
    const operationId = operationRef.current;
    if (!operationId) {
      generationRef.current += 1;
      clearTimer();
      setState((current) => ({
        ...current,
        isStarting: false,
        isCancelling: false,
      }));
      return;
    }
    const generation = generationRef.current;
    setState((current) => ({ ...current, isCancelling: true }));
    try {
      await cancelProviderModelTest(operationId);
      if (
        generationRef.current !== generation ||
        operationRef.current !== operationId
      ) {
        return;
      }
      const result = await getProviderModelTestResult(operationId);
      if (
        generationRef.current !== generation ||
        operationRef.current !== operationId
      ) {
        return;
      }
      setState((current) => ({
        ...current,
        result,
        isCancelling: false,
      }));
      if (TERMINAL_STATUSES.has(result.status)) {
        operationRef.current = null;
        clearTimer();
      }
    } catch (error) {
      if (generationRef.current === generation) {
        setState((current) => ({
          ...current,
          isCancelling: false,
          error: error instanceof Error ? error.message : String(error),
        }));
      }
    }
  }, [clearTimer]);

  const reset = useCallback(() => {
    generationRef.current += 1;
    operationRef.current = null;
    clearTimer();
    setState(INITIAL_STATE);
  }, [clearTimer]);

  useEffect(() => {
    return () => {
      const operationId = operationRef.current;
      if (operationId) {
        void cancelProviderModelTest(operationId).catch(() => undefined);
      }
      generationRef.current += 1;
      operationRef.current = null;
      clearTimer();
    };
  }, [clearTimer]);

  return {
    ...state,
    isTesting: state.isStarting || Boolean(operationRef.current),
    startTest,
    cancelTest,
    reset,
  };
}
