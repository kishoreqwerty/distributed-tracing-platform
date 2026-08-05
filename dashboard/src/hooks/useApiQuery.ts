import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError } from "../api/client";

export type QueryState<T> =
  | { status: "loading" }
  | { status: "error"; error: ApiError }
  | { status: "success"; data: T };

export interface QueryResult<T> {
  state: QueryState<T>;
  refetch: () => void;
}

/**
 * Fetches on mount and whenever `deps` changes, optionally polling on a
 * fixed interval — no user-facing setting for the interval (scope
 * discipline: no settings), just a sensible constant per call site. A
 * poll's failure doesn't clobber previously-good data with an error
 * state; only the initial fetch, an explicit refetch(), or a deps change
 * can put a view into the error state — a transient poll failure shows
 * stale-but-known data rather than yanking the screen to an error
 * message every 10s.
 *
 * `enabled: false` skips fetching entirely (state stays `loading`
 * forever, harmlessly, since a caller passing `enabled: false` is
 * expected to render something else instead of this hook's state at
 * all). Needed because a hook's own effect always runs before its
 * calling component's early returns — a view that conditionally has
 * "nothing to fetch yet" (e.g. no trace selected) would otherwise still
 * fire a request with a garbage argument every render.
 */
export function useApiQuery<T>(
  fetchFn: () => Promise<T>,
  deps: unknown[],
  pollMs?: number,
  enabled = true,
): QueryResult<T> {
  const [state, setState] = useState<QueryState<T>>({ status: "loading" });
  const [retryToken, setRetryToken] = useState(0);
  const hasData = useRef(false);

  useEffect(() => {
    if (!enabled) return;

    let cancelled = false;
    hasData.current = false;
    setState({ status: "loading" });

    async function run(isPoll: boolean) {
      try {
        const data = await fetchFn();
        if (cancelled) return;
        hasData.current = true;
        setState({ status: "success", data });
      } catch (err) {
        if (cancelled) return;
        if (isPoll && hasData.current) return; // keep showing known-good data through a transient poll failure
        setState({ status: "error", error: err instanceof ApiError ? err : new ApiError(String(err), null) });
      }
    }

    run(false);

    let interval: ReturnType<typeof setInterval> | undefined;
    if (pollMs) {
      interval = setInterval(() => run(true), pollMs);
    }
    return () => {
      cancelled = true;
      if (interval) clearInterval(interval);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, retryToken, enabled]);

  const refetch = useCallback(() => setRetryToken((t) => t + 1), []);

  return { state, refetch };
}
