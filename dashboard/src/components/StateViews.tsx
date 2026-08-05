import type { ApiError } from "../api/client";

/** No request has failed and data came back — there just isn't any for
 * this filter/range. Distinct from ErrorState (the request itself
 * failed) and from a component simply rendering nothing, which the
 * phase's own constraints rule out: every view must say what happened.
 */
export function EmptyState({ title, hint }: { title: string; hint?: string }) {
  return (
    <div className="state-view" role="status">
      <p className="state-view__title text-dim">{title}</p>
      {hint && <p className="state-view__hint text-faint">{hint}</p>}
    </div>
  );
}

export function ErrorState({ error, onRetry }: { error: ApiError; onRetry?: () => void }) {
  const message =
    error.status === null
      ? "Could not reach the API. Check that analyzer-api is running and reachable."
      : `API returned ${error.status}: ${error.message}`;
  return (
    <div className="state-view state-view--error" role="alert">
      <p className="state-view__title">{message}</p>
      {onRetry && (
        <button className="link-button" onClick={onRetry}>
          Retry
        </button>
      )}
    </div>
  );
}

export function LoadingState() {
  return (
    <div className="state-view" role="status" aria-label="loading">
      <p className="text-faint">Loading…</p>
    </div>
  );
}
