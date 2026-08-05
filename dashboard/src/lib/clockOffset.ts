// Judgment call, not independently tuned: reuses this project's existing
// ANALYZER_BASELINE_MIN_SAMPLES default (30) as a reasonable proxy for
// "enough observations behind an estimate to trust it" — clock-offset
// confidence is the same kind of quantity (a count of underlying
// resolved-pair observations, see clockskew.py). The API deliberately
// returns raw confidence rather than deciding this itself — see
// docs/DECISIONS.md — so the threshold lives here, once, rather than
// being re-guessed at every call site that renders an offset.
export const CLOCK_OFFSET_CONFIDENCE_THRESHOLD = 30;

export function hasSufficientConfidence(confidence: number): boolean {
  return confidence >= CLOCK_OFFSET_CONFIDENCE_THRESHOLD;
}

export function formatOffset(offsetNs: number): string {
  const ms = offsetNs / 1e6;
  const sign = ms > 0 ? "+" : "";
  return `${sign}${ms.toFixed(1)}ms`;
}
