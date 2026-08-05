import type { TimeRange } from "../api/client";

const PRESETS: { label: string; ms: number }[] = [
  { label: "15m", ms: 15 * 60 * 1000 },
  { label: "1h", ms: 60 * 60 * 1000 },
  { label: "6h", ms: 6 * 60 * 60 * 1000 },
  { label: "24h", ms: 24 * 60 * 60 * 1000 },
];

export function presetRange(ms: number): TimeRange {
  const end = new Date();
  const start = new Date(end.getTime() - ms);
  return { start: start.toISOString(), end: end.toISOString() };
}

function toLocalInputValue(iso: string): string {
  // datetime-local wants "YYYY-MM-DDTHH:mm" in local time, no timezone suffix
  const d = new Date(iso);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

export function TimeRangePicker({ range, onChange }: { range: TimeRange; onChange: (range: TimeRange) => void }) {
  return (
    <div className="time-range-picker">
      {PRESETS.map((p) => (
        <button key={p.label} className="time-range-picker__preset" onClick={() => onChange(presetRange(p.ms))}>
          {p.label}
        </button>
      ))}
      <span className="time-range-picker__custom data">
        <input
          type="datetime-local"
          value={toLocalInputValue(range.start)}
          onChange={(e) => e.target.value && onChange({ ...range, start: new Date(e.target.value).toISOString() })}
        />
        <span className="text-faint">→</span>
        <input
          type="datetime-local"
          value={toLocalInputValue(range.end)}
          onChange={(e) => e.target.value && onChange({ ...range, end: new Date(e.target.value).toISOString() })}
        />
      </span>
    </div>
  );
}
