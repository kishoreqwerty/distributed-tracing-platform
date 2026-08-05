export type View = "topology" | "trace" | "incidents";

const VIEWS: { id: View; label: string }[] = [
  { id: "topology", label: "Topology" },
  { id: "incidents", label: "Incidents" },
  { id: "trace", label: "Trace" },
];

export function Nav({ active, onSelect }: { active: View; onSelect: (v: View) => void }) {
  return (
    <nav className="nav">
      {VIEWS.map((v) => (
        <button key={v.id} className={`nav__item ${active === v.id ? "nav__item--active" : ""}`} onClick={() => onSelect(v.id)}>
          {v.label}
        </button>
      ))}
    </nav>
  );
}
