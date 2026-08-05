import { useState } from "react";
import { Nav, type View } from "./components/Nav";
import { presetRange, TimeRangePicker } from "./components/TimeRangePicker";
import type { TimeRange } from "./api/client";
import { TopologyView } from "./views/TopologyView";
import { TraceView } from "./views/TraceView";
import { IncidentsView } from "./views/IncidentsView";

export default function App() {
  const [range, setRange] = useState<TimeRange>(() => presetRange(60 * 60 * 1000));
  const [view, setView] = useState<View>("topology");
  const [selectedService, setSelectedService] = useState<string | undefined>(undefined);
  const [selectedTraceId, setSelectedTraceId] = useState<string | undefined>(undefined);

  function openTrace(traceId: string) {
    setSelectedTraceId(traceId);
    setView("trace");
  }

  return (
    <div className="app">
      <header className="app__header">
        <h1 className="app__title">distributed tracing platform</h1>
        <TimeRangePicker range={range} onChange={setRange} />
      </header>
      <Nav active={view} onSelect={setView} />
      <main className="app__main">
        {view === "topology" && (
          <TopologyView range={range} selectedService={selectedService} onSelectService={setSelectedService} />
        )}
        {view === "incidents" && (
          <IncidentsView
            range={range}
            targetFilter={selectedService}
            onSelectService={setSelectedService}
            onSelectTrace={openTrace}
          />
        )}
        {view === "trace" && <TraceView traceId={selectedTraceId} />}
      </main>
    </div>
  );
}
