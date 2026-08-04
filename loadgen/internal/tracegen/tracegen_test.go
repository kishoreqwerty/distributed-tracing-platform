package tracegen

import "testing"

func TestTraceProducesValidSpanCount(t *testing.T) {
	for i := 0; i < 20; i++ {
		rs := Trace()

		total := 0
		var traceID []byte
		for _, r := range rs {
			for _, ss := range r.ScopeSpans {
				for _, s := range ss.Spans {
					total++
					if traceID == nil {
						traceID = s.TraceId
					}
					if string(s.TraceId) != string(traceID) {
						t.Errorf("span has mismatched trace_id")
					}
					if len(s.SpanId) != 8 {
						t.Errorf("span_id length = %d, want 8", len(s.SpanId))
					}
				}
			}
		}

		if total < 3 || total > 5 {
			t.Errorf("span count = %d, want 3-5", total)
		}
	}
}

func TestTraceUsesOnlyKnownServices(t *testing.T) {
	rs := Trace()

	known := map[string]bool{}
	for _, svc := range Services {
		known[svc] = true
	}

	for _, r := range rs {
		svc := r.Resource.Attributes[0].Value.GetStringValue()
		if !known[svc] {
			t.Errorf("unexpected service name %q", svc)
		}
	}
}
