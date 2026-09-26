package telemetry

import (
	"io"
	"log/slog"
	"testing"

	"bambu-mqtt-proxy/internal/config"
)

func TestMergeErrorsAndHMS(t *testing.T) {
	c := NewCache([]config.Printer{{Serial: "S1", Name: "Garage"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.Observe("S1", []byte(`{"print":{"print_error":50348044,"hms":[{"attr":50331904,"code":131079},{"attr":"not-a-number","code":1}]}}`))
	st := c.Snapshot()[0]
	if st.Name != "Garage" || st.LastReport.IsZero() {
		t.Fatalf("name/last report = %q/%v", st.Name, st.LastReport)
	}
	if st.PrintError != 50348044 {
		t.Fatalf("print_error = %d", st.PrintError)
	}
	if len(st.HMS) != 1 || st.HMS[0].ID() != "HMS_0300_0100_0002_0007" || st.HMS[0].Severity() != "serious" {
		t.Fatalf("hms = %+v", st.HMS)
	}

	// A report without the keys keeps them; explicit clears replace them.
	c.Observe("S1", []byte(`{"print":{"mc_percent":5}}`))
	if st := c.Snapshot()[0]; st.PrintError == 0 || len(st.HMS) != 1 {
		t.Fatalf("absent keys must not clear errors: %+v", st)
	}
	c.Observe("S1", []byte(`{"print":{"print_error":0,"hms":[]}}`))
	if st := c.Snapshot()[0]; st.PrintError != 0 || len(st.HMS) != 0 {
		t.Fatalf("explicit clear failed: %+v", st)
	}
}
