package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

func TestSaveSnapshotAndLoadLatestSnapshot(t *testing.T) {
	origSnapshotDir := snapshotDir
	snapshotDir = t.TempDir()
	defer func() { snapshotDir = origSnapshotDir }()

	first := DiffSnapshot{Timestamp: time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)}
	second := DiffSnapshot{Timestamp: first.Timestamp.Add(time.Minute), Scheduler: "healthy"}

	if err := saveSnapshot(first); err != nil {
		t.Fatalf("saveSnapshot(first) error = %v", err)
	}
	if err := saveSnapshot(second); err != nil {
		t.Fatalf("saveSnapshot(second) error = %v", err)
	}

	loaded, err := loadLatestSnapshot()
	if err != nil {
		t.Fatalf("loadLatestSnapshot() error = %v", err)
	}
	if loaded.Timestamp != second.Timestamp || loaded.Scheduler != "healthy" {
		t.Fatalf("loadLatestSnapshot() = %#v", loaded)
	}
}

func TestRunDiffWithoutPreviousSnapshot(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "http://airflow.example")

	origSnapshotDir := snapshotDir
	snapshotDir = t.TempDir()
	defer func() { snapshotDir = origSnapshotDir }()

	restoreTransport := withDefaultTransportHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.RequestURI() {
		case "/health":
			_, _ = fmt.Fprint(w, `{"metadatabase":{"status":"healthy"},"scheduler":{"status":"healthy"}}`)
		case "/dags/~/dagRuns?limit=200&order_by=-start_date":
			_, _ = fmt.Fprint(w, `{"dag_runs":[],"total_entries":0}`)
		case "/pools":
			_, _ = fmt.Fprint(w, `{"pools":[],"total_entries":0}`)
		case "/importErrors":
			_, _ = fmt.Fprint(w, `{"import_errors":[],"total_entries":0}`)
		default:
			t.Fatalf("unexpected request %q", r.URL.RequestURI())
		}
	}))
	defer restoreTransport()

	cmd, out, _ := newTestCommand(t, func(cmd *cobra.Command) {
		cmd.Flags().String("format", "text", "")
	})

	if err := runDiff(cmd, nil); err != nil {
		t.Fatalf("runDiff() error = %v", err)
	}
	if !strings.Contains(out.String(), "No previous snapshot found") {
		t.Fatalf("runDiff() output = %q", out.String())
	}

	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("snapshot files = %d, want 1", len(entries))
	}
}

func TestRunDiffJSON(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "http://airflow.example")

	origSnapshotDir := snapshotDir
	snapshotDir = t.TempDir()
	defer func() { snapshotDir = origSnapshotDir }()

	prev := DiffSnapshot{
		Timestamp:    time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC),
		DAGRunStates: map[string]string{"example::run-1": "success"},
		PoolOpen:     map[string]int{"default": 3},
		ImportErrors: 1,
		Scheduler:    "healthy",
	}
	if err := saveSnapshot(prev); err != nil {
		t.Fatalf("saveSnapshot(prev) error = %v", err)
	}

	restoreTransport := withDefaultTransportHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.RequestURI() {
		case "/health":
			_, _ = fmt.Fprint(w, `{"metadatabase":{"status":"healthy"},"scheduler":{"status":"unhealthy"}}`)
		case "/dags/~/dagRuns?limit=200&order_by=-start_date":
			_, _ = fmt.Fprint(w, `{"dag_runs":[{"dag_id":"example","dag_run_id":"run-1","state":"failed"}],"total_entries":1}`)
		case "/pools":
			_, _ = fmt.Fprint(w, `{"pools":[{"name":"default","open_slots":0}],"total_entries":1}`)
		case "/importErrors":
			_, _ = fmt.Fprint(w, `{"import_errors":[{"import_error_id":1},{"import_error_id":2}],"total_entries":2}`)
		default:
			t.Fatalf("unexpected request %q", r.URL.RequestURI())
		}
	}))
	defer restoreTransport()

	cmd, out, _ := newTestCommand(t, func(cmd *cobra.Command) {
		cmd.Flags().String("format", "text", "")
		if err := cmd.Flags().Set("format", "json"); err != nil {
			t.Fatalf("set format flag: %v", err)
		}
	})

	if err := runDiff(cmd, nil); err != nil {
		t.Fatalf("runDiff() error = %v", err)
	}

	var result DiffResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if !result.HasChanges || len(result.NewFailures) != 1 || result.SchedulerDiff == nil {
		t.Fatalf("runDiff() result = %#v", result)
	}

	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("snapshot files = %d, want 2", len(entries))
	}
	latestPath := filepath.Join(snapshotDir, entries[len(entries)-1].Name())
	if _, err := os.Stat(latestPath); err != nil {
		t.Fatalf("Stat(%q) error = %v", latestPath, err)
	}
}
