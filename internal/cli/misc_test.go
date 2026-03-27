package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/ppiankov/airflowpulse/internal/config"
)

func TestRunDepsFormats(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "http://airflow.example")

	restoreTransport := withDefaultTransportHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RequestURI() != "/dags/example/tasks" {
			t.Fatalf("unexpected request %q", r.URL.RequestURI())
		}
		_, _ = fmt.Fprint(w, `{
			"tasks":[
				{"task_id":"extract","downstream_task_ids":["transform"]},
				{"task_id":"transform","downstream_task_ids":["load"]},
				{"task_id":"load","downstream_task_ids":[]}
			],
			"total_entries":3
		}`)
	}))
	defer restoreTransport()

	cmd, out, _ := newTestCommand(t, func(cmd *cobra.Command) {
		cmd.Flags().String("format", "text", "")
		cmd.Flags().String("task", "", "")
		if err := cmd.Flags().Set("task", "transform"); err != nil {
			t.Fatalf("set task flag: %v", err)
		}
	})
	if err := runDeps(cmd, []string{"example"}); err != nil {
		t.Fatalf("runDeps() text error = %v", err)
	}
	if !strings.Contains(out.String(), ">> transform") {
		t.Fatalf("deps text output = %q", out.String())
	}

	cmdJSON, outJSON, _ := newTestCommand(t, func(cmd *cobra.Command) {
		cmd.Flags().String("format", "text", "")
		cmd.Flags().String("task", "", "")
		if err := cmd.Flags().Set("format", "json"); err != nil {
			t.Fatalf("set format flag: %v", err)
		}
	})
	if err := runDeps(cmdJSON, []string{"example"}); err != nil {
		t.Fatalf("runDeps() json error = %v", err)
	}
	var jsonResult DepsResult
	if err := json.Unmarshal(outJSON.Bytes(), &jsonResult); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(jsonResult.Tasks) != 3 {
		t.Fatalf("deps json tasks len = %d, want 3", len(jsonResult.Tasks))
	}

	cmdDOT, outDOT, _ := newTestCommand(t, func(cmd *cobra.Command) {
		cmd.Flags().String("format", "text", "")
		cmd.Flags().String("task", "", "")
		if err := cmd.Flags().Set("format", "dot"); err != nil {
			t.Fatalf("set format flag: %v", err)
		}
	})
	if err := runDeps(cmdDOT, []string{"example"}); err != nil {
		t.Fatalf("runDeps() dot error = %v", err)
	}
	if !strings.Contains(outDOT.String(), `"extract" -> "transform"`) {
		t.Fatalf("deps dot output = %q", outDOT.String())
	}
}

func TestRunDepsNotFound(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "http://airflow.example")

	restoreTransport := withDefaultTransportHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, "missing")
	}))
	defer restoreTransport()

	cmd, _, _ := newTestCommand(t, func(cmd *cobra.Command) {
		cmd.Flags().String("format", "text", "")
		cmd.Flags().String("task", "", "")
	})

	err := runDeps(cmd, []string{"missing"})
	if err == nil || !strings.Contains(err.Error(), `DAG "missing" not found`) {
		t.Fatalf("runDeps() error = %v, want not found", err)
	}
}

func TestTakeSnapshotAndPollAndEmit(t *testing.T) {
	cfg := &config.Config{APIURLs: []string{"http://airflow.example"}}

	restoreTransport := withDefaultTransportHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.RequestURI() {
		case "/health":
			_, _ = fmt.Fprint(w, `{"metadatabase":{"status":"healthy"},"scheduler":{"status":"unhealthy"}}`)
		case "/dags/~/dagRuns?limit=200&order_by=-start_date":
			_, _ = fmt.Fprint(w, `{"dag_runs":[{"dag_id":"example","dag_run_id":"run-1","state":"failed"}],"total_entries":1}`)
		case "/pools":
			_, _ = fmt.Fprint(w, `{"pools":[{"name":"default","open_slots":0,"queued_slots":2}],"total_entries":1}`)
		case "/importErrors":
			_, _ = fmt.Fprint(w, `{"import_errors":[{"import_error_id":1}],"total_entries":1}`)
		default:
			t.Fatalf("unexpected request %q", r.URL.RequestURI())
		}
	}))
	defer restoreTransport()

	snapshot := takeSnapshot(context.Background(), cfg)
	if snapshot.Scheduler != "unhealthy" {
		t.Fatalf("takeSnapshot() scheduler = %q, want unhealthy", snapshot.Scheduler)
	}
	if snapshot.ImportErrors != 1 {
		t.Fatalf("takeSnapshot() import errors = %d, want 1", snapshot.ImportErrors)
	}
	if snapshot.DAGRunStates["example::run-1"] != "failed" {
		t.Fatalf("takeSnapshot() dag runs = %#v", snapshot.DAGRunStates)
	}
	if snapshot.PoolOpen["default"] != 0 {
		t.Fatalf("takeSnapshot() pool open = %#v", snapshot.PoolOpen)
	}

	prevRuns := map[string]string{"airflow.example::example::run-1": "success"}
	prevPools := map[string]int{"airflow.example::default": 1}
	prevScheduler := map[string]string{"airflow.example": "healthy"}
	var buf bytes.Buffer

	pollAndEmit(context.Background(), cfg, json.NewEncoder(&buf), prevRuns, prevPools, prevScheduler)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("pollAndEmit() lines = %d, want 3: %q", len(lines), buf.String())
	}

	var events []StreamEvent
	for _, line := range lines {
		var event StreamEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("json.Unmarshal(%q) error = %v", line, err)
		}
		events = append(events, event)
	}
	if events[0].EventType != "scheduler_state_change" || events[0].Severity != "critical" {
		t.Fatalf("scheduler event = %#v", events[0])
	}
	if events[1].EventType != "dag_run_state_change" || events[1].Severity != "warn" {
		t.Fatalf("dag event = %#v", events[1])
	}
	if events[2].EventType != "pool_saturation" || events[2].Severity != "critical" {
		t.Fatalf("pool event = %#v", events[2])
	}
}

func TestComputeDiffAndPrintDiffText(t *testing.T) {
	prev := DiffSnapshot{
		Timestamp:    time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC),
		DAGRunStates: map[string]string{"example::run-1": "success", "example::run-2": "failed"},
		PoolOpen:     map[string]int{"default": 2},
		ImportErrors: 1,
		Scheduler:    "healthy",
	}
	current := DiffSnapshot{
		Timestamp:    prev.Timestamp.Add(time.Minute),
		DAGRunStates: map[string]string{"example::run-1": "failed", "example::run-2": "success", "example::run-3": "failed"},
		PoolOpen:     map[string]int{"default": 0},
		ImportErrors: 3,
		Scheduler:    "unhealthy",
	}

	result := computeDiff(prev, current)
	if !result.HasChanges || len(result.NewFailures) != 2 || len(result.Recoveries) != 1 {
		t.Fatalf("computeDiff() = %#v", result)
	}

	cmd, out, _ := newTestCommand(t, func(cmd *cobra.Command) {})
	if err := printDiffText(cmd, result); err != nil {
		t.Fatalf("printDiffText() error = %v", err)
	}
	output := out.String()
	if !strings.Contains(output, "New failures") || !strings.Contains(output, "Scheduler: healthy -> unhealthy") {
		t.Fatalf("diff text output = %q", output)
	}
}

func TestPrintDiffTextNoChanges(t *testing.T) {
	cmd, out, _ := newTestCommand(t, func(cmd *cobra.Command) {})
	result := DiffResult{HasChanges: false, Since: "03:04:05"}
	if err := printDiffText(cmd, result); err != nil {
		t.Fatalf("printDiffText() error = %v", err)
	}
	if strings.TrimSpace(out.String()) != "No changes since 03:04:05" {
		t.Fatalf("printDiffText() output = %q", out.String())
	}
}

func TestInitVersionRootAndConfigErrors(t *testing.T) {
	cmd, out, _ := newTestCommand(t, func(cmd *cobra.Command) {})
	if err := initCmd.RunE(cmd, nil); err != nil {
		t.Fatalf("initCmd.RunE() error = %v", err)
	}
	if !strings.Contains(out.String(), "AIRFLOW_API_URL=http://localhost:8080/api/v1") {
		t.Fatalf("init output = %q", out.String())
	}

	oldVersion := appVersion
	SetVersion("9.9.9")
	defer SetVersion(oldVersion)

	versionCmd.SetOut(out)
	out.Reset()
	versionCmd.Run(versionCmd, nil)
	if strings.TrimSpace(out.String()) != "airflowpulse 9.9.9" {
		t.Fatalf("version output = %q", out.String())
	}

	rootCmd.SetOut(out)
	rootCmd.SetErr(out)
	rootCmd.SetArgs([]string{"version"})
	out.Reset()
	if err := Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if strings.TrimSpace(out.String()) != "airflowpulse 9.9.9" {
		t.Fatalf("Execute() output = %q", out.String())
	}

	t.Setenv("AIRFLOW_API_URL", "")
	if err := pulseCmd.RunE(pulseCmd, nil); err == nil {
		t.Fatal("pulseCmd.RunE() error = nil, want config error")
	}
	if err := serveCmd.RunE(serveCmd, nil); err == nil {
		t.Fatal("serveCmd.RunE() error = nil, want config error")
	}
}

func TestRunWithWatchWithoutWatch(t *testing.T) {
	called := 0
	wrapped := runWithWatch(func(cmd *cobra.Command, args []string) error {
		called++
		return nil
	})

	cmd, _, _ := newTestCommand(t, func(cmd *cobra.Command) {
		cmd.Flags().Bool("watch", false, "")
		cmd.Flags().Duration("interval", 0, "")
	})

	if err := wrapped(cmd, nil); err != nil {
		t.Fatalf("runWithWatch() error = %v", err)
	}
	if called != 1 {
		t.Fatalf("wrapped function calls = %d, want 1", called)
	}
}

func TestRunStreamConfigError(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "")

	cmd, _, _ := newTestCommand(t, func(cmd *cobra.Command) {})
	if err := runStream(cmd, nil); err == nil {
		t.Fatal("runStream() error = nil, want config error")
	}
}
