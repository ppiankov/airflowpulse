package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/ppiankov/airflowpulse/internal/airflow"
)

func TestRunWhyText(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "http://airflow.example")

	restoreTransport := withDefaultTransportHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.RequestURI() {
		case "/dags/example/dagRuns?limit=1&order_by=-start_date":
			_, _ = fmt.Fprint(w, `{"dag_runs":[{"dag_id":"example","dag_run_id":"run-1","state":"queued"}],"total_entries":1}`)
		case "/dags/example/dagRuns/run-1/taskInstances":
			_, _ = fmt.Fprint(w, `{
				"task_instances":[
					{"task_id":"extract","dag_id":"example","dag_run_id":"run-1","state":"queued","pool":"default","try_number":1}
				],
				"total_entries":1
			}`)
		case "/health":
			_, _ = fmt.Fprintf(w, `{
				"metadatabase":{"status":"healthy"},
				"scheduler":{"status":"healthy","latest_heartbeat":%q}
			}`, time.Now().Format(time.RFC3339))
		case "/pools":
			_, _ = fmt.Fprint(w, `{
				"pools":[{"name":"default","slots":2,"running_slots":2,"queued_slots":3,"open_slots":0}],
				"total_entries":1
			}`)
		default:
			t.Fatalf("unexpected request %q", r.URL.RequestURI())
		}
	}))
	defer restoreTransport()

	cmd, out, _ := newTestCommand(t, func(cmd *cobra.Command) {
		cmd.Flags().String("format", "text", "")
		cmd.Flags().String("run-id", "", "")
	})

	if err := runWhy(cmd, []string{"example", "extract"}); err != nil {
		t.Fatalf("runWhy() error = %v", err)
	}

	output := out.String()
	if !strings.Contains(output, "Verdict: problem found") {
		t.Fatalf("why output = %q", output)
	}
	if !strings.Contains(output, `pool "default" has 0 open slots`) {
		t.Fatalf("why output = %q", output)
	}
}

func TestRunWhyJSONWithRunID(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "http://airflow.example")

	restoreTransport := withDefaultTransportHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.RequestURI() {
		case "/dags/example/dagRuns?limit=50&order_by=-start_date":
			_, _ = fmt.Fprint(w, `{
				"dag_runs":[
					{"dag_id":"example","dag_run_id":"run-new","state":"success"},
					{"dag_id":"example","dag_run_id":"run-old","state":"failed"}
				],
				"total_entries":2
			}`)
		case "/dags/example/dagRuns/run-old/taskInstances":
			_, _ = fmt.Fprint(w, `{
				"task_instances":[
					{"task_id":"extract","dag_id":"example","dag_run_id":"run-old","state":"up_for_retry","try_number":3}
				],
				"total_entries":1
			}`)
		default:
			t.Fatalf("unexpected request %q", r.URL.RequestURI())
		}
	}))
	defer restoreTransport()

	cmd, out, _ := newTestCommand(t, func(cmd *cobra.Command) {
		cmd.Flags().String("format", "text", "")
		cmd.Flags().String("run-id", "", "")
		if err := cmd.Flags().Set("format", "json"); err != nil {
			t.Fatalf("set format flag: %v", err)
		}
		if err := cmd.Flags().Set("run-id", "run-old"); err != nil {
			t.Fatalf("set run-id flag: %v", err)
		}
	})

	if err := runWhy(cmd, []string{"example", "extract"}); err != nil {
		t.Fatalf("runWhy() error = %v", err)
	}

	var result WhyResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if result.RunID != "run-old" {
		t.Fatalf("run id = %q, want run-old", result.RunID)
	}
	if result.Verdict != "warning" {
		t.Fatalf("verdict = %q, want warning", result.Verdict)
	}
	if len(result.Chain) != 1 || result.Chain[0].Check != "retry_pending" {
		t.Fatalf("chain = %#v, want retry_pending", result.Chain)
	}
}

func TestInvestigateHelpers(t *testing.T) {
	duration := 5400.0
	running := investigateRunning(context.Background(), nil, "airflow.example", &airflow.TaskInstance{
		TaskID:   "task",
		Duration: &duration,
	})
	if len(running) != 2 || running[1].Check != "long_running" || running[1].Status != "warn" {
		t.Fatalf("investigateRunning() = %#v", running)
	}

	failed := investigateFailed(&airflow.TaskInstance{TryNumber: 4})
	if len(failed) != 1 || failed[0].Status != "fail" {
		t.Fatalf("investigateFailed() = %#v", failed)
	}

	failedState := "failed"
	successState := "success"
	upstream := investigateUpstreamFailed(context.Background(), nil, "example", "run-1", []airflow.TaskInstance{
		{TaskID: "upstream-1", State: &failedState},
		{TaskID: "upstream-2", State: &successState},
	})
	if len(upstream) != 2 || !strings.Contains(upstream[1].Detail, "upstream-1") {
		t.Fatalf("investigateUpstreamFailed() = %#v", upstream)
	}
}

func TestInvestigateTaskMissingTask(t *testing.T) {
	origTransport := http.DefaultTransport
	http.DefaultTransport = roundTripHandler{handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.RequestURI() {
		case "/dags/example/dagRuns?limit=1&order_by=-start_date":
			_, _ = fmt.Fprint(w, `{"dag_runs":[{"dag_id":"example","dag_run_id":"run-1","state":"running"}],"total_entries":1}`)
		case "/dags/example/dagRuns/run-1/taskInstances":
			_, _ = fmt.Fprint(w, `{"task_instances":[{"task_id":"other","dag_id":"example","dag_run_id":"run-1","state":"success"}],"total_entries":1}`)
		default:
			t.Fatalf("unexpected request %q", r.URL.RequestURI())
		}
	})}
	defer func() { http.DefaultTransport = origTransport }()

	client := airflow.New("http://airflow.example", "", "")
	result, found := investigateTask(context.Background(), client, "airflow.example", "example", "missing", "")
	if !found {
		t.Fatal("investigateTask() found = false, want true")
	}
	if result.Verdict != "task not found in this DAG run" {
		t.Fatalf("investigateTask() result = %#v", result)
	}
}
