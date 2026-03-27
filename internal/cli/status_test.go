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

func TestRunStatusText(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "http://airflow.example")

	restoreTransport := withDefaultTransportHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.RequestURI() {
		case "/health":
			_, _ = fmt.Fprintf(w, `{
				"metadatabase":{"status":"healthy"},
				"scheduler":{"status":"healthy","latest_heartbeat":%q}
			}`, time.Now().Add(-15*time.Second).Format(time.RFC3339))
		case "/dags/~/dagRuns?limit=200&order_by=-start_date&state=failed&state=running":
			_, _ = fmt.Fprint(w, `{
				"dag_runs":[
					{"dag_id":"etl_main","dag_run_id":"run-1","state":"failed"},
					{"dag_id":"etl_main","dag_run_id":"run-2","state":"running"},
					{"dag_id":"other","dag_run_id":"run-3","state":"failed"}
				],
				"total_entries":3
			}`)
		case "/pools":
			_, _ = fmt.Fprint(w, `{
				"pools":[
					{"name":"default","slots":8,"running_slots":5,"queued_slots":2,"open_slots":1},
					{"name":"other","slots":4,"running_slots":1,"queued_slots":0,"open_slots":3}
				],
				"total_entries":2
			}`)
		case "/importErrors":
			_, _ = fmt.Fprint(w, `{"import_errors":[{"import_error_id":1},{"import_error_id":2}],"total_entries":2}`)
		default:
			t.Fatalf("unexpected request %q", r.URL.RequestURI())
		}
	}))
	defer restoreTransport()

	cmd, out, _ := newTestCommand(t, func(cmd *cobra.Command) {
		cmd.Flags().String("format", "text", "")
		cmd.Flags().String("dag", "", "")
		cmd.Flags().String("state", "", "")
		cmd.Flags().String("pool", "", "")
		if err := cmd.Flags().Set("dag", "etl_*"); err != nil {
			t.Fatalf("set dag flag: %v", err)
		}
		if err := cmd.Flags().Set("state", "failed,running"); err != nil {
			t.Fatalf("set state flag: %v", err)
		}
		if err := cmd.Flags().Set("pool", "default"); err != nil {
			t.Fatalf("set pool flag: %v", err)
		}
	})

	if err := runStatus(cmd, nil); err != nil {
		t.Fatalf("runStatus() error = %v", err)
	}

	output := out.String()
	if !strings.Contains(output, "Instance: airflow.example") {
		t.Fatalf("status output = %q", output)
	}
	if !strings.Contains(output, "Scheduler:    healthy") {
		t.Fatalf("status output = %q", output)
	}
	if !strings.Contains(output, "failed=1") || !strings.Contains(output, "running=1") {
		t.Fatalf("status output = %q", output)
	}
	if !strings.Contains(output, "default") || strings.Contains(output, "other") {
		t.Fatalf("status output = %q", output)
	}
	if !strings.Contains(output, "Import Errors: 2") {
		t.Fatalf("status output = %q", output)
	}
}

func TestRunStatusJSON(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "http://airflow.example")

	restoreTransport := withDefaultTransportHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.RequestURI() {
		case "/health":
			_, _ = fmt.Fprintf(w, `{
				"metadatabase":{"status":"healthy"},
				"scheduler":{"status":"healthy","latest_heartbeat":%q}
			}`, time.Now().Format(time.RFC3339))
		case "/dags/~/dagRuns?limit=200&order_by=-start_date":
			_, _ = fmt.Fprint(w, `{"dag_runs":[{"dag_id":"etl_main","dag_run_id":"run-1","state":"success"}],"total_entries":1}`)
		case "/pools":
			_, _ = fmt.Fprint(w, `{"pools":[{"name":"default","slots":8,"running_slots":1,"queued_slots":0,"open_slots":7}],"total_entries":1}`)
		case "/importErrors":
			_, _ = fmt.Fprint(w, `{"import_errors":[],"total_entries":0}`)
		default:
			t.Fatalf("unexpected request %q", r.URL.RequestURI())
		}
	}))
	defer restoreTransport()

	cmd, out, _ := newTestCommand(t, func(cmd *cobra.Command) {
		cmd.Flags().String("format", "text", "")
		cmd.Flags().String("dag", "", "")
		cmd.Flags().String("state", "", "")
		cmd.Flags().String("pool", "", "")
		if err := cmd.Flags().Set("format", "json"); err != nil {
			t.Fatalf("set format flag: %v", err)
		}
	})

	if err := runStatus(cmd, nil); err != nil {
		t.Fatalf("runStatus() error = %v", err)
	}

	var result StatusResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(result.Instances) != 1 {
		t.Fatalf("instances len = %d, want 1", len(result.Instances))
	}
	if result.Instances[0].DAGRuns["success"] != 1 {
		t.Fatalf("dag runs = %#v, want success=1", result.Instances[0].DAGRuns)
	}
}

func TestFetchInstanceStatusAppliesFilters(t *testing.T) {
	origTransport := http.DefaultTransport
	http.DefaultTransport = roundTripHandler{handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.RequestURI() {
		case "/health":
			_, _ = fmt.Fprintf(w, `{
				"metadatabase":{"status":"healthy"},
				"scheduler":{"status":"healthy","latest_heartbeat":%q}
			}`, time.Now().Format(time.RFC3339))
		case "/dags/~/dagRuns?limit=200&order_by=-start_date&state=failed":
			_, _ = fmt.Fprint(w, `{
				"dag_runs":[
					{"dag_id":"etl_main","dag_run_id":"run-1","state":"failed"},
					{"dag_id":"skip_me","dag_run_id":"run-2","state":"failed"}
				],
				"total_entries":2
			}`)
		case "/pools":
			_, _ = fmt.Fprint(w, `{
				"pools":[
					{"name":"default","slots":8,"running_slots":2,"queued_slots":1,"open_slots":5},
					{"name":"batch","slots":4,"running_slots":1,"queued_slots":0,"open_slots":3}
				],
				"total_entries":2
			}`)
		case "/importErrors":
			_, _ = fmt.Fprint(w, `{"import_errors":[{"import_error_id":1}],"total_entries":1}`)
		default:
			t.Fatalf("unexpected request %q", r.URL.RequestURI())
		}
	})}
	defer func() { http.DefaultTransport = origTransport }()

	client := airflow.New("http://airflow.example", "", "")
	result, err := fetchInstanceStatus(context.Background(), client, "airflow.example", "etl_*", []string{"failed"}, "default")
	if err != nil {
		t.Fatalf("fetchInstanceStatus() error = %v", err)
	}
	if len(result.Pools) != 1 || result.Pools[0].Name != "default" {
		t.Fatalf("fetchInstanceStatus() pools = %#v", result.Pools)
	}
	if result.DAGRuns["failed"] != 1 {
		t.Fatalf("fetchInstanceStatus() dag runs = %#v", result.DAGRuns)
	}
	if result.ImportErrors != 1 {
		t.Fatalf("fetchInstanceStatus() import errors = %d, want 1", result.ImportErrors)
	}
}
