package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/ppiankov/airflowpulse/internal/airflow"
)

func TestRunHistoryText(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "http://airflow.example")

	restoreTransport := withDefaultTransportHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.RequestURI() {
		case "/dags/example/dagRuns?limit=4&order_by=-start_date":
			_, _ = fmt.Fprint(w, `{
				"dag_runs":[
					{"dag_id":"example","dag_run_id":"run-4","start_date":"2024-01-04T00:00:00Z"},
					{"dag_id":"example","dag_run_id":"run-3","start_date":"2024-01-03T00:00:00Z"},
					{"dag_id":"example","dag_run_id":"run-2","start_date":"2024-01-02T00:00:00Z"},
					{"dag_id":"example","dag_run_id":"run-1","start_date":"2024-01-01T00:00:00Z"}
				],
				"total_entries":4
			}`)
		case "/dags/example/dagRuns/run-4/taskInstances":
			_, _ = fmt.Fprint(w, `{"task_instances":[{"task_id":"extract","state":"success","duration":120,"try_number":1}],"total_entries":1}`)
		case "/dags/example/dagRuns/run-3/taskInstances":
			_, _ = fmt.Fprint(w, `{"task_instances":[{"task_id":"extract","state":"failed","duration":110,"try_number":2}],"total_entries":1}`)
		case "/dags/example/dagRuns/run-2/taskInstances":
			_, _ = fmt.Fprint(w, `{"task_instances":[{"task_id":"extract","state":"success","duration":100,"try_number":1}],"total_entries":1}`)
		case "/dags/example/dagRuns/run-1/taskInstances":
			_, _ = fmt.Fprint(w, `{"task_instances":[{"task_id":"extract","state":"success","duration":90,"try_number":1}],"total_entries":1}`)
		default:
			t.Fatalf("unexpected request %q", r.URL.RequestURI())
		}
	}))
	defer restoreTransport()

	cmd, out, _ := newTestCommand(t, func(cmd *cobra.Command) {
		cmd.Flags().String("format", "text", "")
		cmd.Flags().Int("runs", 10, "")
		if err := cmd.Flags().Set("runs", "4"); err != nil {
			t.Fatalf("set runs flag: %v", err)
		}
	})

	if err := runHistory(cmd, []string{"example", "extract"}); err != nil {
		t.Fatalf("runHistory() error = %v", err)
	}

	output := out.String()
	if !strings.Contains(output, "History: example.extract") {
		t.Fatalf("history output = %q", output)
	}
	if !strings.Contains(output, "Mean: 105.0s") || !strings.Contains(output, "Trend: increasing") {
		t.Fatalf("history output = %q", output)
	}
	if !strings.Contains(output, "Duration:") {
		t.Fatalf("history output = %q", output)
	}
}

func TestRunHistoryJSON(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "http://airflow.example")

	restoreTransport := withDefaultTransportHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.RequestURI() {
		case "/dags/example/dagRuns?limit=2&order_by=-start_date":
			_, _ = fmt.Fprint(w, `{
				"dag_runs":[
					{"dag_id":"example","dag_run_id":"run-2","start_date":"2024-01-02T00:00:00Z"},
					{"dag_id":"example","dag_run_id":"run-1","start_date":"2024-01-01T00:00:00Z"}
				],
				"total_entries":2
			}`)
		case "/dags/example/dagRuns/run-2/taskInstances":
			_, _ = fmt.Fprint(w, `{"task_instances":[{"task_id":"extract","state":"success","duration":50,"try_number":1}],"total_entries":1}`)
		case "/dags/example/dagRuns/run-1/taskInstances":
			_, _ = fmt.Fprint(w, `{"task_instances":[{"task_id":"extract","state":"failed","duration":30,"try_number":3}],"total_entries":1}`)
		default:
			t.Fatalf("unexpected request %q", r.URL.RequestURI())
		}
	}))
	defer restoreTransport()

	cmd, out, _ := newTestCommand(t, func(cmd *cobra.Command) {
		cmd.Flags().String("format", "text", "")
		cmd.Flags().Int("runs", 10, "")
		if err := cmd.Flags().Set("format", "json"); err != nil {
			t.Fatalf("set format flag: %v", err)
		}
		if err := cmd.Flags().Set("runs", "2"); err != nil {
			t.Fatalf("set runs flag: %v", err)
		}
	})

	if err := runHistory(cmd, []string{"example", "extract"}); err != nil {
		t.Fatalf("runHistory() error = %v", err)
	}

	var result HistoryResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(result.Runs) != 2 {
		t.Fatalf("runs len = %d, want 2", len(result.Runs))
	}
	if result.Stats == nil || result.Stats.FailureRate != 0.5 {
		t.Fatalf("stats = %#v, want failure rate 0.5", result.Stats)
	}
	if result.Runs[1].Retries != 2 {
		t.Fatalf("retries = %d, want 2", result.Runs[1].Retries)
	}
}

func TestHistoryHelpers(t *testing.T) {
	values := []float64{3, 1, 2}
	sortFloat64s(values)
	if values[0] != 1 || values[1] != 2 || values[2] != 3 {
		t.Fatalf("sortFloat64s() = %#v", values)
	}

	if got := truncate("abcdef", 4); got != "a..." {
		t.Fatalf("truncate() = %q, want a...", got)
	}
	if got := truncate("abc", 4); got != "abc" {
		t.Fatalf("truncate() short = %q, want abc", got)
	}

	if got := sparkline([]float64{1, 2, 3, 4}); len(got) != 4 {
		t.Fatalf("sparkline() = %q, want 4 runes", got)
	}
	if got := sparkline(nil); got != "" {
		t.Fatalf("sparkline(nil) = %q, want empty", got)
	}

	if condStr(true, "set") != "set" || condStr(false, "set") != "" {
		t.Fatal("condStr() returned unexpected value")
	}
}

func TestFetchHistoryNoRuns(t *testing.T) {
	origTransport := http.DefaultTransport
	http.DefaultTransport = roundTripHandler{handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RequestURI() != "/dags/example/dagRuns?limit=3&order_by=-start_date" {
			t.Fatalf("unexpected request %q", r.URL.RequestURI())
		}
		_, _ = fmt.Fprint(w, `{"dag_runs":[],"total_entries":0}`)
	})}
	defer func() { http.DefaultTransport = origTransport }()

	client := airflow.New("http://airflow.example", "", "")
	result, found := fetchHistory(context.Background(), client, "airflow.example", "example", "extract", 3)
	if found {
		t.Fatalf("fetchHistory() found = true, want false with result %#v", result)
	}
}
