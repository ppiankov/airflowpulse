package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/ppiankov/airflowpulse/internal/airflow"
)

func TestRunDoctorText(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "http://airflow.example")

	restoreTransport := withDefaultTransportHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.RequestURI() {
		case "/health":
			_, _ = fmt.Fprintf(w, `{
				"metadatabase":{"status":"healthy"},
				"scheduler":{"status":"healthy","latest_heartbeat":%q}
			}`, time.Now().Format(time.RFC3339))
		case "/pools":
			_, _ = fmt.Fprint(w, `{"pools":[],"total_entries":0}`)
		case "/dags?limit=1":
			_, _ = fmt.Fprint(w, `{"dags":[{"dag_id":"example"}],"total_entries":1}`)
		case "/importErrors":
			_, _ = fmt.Fprint(w, `{"import_errors":[],"total_entries":0}`)
		default:
			t.Fatalf("unexpected request %q", r.URL.RequestURI())
		}
	}))
	defer restoreTransport()

	oldVersion := appVersion
	SetVersion("1.2.3")
	defer SetVersion(oldVersion)

	cmd, out, _ := newTestCommand(t, func(cmd *cobra.Command) {
		cmd.Flags().String("format", "text", "")
	})

	if err := runDoctor(cmd, nil); err != nil {
		t.Fatalf("runDoctor() error = %v", err)
	}

	output := out.String()
	if !strings.Contains(output, "airflowpulse doctor (v1.2.3)") {
		t.Fatalf("doctor output = %q", output)
	}
	if !strings.Contains(output, "[+] api_connectivity") {
		t.Fatalf("doctor output = %q", output)
	}
	if !strings.Contains(output, "Overall: pass") {
		t.Fatalf("doctor output = %q", output)
	}
}

func TestRunInstanceChecksConnectivityFailure(t *testing.T) {
	origTransport := http.DefaultTransport
	http.DefaultTransport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("dial failed")
	})
	defer func() { http.DefaultTransport = origTransport }()

	client := airflow.New("http://airflow.example", "", "")
	checks := runInstanceChecks(context.Background(), client, "airflow.example")
	if len(checks) != 1 {
		t.Fatalf("runInstanceChecks() len = %d, want 1", len(checks))
	}
	if checks[0].Name != "api_connectivity" || checks[0].Status != "fail" {
		t.Fatalf("runInstanceChecks() = %#v", checks)
	}
}

func TestRunDoctorJSONWarnsOnStaleHeartbeat(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "http://airflow.example")

	restoreTransport := withDefaultTransportHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.RequestURI() {
		case "/health":
			_, _ = fmt.Fprintf(w, `{
				"metadatabase":{"status":"healthy"},
				"scheduler":{"status":"healthy","latest_heartbeat":%q}
			}`, time.Now().Add(-2*time.Minute).Format(time.RFC3339))
		case "/pools":
			_, _ = fmt.Fprint(w, `{"pools":[],"total_entries":0}`)
		case "/dags?limit=1":
			_, _ = fmt.Fprint(w, `{"dags":[{"dag_id":"example"}],"total_entries":1}`)
		case "/importErrors":
			_, _ = fmt.Fprint(w, `{"import_errors":[],"total_entries":0}`)
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

	if err := runDoctor(cmd, nil); err != nil {
		t.Fatalf("runDoctor() error = %v", err)
	}

	var result DoctorResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if result.Status != "warn" {
		t.Fatalf("doctor status = %q, want warn", result.Status)
	}

	foundWarn := false
	for _, check := range result.Checks {
		if check.Name == "scheduler" && check.Status == "warn" {
			foundWarn = true
		}
	}
	if !foundWarn {
		t.Fatalf("doctor checks = %#v, want scheduler warn", result.Checks)
	}
}
