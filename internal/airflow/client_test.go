package airflow

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClientHealth(t *testing.T) {
	client := newTransportClient("http://airflow.example", "user", "pass", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Fatalf("path = %q, want /health", r.URL.Path)
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "user" || pass != "pass" {
			t.Fatalf("basic auth = %q/%q/%v, want user/pass/true", user, pass, ok)
		}
		_, _ = fmt.Fprint(w, `{
			"metadatabase":{"status":"healthy"},
			"scheduler":{"status":"healthy","latest_heartbeat":"2024-01-02T03:04:05Z"}
		}`)
	}))

	resp, err := client.Health(context.Background())
	if err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if client.BaseURL() != "http://airflow.example" {
		t.Fatalf("BaseURL() = %q, want %q", client.BaseURL(), "http://airflow.example")
	}
	if resp.Scheduler.Status != "healthy" {
		t.Fatalf("Health() scheduler status = %q, want healthy", resp.Scheduler.Status)
	}
	if resp.Metadatabase.Status != "healthy" {
		t.Fatalf("Health() metadatabase status = %q, want healthy", resp.Metadatabase.Status)
	}
}

func TestClientListMethods(t *testing.T) {
	tests := []struct {
		name       string
		requestURI string
		body       string
		call       func(*Client) (any, error)
		assert     func(t *testing.T, got any)
	}{
		{
			name:       "ListDAGs",
			requestURI: "/dags?limit=25",
			body:       `{"dags":[{"dag_id":"etl","is_paused":true,"is_active":true,"tags":[{"name":"team"}]}],"total_entries":1}`,
			call: func(client *Client) (any, error) {
				return client.ListDAGs(context.Background(), 25)
			},
			assert: func(t *testing.T, got any) {
				t.Helper()
				resp := got.(*DAGList)
				if resp.TotalEntries != 1 || len(resp.DAGs) != 1 || resp.DAGs[0].DagID != "etl" {
					t.Fatalf("ListDAGs() = %#v", resp)
				}
			},
		},
		{
			name:       "ListDAGRuns",
			requestURI: "/dags/dag%20with%2Fslash/dagRuns?limit=25&order_by=-start_date&state=failed&state=running",
			body:       `{"dag_runs":[{"dag_id":"dag with/slash","dag_run_id":"manual__1","state":"failed"}],"total_entries":1}`,
			call: func(client *Client) (any, error) {
				return client.ListDAGRuns(context.Background(), "dag with/slash", 25, "failed", "running")
			},
			assert: func(t *testing.T, got any) {
				t.Helper()
				resp := got.(*DAGRunList)
				if resp.TotalEntries != 1 || len(resp.DAGRuns) != 1 || resp.DAGRuns[0].DagRunID != "manual__1" {
					t.Fatalf("ListDAGRuns() = %#v", resp)
				}
			},
		},
		{
			name:       "ListTaskInstances",
			requestURI: "/dags/example/dagRuns/manual__2024-01-02T03:04:05Z/taskInstances",
			body:       `{"task_instances":[{"task_id":"extract","dag_id":"example","dag_run_id":"manual__2024-01-02T03:04:05Z","state":"running","pool":"default","operator":"python"}],"total_entries":1}`,
			call: func(client *Client) (any, error) {
				return client.ListTaskInstances(context.Background(), "example", "manual__2024-01-02T03:04:05Z")
			},
			assert: func(t *testing.T, got any) {
				t.Helper()
				resp := got.(*TaskInstanceList)
				if resp.TotalEntries != 1 || len(resp.TaskInstances) != 1 || resp.TaskInstances[0].TaskID != "extract" {
					t.Fatalf("ListTaskInstances() = %#v", resp)
				}
			},
		},
		{
			name:       "ListPools",
			requestURI: "/pools",
			body:       `{"pools":[{"name":"default","slots":16,"running_slots":4,"queued_slots":2,"open_slots":10}],"total_entries":1}`,
			call: func(client *Client) (any, error) {
				return client.ListPools(context.Background())
			},
			assert: func(t *testing.T, got any) {
				t.Helper()
				resp := got.(*PoolList)
				if resp.TotalEntries != 1 || len(resp.Pools) != 1 || resp.Pools[0].Name != "default" {
					t.Fatalf("ListPools() = %#v", resp)
				}
			},
		},
		{
			name:       "ListImportErrors",
			requestURI: "/importErrors",
			body:       `{"import_errors":[{"import_error_id":7,"filename":"dag.py"}],"total_entries":1}`,
			call: func(client *Client) (any, error) {
				return client.ListImportErrors(context.Background())
			},
			assert: func(t *testing.T, got any) {
				t.Helper()
				resp := got.(*ImportErrorList)
				if resp.TotalEntries != 1 || len(resp.ImportErrors) != 1 || resp.ImportErrors[0].ImportErrorID != 7 {
					t.Fatalf("ListImportErrors() = %#v", resp)
				}
			},
		},
		{
			name:       "ListDatasetEvents",
			requestURI: "/datasets/events?limit=10&order_by=-timestamp",
			body:       `{"dataset_events":[{"dataset_uri":"s3://bucket/key","source_dag_id":"producer","source_task_id":"task","source_run_id":"run"}],"total_entries":1}`,
			call: func(client *Client) (any, error) {
				return client.ListDatasetEvents(context.Background(), 10)
			},
			assert: func(t *testing.T, got any) {
				t.Helper()
				resp := got.(*DatasetEventList)
				if resp.TotalEntries != 1 || len(resp.DatasetEvents) != 1 || resp.DatasetEvents[0].DatasetURI != "s3://bucket/key" {
					t.Fatalf("ListDatasetEvents() = %#v", resp)
				}
			},
		},
		{
			name:       "GetDAGTasks",
			requestURI: "/dags/example/tasks",
			body:       `{"tasks":[{"task_id":"extract","downstream_task_ids":["transform"]}],"total_entries":1}`,
			call: func(client *Client) (any, error) {
				return client.GetDAGTasks(context.Background(), "example")
			},
			assert: func(t *testing.T, got any) {
				t.Helper()
				resp := got.(*TaskDetailList)
				if resp.TotalEntries != 1 || len(resp.Tasks) != 1 || resp.Tasks[0].TaskID != "extract" {
					t.Fatalf("GetDAGTasks() = %#v", resp)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTransportClient("http://airflow.example", "", "", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.RequestURI(); got != tt.requestURI {
					t.Fatalf("RequestURI = %q, want %q", got, tt.requestURI)
				}
				_, _ = fmt.Fprint(w, tt.body)
			}))
			got, err := tt.call(client)
			if err != nil {
				t.Fatalf("%s error = %v", tt.name, err)
			}
			tt.assert(t, got)
		})
	}
}

func TestClientErrorResponse(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
	}{
		{name: "not found", statusCode: http.StatusNotFound, body: "missing"},
		{name: "server error", statusCode: http.StatusInternalServerError, body: "boom"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTransportClient("http://airflow.example", "", "", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				_, _ = fmt.Fprint(w, tt.body)
			}))
			_, err := client.ListPools(context.Background())
			if err == nil {
				t.Fatal("ListPools() error = nil, want error")
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("returned %d", tt.statusCode)) {
				t.Fatalf("ListPools() error = %q, want status code", err)
			}
			if !strings.Contains(err.Error(), tt.body) {
				t.Fatalf("ListPools() error = %q, want body %q", err, tt.body)
			}
		})
	}
}

func TestClientDecodeError(t *testing.T) {
	client := newTransportClient("http://airflow.example", "", "", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"broken"`)
	}))
	_, err := client.ListDAGs(context.Background(), 1)
	if err == nil {
		t.Fatal("ListDAGs() error = nil, want decode error")
	}
}

func newTransportClient(baseURL, user, password string, handler http.Handler) *Client {
	client := New(baseURL, user, password)
	client.http.Transport = roundTripHandler{handler: handler}
	return client
}

type roundTripHandler struct {
	handler http.Handler
}

func (r roundTripHandler) RoundTrip(req *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	r.handler.ServeHTTP(recorder, req)
	return recorder.Result(), nil
}
