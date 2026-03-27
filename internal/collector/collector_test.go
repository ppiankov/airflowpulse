package collector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/metrics"
)

func TestAllReturnsCollectors(t *testing.T) {
	collectors := All()
	want := []string{
		"scheduler",
		"dagruns",
		"taskinstances",
		"pools",
		"importerrors",
		"daginventory",
		"datasetevents",
	}

	if len(collectors) != len(want) {
		t.Fatalf("All() len = %d, want %d", len(collectors), len(want))
	}
	for i, name := range want {
		if collectors[i].Name() != name {
			t.Fatalf("All()[%d].Name() = %q, want %q", i, collectors[i].Name(), name)
		}
	}
}

func TestSchedulerCollect(t *testing.T) {
	resetCollectorMetrics()
	reg := mustRegistry(t, metrics.SchedulerHealthy, metrics.SchedulerHeartbeatAge, metrics.MetadatabaseHealthy)

	fixedNow := time.Date(2024, time.January, 2, 3, 4, 50, 0, time.UTC)
	origNow := collectorNow
	collectorNow = func() time.Time { return fixedNow }
	defer func() { collectorNow = origNow }()

	client, closeServer := newCollectorClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.RequestURI(); got != "/health" {
			t.Fatalf("RequestURI = %q, want /health", got)
		}
		_, _ = fmt.Fprintf(w, `{
			"metadatabase":{"status":"unhealthy"},
			"scheduler":{"status":"healthy","latest_heartbeat":%q}
		}`, fixedNow.Add(-45*time.Second).Format(time.RFC3339))
	})
	defer closeServer()

	err := (&Scheduler{}).Collect(context.Background(), client, "airflow.example")
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	assertGaugeValue(t, reg, "airflow_scheduler_healthy", map[string]string{"instance": "airflow.example"}, 1)
	assertGaugeValue(t, reg, "airflow_scheduler_heartbeat_seconds_ago", map[string]string{"instance": "airflow.example"}, 45)
	assertGaugeValue(t, reg, "airflow_metadatabase_healthy", map[string]string{"instance": "airflow.example"}, 0)
}

func TestDAGRunsCollectResetsAndCounts(t *testing.T) {
	resetCollectorMetrics()
	reg := mustRegistry(t, metrics.DAGRunsByState, metrics.DAGRunsTotal)

	metrics.DAGRunsByState.WithLabelValues("airflow.example", "old_dag", "failed").Set(99)
	metrics.DAGRunsTotal.WithLabelValues("airflow.example", "failed").Set(99)

	client, closeServer := newCollectorClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.RequestURI(); got != "/dags/~/dagRuns?limit=200&order_by=-start_date" {
			t.Fatalf("RequestURI = %q", got)
		}
		_, _ = fmt.Fprint(w, `{
			"dag_runs":[
				{"dag_id":"etl","dag_run_id":"run-1","state":"failed"},
				{"dag_id":"etl","dag_run_id":"run-2","state":"failed"},
				{"dag_id":"etl","dag_run_id":"run-3","state":"success"},
				{"dag_id":"ml","dag_run_id":"run-4","state":"running"}
			],
			"total_entries":4
		}`)
	})
	defer closeServer()

	err := (&DAGRuns{}).Collect(context.Background(), client, "airflow.example")
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	assertGaugeValue(t, reg, "airflow_dag_runs", map[string]string{
		"instance": "airflow.example",
		"dag_id":   "etl",
		"state":    "failed",
	}, 2)
	assertGaugeValue(t, reg, "airflow_dag_runs", map[string]string{
		"instance": "airflow.example",
		"dag_id":   "etl",
		"state":    "success",
	}, 1)
	assertGaugeValue(t, reg, "airflow_dag_runs_total", map[string]string{
		"instance": "airflow.example",
		"state":    "failed",
	}, 2)
	assertGaugeValue(t, reg, "airflow_dag_runs_total", map[string]string{
		"instance": "airflow.example",
		"state":    "running",
	}, 1)

	if metricExists(t, reg, "airflow_dag_runs", map[string]string{
		"instance": "airflow.example",
		"dag_id":   "old_dag",
		"state":    "failed",
	}) {
		t.Fatal("stale DAG run series still present after Reset")
	}
}

func TestTaskInstancesCollectAndZombieThreshold(t *testing.T) {
	resetCollectorMetrics()
	reg := mustRegistry(t, metrics.TaskInstancesByState, metrics.TaskInstancesTotal, metrics.ZombieTasks)

	client, closeServer := newCollectorClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.RequestURI() {
		case "/dags/~/dagRuns?limit=100&order_by=-start_date&state=running&state=queued":
			_, _ = fmt.Fprint(w, `{
				"dag_runs":[
					{"dag_id":"etl","dag_run_id":"run-1","state":"running"},
					{"dag_id":"etl","dag_run_id":"run-2","state":"queued"}
				],
				"total_entries":2
			}`)
		case "/dags/etl/dagRuns/run-1/taskInstances":
			_, _ = fmt.Fprint(w, `{
				"task_instances":[
					{"task_id":"extract","dag_id":"etl","dag_run_id":"run-1","state":"running","duration":3601},
					{"task_id":"transform","dag_id":"etl","dag_run_id":"run-1","state":"running","duration":3600},
					{"task_id":"load","dag_id":"etl","dag_run_id":"run-1","state":"queued"}
				],
				"total_entries":3
			}`)
		case "/dags/etl/dagRuns/run-2/taskInstances":
			_, _ = fmt.Fprint(w, `{
				"task_instances":[
					{"task_id":"notify","dag_id":"etl","dag_run_id":"run-2","state":"queued"},
					{"task_id":"mark","dag_id":"etl","dag_run_id":"run-2"}
				],
				"total_entries":2
			}`)
		default:
			t.Fatalf("unexpected RequestURI %q", r.URL.RequestURI())
		}
	})
	defer closeServer()

	err := (&TaskInstances{}).Collect(context.Background(), client, "airflow.example")
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	assertGaugeValue(t, reg, "airflow_task_instances", map[string]string{
		"instance": "airflow.example",
		"dag_id":   "etl",
		"state":    "running",
	}, 2)
	assertGaugeValue(t, reg, "airflow_task_instances", map[string]string{
		"instance": "airflow.example",
		"dag_id":   "etl",
		"state":    "queued",
	}, 2)
	assertGaugeValue(t, reg, "airflow_task_instances", map[string]string{
		"instance": "airflow.example",
		"dag_id":   "etl",
		"state":    "no_status",
	}, 1)
	assertGaugeValue(t, reg, "airflow_task_instances_total", map[string]string{
		"instance": "airflow.example",
		"state":    "queued",
	}, 2)
	assertGaugeValue(t, reg, "airflow_zombie_tasks", map[string]string{
		"instance": "airflow.example",
	}, 1)
}

func TestPoolsCollect(t *testing.T) {
	resetCollectorMetrics()
	reg := mustRegistry(t, metrics.PoolOpenSlots, metrics.PoolUsedSlots, metrics.PoolQueuedSlots, metrics.PoolTotalSlots)

	client, closeServer := newCollectorClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.RequestURI(); got != "/pools" {
			t.Fatalf("RequestURI = %q, want /pools", got)
		}
		_, _ = fmt.Fprint(w, `{
			"pools":[{"name":"default","slots":16,"running_slots":5,"queued_slots":2,"open_slots":9}],
			"total_entries":1
		}`)
	})
	defer closeServer()

	err := (&Pools{}).Collect(context.Background(), client, "airflow.example")
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	labels := map[string]string{"instance": "airflow.example", "pool": "default"}
	assertGaugeValue(t, reg, "airflow_pool_open_slots", labels, 9)
	assertGaugeValue(t, reg, "airflow_pool_used_slots", labels, 5)
	assertGaugeValue(t, reg, "airflow_pool_queued_slots", labels, 2)
	assertGaugeValue(t, reg, "airflow_pool_total_slots", labels, 16)
}

func TestImportErrorsCollect(t *testing.T) {
	resetCollectorMetrics()
	reg := mustRegistry(t, metrics.ImportErrorsCount)

	client, closeServer := newCollectorClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.RequestURI(); got != "/importErrors" {
			t.Fatalf("RequestURI = %q, want /importErrors", got)
		}
		_, _ = fmt.Fprint(w, `{"import_errors":[{"import_error_id":1}],"total_entries":3}`)
	})
	defer closeServer()

	err := (&ImportErrors{}).Collect(context.Background(), client, "airflow.example")
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	assertGaugeValue(t, reg, "airflow_import_errors", map[string]string{"instance": "airflow.example"}, 3)
}

func TestDAGInventoryCollect(t *testing.T) {
	resetCollectorMetrics()
	reg := mustRegistry(t, metrics.DAGCount, metrics.DAGPausedCount)

	client, closeServer := newCollectorClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.RequestURI(); got != "/dags?limit=1000" {
			t.Fatalf("RequestURI = %q, want /dags?limit=1000", got)
		}
		_, _ = fmt.Fprint(w, `{
			"dags":[
				{"dag_id":"etl","is_paused":false},
				{"dag_id":"ml","is_paused":true},
				{"dag_id":"reports","is_paused":true}
			],
			"total_entries":3
		}`)
	})
	defer closeServer()

	err := (&DAGInventory{}).Collect(context.Background(), client, "airflow.example")
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	assertGaugeValue(t, reg, "airflow_dags_total", map[string]string{"instance": "airflow.example"}, 3)
	assertGaugeValue(t, reg, "airflow_dags_paused", map[string]string{"instance": "airflow.example"}, 2)
}

func TestDatasetEventsCollect(t *testing.T) {
	resetCollectorMetrics()
	reg := mustRegistry(t, metrics.DatasetEventsTotal)

	client, closeServer := newCollectorClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.RequestURI(); got != "/datasets/events?limit=100&order_by=-timestamp" {
			t.Fatalf("RequestURI = %q", got)
		}
		_, _ = fmt.Fprint(w, `{"dataset_events":[{"dataset_uri":"s3://bucket/key"}],"total_entries":4}`)
	})
	defer closeServer()

	err := (&DatasetEvents{}).Collect(context.Background(), client, "airflow.example")
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	assertGaugeValue(t, reg, "airflow_dataset_events_total", map[string]string{"instance": "airflow.example"}, 4)
}

func TestDatasetEventsCollectIgnoresOlderAirflowError(t *testing.T) {
	resetCollectorMetrics()
	reg := mustRegistry(t, metrics.DatasetEventsTotal)

	client, closeServer := newCollectorClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, "not found")
	})
	defer closeServer()

	err := (&DatasetEvents{}).Collect(context.Background(), client, "airflow.example")
	if err != nil {
		t.Fatalf("Collect() error = %v, want nil", err)
	}

	if metricExists(t, reg, "airflow_dataset_events_total", map[string]string{"instance": "airflow.example"}) {
		t.Fatal("dataset events metric was set on graceful error")
	}
}

func TestMetadataCollectors(t *testing.T) {
	resetCollectorMetrics()
	reg := mustRegistry(t, metrics.SLAMissesTotal, metrics.XComRowCount, metrics.XComTotalBytes, metrics.ExecutorSlots)

	err := (&SLAMiss{DB: fakeSLAMissStore{count: 7}}).Collect(context.Background(), nil, "airflow.example")
	if err != nil {
		t.Fatalf("SLAMiss.Collect() error = %v", err)
	}
	err = (&XCom{DB: fakeXComStore{rows: 5, bytes: 1024}}).Collect(context.Background(), nil, "airflow.example")
	if err != nil {
		t.Fatalf("XCom.Collect() error = %v", err)
	}
	err = (&Executor{DB: fakeExecutorStore{slots: map[string]int64{"running": 3, "queued": 2}}}).Collect(context.Background(), nil, "airflow.example")
	if err != nil {
		t.Fatalf("Executor.Collect() error = %v", err)
	}

	assertGaugeValue(t, reg, "airflow_sla_misses_total", map[string]string{"instance": "airflow.example"}, 7)
	assertGaugeValue(t, reg, "airflow_xcom_rows", map[string]string{"instance": "airflow.example"}, 5)
	assertGaugeValue(t, reg, "airflow_xcom_bytes", map[string]string{"instance": "airflow.example"}, 1024)
	assertGaugeValue(t, reg, "airflow_executor_slots", map[string]string{
		"instance": "airflow.example",
		"state":    "running",
	}, 3)
	assertGaugeValue(t, reg, "airflow_executor_slots", map[string]string{
		"instance": "airflow.example",
		"state":    "queued",
	}, 2)
}

func resetCollectorMetrics() {
	metrics.SchedulerHealthy.Reset()
	metrics.SchedulerHeartbeatAge.Reset()
	metrics.MetadatabaseHealthy.Reset()
	metrics.DAGRunsByState.Reset()
	metrics.DAGRunsTotal.Reset()
	metrics.TaskInstancesByState.Reset()
	metrics.TaskInstancesTotal.Reset()
	metrics.ZombieTasks.Reset()
	metrics.PoolOpenSlots.Reset()
	metrics.PoolUsedSlots.Reset()
	metrics.PoolQueuedSlots.Reset()
	metrics.PoolTotalSlots.Reset()
	metrics.ImportErrorsCount.Reset()
	metrics.DAGCount.Reset()
	metrics.DAGPausedCount.Reset()
	metrics.DatasetEventsTotal.Reset()
	metrics.SLAMissesTotal.Reset()
	metrics.XComRowCount.Reset()
	metrics.XComTotalBytes.Reset()
	metrics.ExecutorSlots.Reset()
}

func newCollectorClient(t *testing.T, handler http.HandlerFunc) (*airflow.Client, func()) {
	t.Helper()

	origTransport := http.DefaultTransport
	http.DefaultTransport = roundTripHandler{handler: handler}

	client := airflow.New("http://airflow.example", "", "")
	return client, func() {
		http.DefaultTransport = origTransport
	}
}

func mustRegistry(t *testing.T, collectors ...prometheus.Collector) *prometheus.Registry {
	t.Helper()

	reg := prometheus.NewRegistry()
	for _, collector := range collectors {
		if err := reg.Register(collector); err != nil {
			t.Fatalf("register collector: %v", err)
		}
	}
	return reg
}

func assertGaugeValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string, want float64) {
	t.Helper()

	got, ok := gatherGaugeValue(t, reg, name, labels)
	if !ok {
		t.Fatalf("metric %s with labels %#v not found", name, labels)
	}
	if got != want {
		t.Fatalf("metric %s labels %#v = %v, want %v", name, labels, got, want)
	}
}

func metricExists(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) bool {
	t.Helper()

	_, ok := gatherGaugeValue(t, reg, name, labels)
	return ok
}

func gatherGaugeValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
	t.Helper()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}

	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if len(metric.GetLabel()) != len(labels) {
				continue
			}
			match := true
			for _, pair := range metric.GetLabel() {
				if labels[pair.GetName()] != pair.GetValue() {
					match = false
					break
				}
			}
			if match {
				return metric.GetGauge().GetValue(), true
			}
		}
	}

	return 0, false
}

type roundTripHandler struct {
	handler http.Handler
}

func (r roundTripHandler) RoundTrip(req *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	r.handler.ServeHTTP(recorder, req)
	return recorder.Result(), nil
}

type fakeSLAMissStore struct {
	count int64
	err   error
}

func (f fakeSLAMissStore) SLAMissCount(context.Context) (int64, error) {
	return f.count, f.err
}

type fakeXComStore struct {
	rows  int64
	bytes int64
	err   error
}

func (f fakeXComStore) XComStats(context.Context) (int64, int64, error) {
	return f.rows, f.bytes, f.err
}

type fakeExecutorStore struct {
	slots map[string]int64
	err   error
}

func (f fakeExecutorStore) ExecutorSlots(context.Context) (map[string]int64, error) {
	return f.slots, f.err
}
