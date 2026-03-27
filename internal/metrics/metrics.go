package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// Scrape health — per instance
	Up = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_up",
		Help: "1 if Airflow API is reachable, 0 otherwise",
	}, []string{"instance"})
	ScrapeDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_scrape_duration_seconds",
		Help: "Time taken to collect metrics",
	}, []string{"instance"})
	ScrapeErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "airflow_scrape_errors_total",
		Help: "Cumulative scrape error count",
	}, []string{"instance"})

	// Scheduler health (WO-7)
	SchedulerHealthy = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_scheduler_healthy",
		Help: "1 if scheduler reports healthy status, 0 otherwise",
	}, []string{"instance"})
	SchedulerHeartbeatAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_scheduler_heartbeat_seconds_ago",
		Help: "Seconds since the last scheduler heartbeat",
	}, []string{"instance"})
	MetadatabaseHealthy = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_metadatabase_healthy",
		Help: "1 if metadatabase reports healthy status, 0 otherwise",
	}, []string{"instance"})

	// DAG runs (WO-8)
	DAGRunsByState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_dag_runs",
		Help: "Number of recent DAG runs by state per DAG",
	}, []string{"instance", "dag_id", "state"})
	DAGRunsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_dag_runs_total",
		Help: "Total recent DAG runs by state across all DAGs",
	}, []string{"instance", "state"})

	// Task instances (WO-9)
	TaskInstancesByState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_task_instances",
		Help: "Number of task instances by state in active runs",
	}, []string{"instance", "dag_id", "state"})
	TaskInstancesTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_task_instances_total",
		Help: "Total task instances by state across all active runs",
	}, []string{"instance", "state"})

	// Pool utilization (WO-10)
	PoolOpenSlots = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_pool_open_slots",
		Help: "Number of open (free) slots in a pool",
	}, []string{"instance", "pool"})
	PoolUsedSlots = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_pool_used_slots",
		Help: "Number of running slots in a pool",
	}, []string{"instance", "pool"})
	PoolQueuedSlots = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_pool_queued_slots",
		Help: "Number of queued slots in a pool",
	}, []string{"instance", "pool"})
	PoolTotalSlots = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_pool_total_slots",
		Help: "Total configured slots for a pool",
	}, []string{"instance", "pool"})

	// Import errors (WO-11)
	ImportErrorsCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_import_errors",
		Help: "Number of DAG import errors",
	}, []string{"instance"})
)

func init() {
	prometheus.MustRegister(
		Up, ScrapeDuration, ScrapeErrors,
		SchedulerHealthy, SchedulerHeartbeatAge, MetadatabaseHealthy,
		DAGRunsByState, DAGRunsTotal,
		TaskInstancesByState, TaskInstancesTotal,
		PoolOpenSlots, PoolUsedSlots, PoolQueuedSlots, PoolTotalSlots,
		ImportErrorsCount,
	)
}
