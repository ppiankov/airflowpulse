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

	// DAG inventory (WO-12)
	DAGCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_dags_total",
		Help: "Total number of DAGs",
	}, []string{"instance"})
	DAGPausedCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_dags_paused",
		Help: "Number of paused DAGs",
	}, []string{"instance"})

	// Dataset events (WO-16)
	DatasetEventsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_dataset_events_total",
		Help: "Total number of recent dataset events",
	}, []string{"instance"})

	// Zombie tasks (WO-32)
	ZombieTasks = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_zombie_tasks",
		Help: "Number of suspected zombie tasks (running with stale heartbeat)",
	}, []string{"instance"})

	// SLA misses (WO-13)
	SLAMissesTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_sla_misses_total",
		Help: "Total SLA misses from metadata database",
	}, []string{"instance"})

	// XCom bloat (WO-14)
	XComRowCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_xcom_rows",
		Help: "Number of rows in the xcom table",
	}, []string{"instance"})
	XComTotalBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_xcom_bytes",
		Help: "Estimated total size of xcom values in bytes",
	}, []string{"instance"})

	// Executor (WO-15)
	ExecutorSlots = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "airflow_executor_slots",
		Help: "Executor slot count by state",
	}, []string{"instance", "state"})
)

func init() {
	prometheus.MustRegister(
		Up, ScrapeDuration, ScrapeErrors,
		SchedulerHealthy, SchedulerHeartbeatAge, MetadatabaseHealthy,
		DAGRunsByState, DAGRunsTotal,
		TaskInstancesByState, TaskInstancesTotal,
		PoolOpenSlots, PoolUsedSlots, PoolQueuedSlots, PoolTotalSlots,
		ImportErrorsCount,
		DAGCount, DAGPausedCount,
		DatasetEventsTotal,
		ZombieTasks,
		SLAMissesTotal,
		XComRowCount, XComTotalBytes,
		ExecutorSlots,
	)
}
