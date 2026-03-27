package collector

import (
	"context"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/metrics"
)

// DAGRuns collects DAG run state counts from GET /dags/~/dagRuns.
type DAGRuns struct{}

func (d *DAGRuns) Name() string { return "dagruns" }

func (d *DAGRuns) Collect(ctx context.Context, client *airflow.Client, instance string) error {
	runs, err := client.ListDAGRuns(ctx, "~", 200)
	if err != nil {
		return err
	}

	// Reset to avoid stale series from disappeared DAGs.
	metrics.DAGRunsByState.Reset()
	metrics.DAGRunsTotal.Reset()

	perDAG := make(map[string]map[string]int)
	totals := make(map[string]int)

	for _, r := range runs.DAGRuns {
		if perDAG[r.DagID] == nil {
			perDAG[r.DagID] = make(map[string]int)
		}
		perDAG[r.DagID][r.State]++
		totals[r.State]++
	}

	for dagID, states := range perDAG {
		for state, count := range states {
			metrics.DAGRunsByState.WithLabelValues(instance, dagID, state).Set(float64(count))
		}
	}
	for state, count := range totals {
		metrics.DAGRunsTotal.WithLabelValues(instance, state).Set(float64(count))
	}

	return nil
}
