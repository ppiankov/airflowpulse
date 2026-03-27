package collector

import (
	"context"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/metrics"
)

// DAGInventory collects DAG count and paused count from GET /dags.
type DAGInventory struct{}

func (d *DAGInventory) Name() string { return "daginventory" }

func (d *DAGInventory) Collect(ctx context.Context, client *airflow.Client, instance string) error {
	dags, err := client.ListDAGs(ctx, 1000)
	if err != nil {
		return err
	}

	total := 0
	paused := 0
	for _, dag := range dags.DAGs {
		total++
		if dag.IsPaused {
			paused++
		}
	}

	metrics.DAGCount.WithLabelValues(instance).Set(float64(total))
	metrics.DAGPausedCount.WithLabelValues(instance).Set(float64(paused))
	return nil
}
