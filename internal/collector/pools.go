package collector

import (
	"context"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/metrics"
)

// Pools collects pool utilization from GET /pools.
type Pools struct{}

func (p *Pools) Name() string { return "pools" }

func (p *Pools) Collect(ctx context.Context, client *airflow.Client, instance string) error {
	pools, err := client.ListPools(ctx)
	if err != nil {
		return err
	}

	for _, pool := range pools.Pools {
		metrics.PoolOpenSlots.WithLabelValues(instance, pool.Name).Set(float64(pool.OpenSlots))
		metrics.PoolUsedSlots.WithLabelValues(instance, pool.Name).Set(float64(pool.RunningSlots))
		metrics.PoolQueuedSlots.WithLabelValues(instance, pool.Name).Set(float64(pool.QueuedSlots))
		metrics.PoolTotalSlots.WithLabelValues(instance, pool.Name).Set(float64(pool.Slots))
	}

	return nil
}
