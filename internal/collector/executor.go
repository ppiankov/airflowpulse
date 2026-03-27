package collector

import (
	"context"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/metrics"
)

// Executor collects executor slot utilization from the metadata database.
type Executor struct {
	DB executorStore
}

func (e *Executor) Name() string { return "executor" }

func (e *Executor) Collect(_ context.Context, _ *airflow.Client, instance string) error {
	ctx := context.Background()
	slots, err := e.DB.ExecutorSlots(ctx)
	if err != nil {
		return err
	}
	for state, count := range slots {
		metrics.ExecutorSlots.WithLabelValues(instance, state).Set(float64(count))
	}
	return nil
}
