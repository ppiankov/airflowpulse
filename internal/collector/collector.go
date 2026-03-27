package collector

import (
	"context"

	"github.com/ppiankov/airflowpulse/internal/airflow"
)

// Collector defines the interface for all metric collectors.
type Collector interface {
	// Name returns the collector name for logging.
	Name() string

	// Collect gathers metrics from an Airflow instance.
	Collect(ctx context.Context, client *airflow.Client, instance string) error
}

// All returns every registered API-based collector.
func All() []Collector {
	return []Collector{
		&Scheduler{},
		&DAGRuns{},
		&TaskInstances{},
		&Pools{},
		&ImportErrors{},
		&DAGInventory{},
		&DatasetEvents{},
	}
}
