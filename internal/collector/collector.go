package collector

import (
	"github.com/ppiankov/airflowpulse/internal/airflow"
)

// Collector defines the interface for all metric collectors.
type Collector interface {
	// Name returns the collector name for logging.
	Name() string

	// Collect gathers metrics from an Airflow instance.
	// instance is the host label identifying which Airflow is being polled.
	Collect(client *airflow.Client, instance string) error
}
