package collector

import (
	"context"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/metrics"
)

// DatasetEvents collects dataset event count from GET /datasets/events (Airflow 2.4+).
type DatasetEvents struct{}

func (d *DatasetEvents) Name() string { return "datasetevents" }

func (d *DatasetEvents) Collect(ctx context.Context, client *airflow.Client, instance string) error {
	events, err := client.ListDatasetEvents(ctx, 100)
	if err != nil {
		// Dataset events endpoint may not exist on older Airflow — degrade gracefully.
		return nil
	}

	metrics.DatasetEventsTotal.WithLabelValues(instance).Set(float64(events.TotalEntries))
	return nil
}
