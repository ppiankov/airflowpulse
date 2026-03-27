package collector

import (
	"context"
	"time"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/metrics"
)

// Scheduler collects scheduler and metadatabase health from GET /health.
type Scheduler struct{}

func (s *Scheduler) Name() string { return "scheduler" }

func (s *Scheduler) Collect(ctx context.Context, client *airflow.Client, instance string) error {
	h, err := client.Health(ctx)
	if err != nil {
		return err
	}

	if h.Scheduler.Status == "healthy" {
		metrics.SchedulerHealthy.WithLabelValues(instance).Set(1)
	} else {
		metrics.SchedulerHealthy.WithLabelValues(instance).Set(0)
	}

	if h.Scheduler.LatestHeartbeat != "" {
		t, perr := time.Parse(time.RFC3339, h.Scheduler.LatestHeartbeat)
		if perr == nil {
			age := time.Since(t).Seconds()
			metrics.SchedulerHeartbeatAge.WithLabelValues(instance).Set(age)
		}
	}

	if h.Metadatabase.Status == "healthy" {
		metrics.MetadatabaseHealthy.WithLabelValues(instance).Set(1)
	} else {
		metrics.MetadatabaseHealthy.WithLabelValues(instance).Set(0)
	}

	return nil
}
