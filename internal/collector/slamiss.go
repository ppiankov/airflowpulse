package collector

import (
	"context"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/metrics"
)

// SLAMiss collects SLA miss count from the metadata database.
type SLAMiss struct {
	DB slaMissStore
}

func (s *SLAMiss) Name() string { return "slamiss" }

func (s *SLAMiss) Collect(_ context.Context, _ *airflow.Client, instance string) error {
	ctx := context.Background()
	count, err := s.DB.SLAMissCount(ctx)
	if err != nil {
		return err
	}
	metrics.SLAMissesTotal.WithLabelValues(instance).Set(float64(count))
	return nil
}
