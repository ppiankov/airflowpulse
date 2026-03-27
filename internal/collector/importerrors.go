package collector

import (
	"context"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/metrics"
)

// ImportErrors collects import error count from GET /importErrors.
type ImportErrors struct{}

func (i *ImportErrors) Name() string { return "importerrors" }

func (i *ImportErrors) Collect(ctx context.Context, client *airflow.Client, instance string) error {
	errs, err := client.ListImportErrors(ctx)
	if err != nil {
		return err
	}

	metrics.ImportErrorsCount.WithLabelValues(instance).Set(float64(errs.TotalEntries))
	return nil
}
