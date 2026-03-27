package collector

import (
	"context"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/metadb"
	"github.com/ppiankov/airflowpulse/internal/metrics"
)

// XCom collects XCom table bloat metrics from the metadata database.
type XCom struct {
	DB *metadb.Client
}

func (x *XCom) Name() string { return "xcom" }

func (x *XCom) Collect(_ context.Context, _ *airflow.Client, instance string) error {
	ctx := context.Background()
	rows, bytes, err := x.DB.XComStats(ctx)
	if err != nil {
		return err
	}
	metrics.XComRowCount.WithLabelValues(instance).Set(float64(rows))
	metrics.XComTotalBytes.WithLabelValues(instance).Set(float64(bytes))
	return nil
}
