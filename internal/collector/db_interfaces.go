package collector

import "context"

type slaMissStore interface {
	SLAMissCount(ctx context.Context) (int64, error)
}

type xcomStore interface {
	XComStats(ctx context.Context) (rows int64, bytes int64, err error)
}

type executorStore interface {
	ExecutorSlots(ctx context.Context) (map[string]int64, error)
}
