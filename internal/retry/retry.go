package retry

import (
	"context"
	"math"
	"time"
)

const (
	DefaultMaxAttempts = 3
	DefaultBaseDelay   = 500 * time.Millisecond
	DefaultMaxDelay    = 5 * time.Second
)

// Do executes fn up to maxAttempts times with exponential backoff.
func Do(ctx context.Context, maxAttempts int, fn func() error) error {
	var lastErr error
	for attempt := range maxAttempts {
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if attempt < maxAttempts-1 {
			backoff := time.Duration(math.Pow(2, float64(attempt))) * DefaultBaseDelay
			if backoff > DefaultMaxDelay {
				backoff = DefaultMaxDelay
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return lastErr
}
