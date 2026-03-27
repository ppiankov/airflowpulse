package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDoReturnsOnFirstSuccess(t *testing.T) {
	restore := stubRetryAfter(t, nil)
	defer restore()

	attempts := 0
	err := Do(context.Background(), 3, func() error {
		attempts++
		return nil
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if attempts != 1 {
		t.Fatalf("Do() attempts = %d, want 1", attempts)
	}
}

func TestDoReturnsLastErrorAfterMaxAttempts(t *testing.T) {
	var waits []time.Duration
	restore := stubRetryAfter(t, &waits)
	defer restore()

	wantErr := errors.New("still failing")
	attempts := 0

	err := Do(context.Background(), 3, func() error {
		attempts++
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Do() error = %v, want %v", err, wantErr)
	}
	if attempts != 3 {
		t.Fatalf("Do() attempts = %d, want 3", attempts)
	}

	wantWaits := []time.Duration{500 * time.Millisecond, time.Second}
	if len(waits) != len(wantWaits) {
		t.Fatalf("Do() waits len = %d, want %d", len(waits), len(wantWaits))
	}
	for i, want := range wantWaits {
		if waits[i] != want {
			t.Fatalf("Do() waits[%d] = %v, want %v", i, waits[i], want)
		}
	}
}

func TestDoReturnsContextErrorBeforeAttempt(t *testing.T) {
	restore := stubRetryAfter(t, nil)
	defer restore()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	err := Do(ctx, 3, func() error {
		called = true
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do() error = %v, want %v", err, context.Canceled)
	}
	if called {
		t.Fatal("Do() called fn after context cancellation")
	}
}

func TestDoReturnsContextErrorDuringBackoff(t *testing.T) {
	restore := stubRetryBlockingAfter(t)
	defer restore()

	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0

	err := Do(ctx, 3, func() error {
		attempts++
		cancel()
		return errors.New("retry")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do() error = %v, want %v", err, context.Canceled)
	}
	if attempts != 1 {
		t.Fatalf("Do() attempts = %d, want 1", attempts)
	}
}

func TestDoUsesCappedExponentialBackoff(t *testing.T) {
	var waits []time.Duration
	restore := stubRetryAfter(t, &waits)
	defer restore()

	wantErr := errors.New("retry")
	err := Do(context.Background(), 6, func() error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Do() error = %v, want %v", err, wantErr)
	}

	wantWaits := []time.Duration{
		500 * time.Millisecond,
		time.Second,
		2 * time.Second,
		4 * time.Second,
		5 * time.Second,
	}
	if len(waits) != len(wantWaits) {
		t.Fatalf("Do() waits len = %d, want %d", len(waits), len(wantWaits))
	}
	for i, want := range wantWaits {
		if waits[i] != want {
			t.Fatalf("Do() waits[%d] = %v, want %v", i, waits[i], want)
		}
	}
}

func stubRetryAfter(t *testing.T, waits *[]time.Duration) func() {
	t.Helper()

	origAfter := retryAfter
	origBaseDelay := retryBaseDelay
	origMaxDelay := retryMaxDelay

	retryBaseDelay = DefaultBaseDelay
	retryMaxDelay = DefaultMaxDelay
	retryAfter = func(d time.Duration) <-chan time.Time {
		if waits != nil {
			*waits = append(*waits, d)
		}
		ch := make(chan time.Time, 1)
		ch <- time.Unix(0, 0)
		return ch
	}

	return func() {
		retryAfter = origAfter
		retryBaseDelay = origBaseDelay
		retryMaxDelay = origMaxDelay
	}
}

func stubRetryBlockingAfter(t *testing.T) func() {
	t.Helper()

	origAfter := retryAfter
	origBaseDelay := retryBaseDelay
	origMaxDelay := retryMaxDelay

	retryBaseDelay = DefaultBaseDelay
	retryMaxDelay = DefaultMaxDelay
	retryAfter = func(d time.Duration) <-chan time.Time {
		return make(chan time.Time)
	}

	return func() {
		retryAfter = origAfter
		retryBaseDelay = origBaseDelay
		retryMaxDelay = origMaxDelay
	}
}
