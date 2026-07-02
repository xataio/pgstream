// SPDX-License-Identifier: Apache-2.0

package backoff

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewExponentialBackoff_MaxIntervalCapsPerRetryInterval(t *testing.T) {
	t.Parallel()

	const maxInterval = 10 * time.Millisecond
	upperBound := maxInterval + maxInterval/2

	cfg := &ExponentialConfig{
		InitialInterval: 5 * time.Millisecond,
		MaxInterval:     maxInterval,
		MaxRetries:      8,
	}

	bo := NewExponentialBackoff(context.Background(), cfg)

	wantErr := errors.New("boom")
	intervals := make([]time.Duration, 0, cfg.MaxRetries)
	err := bo.RetryNotify(
		func() error { return wantErr },
		func(_ error, d time.Duration) { intervals = append(intervals, d) },
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}

	if len(intervals) == 0 {
		t.Fatal("expected at least one retry interval to be recorded")
	}
	for i, d := range intervals {
		if d > upperBound {
			t.Errorf("retry interval %d = %v exceeds MaxInterval bound %v", i, d, upperBound)
		}
	}
}

func TestNewExponentialBackoff_MaxElapsedTimeCapsTotal(t *testing.T) {
	t.Parallel()

	const maxElapsed = 40 * time.Millisecond
	cfg := &ExponentialConfig{
		InitialInterval: time.Millisecond,
		MaxInterval:     5 * time.Millisecond,
		MaxElapsedTime:  maxElapsed,
		MaxRetries:      0,
	}

	bo := NewExponentialBackoff(context.Background(), cfg)

	wantErr := errors.New("boom")
	start := time.Now()
	err := bo.Retry(func() error { return wantErr })
	elapsed := time.Since(start)

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("retrying took %v, expected it to stop around MaxElapsedTime %v", elapsed, maxElapsed)
	}
}

func TestNewExponentialBackoff_ZeroMaxElapsedTimeDoesNotCapByMaxInterval(t *testing.T) {
	t.Parallel()

	const maxInterval = 5 * time.Millisecond
	cfg := &ExponentialConfig{
		InitialInterval: 3 * time.Millisecond,
		MaxInterval:     maxInterval,
		MaxRetries:      6,
	}

	bo := NewExponentialBackoff(context.Background(), cfg)

	wantErr := errors.New("boom")
	start := time.Now()
	err := bo.Retry(func() error { return wantErr })
	elapsed := time.Since(start)

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}
	if elapsed <= maxInterval {
		t.Errorf("total retry time %v did not exceed MaxInterval %v; MaxInterval is capping total time", elapsed, maxInterval)
	}
}
