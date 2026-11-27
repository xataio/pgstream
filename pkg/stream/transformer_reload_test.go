// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
)

const transformerReloadTestTimeout = 5 * time.Second

type fakeTransformerReloader struct {
	reloadFn func(context.Context) (bool, error)
	calls    atomic.Int32
}

func (f *fakeTransformerReloader) Reload(ctx context.Context) (bool, error) {
	f.calls.Add(1)
	if f.reloadFn != nil {
		return f.reloadFn(ctx)
	}
	return false, nil
}

func waitForTransformerReloadTestSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(transformerReloadTestTimeout):
		t.Fatal("timed out waiting for signal")
	}
}

func TestStartTransformerReloadWorker_ProcessesRequests(t *testing.T) {
	t.Parallel()

	called := make(chan struct{}, 1)
	reloader := &fakeTransformerReloader{
		reloadFn: func(context.Context) (bool, error) {
			called <- struct{}{}
			return true, nil
		},
	}
	requests := make(chan struct{}, 1)
	stop := startTransformerReloadWorker(t.Context(), reloader, requests, loglib.NewNoopLogger())
	defer stop()

	requests <- struct{}{}
	waitForTransformerReloadTestSignal(t, called)
	require.Equal(t, int32(1), reloader.calls.Load())
}

func TestStartTransformerReloadWorker_NoAutomaticReload(t *testing.T) {
	t.Parallel()

	reloader := &fakeTransformerReloader{}
	requests := make(chan struct{}, 1)
	stop := startTransformerReloadWorker(t.Context(), reloader, requests, loglib.NewNoopLogger())
	stop()

	require.Equal(t, int32(0), reloader.calls.Load())
}

func TestStartTransformerReloadWorker_StopWaitsForReload(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	release := make(chan struct{})
	reloader := &fakeTransformerReloader{
		reloadFn: func(context.Context) (bool, error) {
			close(entered)
			<-release
			return true, nil
		},
	}
	requests := make(chan struct{}, 1)
	stop := startTransformerReloadWorker(t.Context(), reloader, requests, loglib.NewNoopLogger())

	requests <- struct{}{}
	waitForTransformerReloadTestSignal(t, entered)

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("stop returned before the in-flight reload completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	waitForTransformerReloadTestSignal(t, stopped)
	require.Equal(t, int32(1), reloader.calls.Load())
}

func TestStartTransformerReloadWorker_NilInputs(t *testing.T) {
	t.Parallel()

	require.NotPanics(t, func() {
		stop := startTransformerReloadWorker(t.Context(), nil, nil, loglib.NewNoopLogger())
		stop()
	})

	require.NotPanics(t, func() {
		requests := make(chan struct{}, 1)
		stop := startTransformerReloadWorker(t.Context(), nil, requests, loglib.NewNoopLogger())
		stop()
	})
}

func TestStartTransformerReloadWorker_ClosedRequestChannel(t *testing.T) {
	t.Parallel()

	requests := make(chan struct{})
	close(requests)
	reloader := &fakeTransformerReloader{}
	stop := startTransformerReloadWorker(t.Context(), reloader, requests, loglib.NewNoopLogger())
	stop()

	require.Equal(t, int32(0), reloader.calls.Load())
}

func TestTransformerReloadOptions(t *testing.T) {
	t.Parallel()

	loader := func(context.Context) (*transformer.Config, error) {
		return nil, nil
	}
	requests := make(chan struct{})
	cfg := (&Config{}).GetInitConfig(
		WithTransformerConfigLoader(loader),
		WithTransformerReloadRequests(requests),
	)

	require.NotNil(t, cfg.TransformerConfigLoader)
	require.Equal(t, (<-chan struct{})(requests), cfg.TransformerReloadRequests)
}

func TestAddProcessorModifiers_ExposesTransformerReloader(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Processor: ProcessorConfig{
			Transformer: &transformer.Config{
				TransformerRules: []transformer.TableRules{
					{
						Schema: "public",
						Table:  "test_table",
						ColumnRules: map[string]transformer.TransformerRules{
							"column_1": {Name: "string"},
						},
					},
				},
			},
		},
	}
	chain, closer, err := addProcessorModifiers(t.Context(), cfg, loglib.NewNoopLogger(), &mocks.Processor{}, nil)
	require.NoError(t, err)
	defer closer()

	require.NotNil(t, chain.transformerReloader)
}
