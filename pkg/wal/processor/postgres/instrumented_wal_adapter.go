// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/wal"

	"go.opentelemetry.io/otel/trace"
)

type instrumentedWalAdapter struct {
	inner  walAdapter
	tracer trace.Tracer
}

func newInstrumentedWalAdapter(a walAdapter, i *otel.Instrumentation) walAdapter {
	if i == nil {
		return a
	}

	return &instrumentedWalAdapter{
		inner:  a,
		tracer: i.Tracer,
	}
}

func (i *instrumentedWalAdapter) walEventToQueries(ctx context.Context, event *wal.Event) (queries []*query, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "walAdapter.walEventToQueries")
	defer otel.CloseSpan(span, err)

	return i.inner.walEventToQueries(ctx, event)
}
