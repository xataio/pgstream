// SPDX-License-Identifier: Apache-2.0

package instrumentation

import (
	"context"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type PGDumpRestore struct {
	pgdumpFn    pglib.PGDumpFn
	pgdumpallFn pglib.PGDumpAllFn
	pgrestoreFn pglib.PGRestoreFn
	tracer      trace.Tracer
}

func NewPGDumpFn(pgdumpFn pglib.PGDumpFn, instrumentation *otel.Instrumentation) pglib.PGDumpFn {
	pgdr := &PGDumpRestore{
		pgdumpFn: pgdumpFn,
		tracer:   instrumentation.Tracer,
	}
	return pgdr.PGDump
}

func NewPGDumpAllFn(pgdumpallFn pglib.PGDumpAllFn, instrumentation *otel.Instrumentation) pglib.PGDumpAllFn {
	pgdr := &PGDumpRestore{
		pgdumpallFn: pgdumpallFn,
		tracer:      instrumentation.Tracer,
	}
	return pgdr.PGDumpAll
}

func NewPGRestoreFn(pgrestoreFn pglib.PGRestoreFn, instrumentation *otel.Instrumentation) pglib.PGRestoreFn {
	pgdr := &PGDumpRestore{
		pgrestoreFn: pgrestoreFn,
		tracer:      instrumentation.Tracer,
	}
	return pgdr.PGRestore
}

func (i *PGDumpRestore) PGDump(ctx context.Context, opts pglib.PGDumpOptions) (dump []byte, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "pgdump", trace.WithAttributes([]attribute.KeyValue{
		{Key: "schemas", Value: attribute.StringSliceValue(opts.Schemas)},
		{Key: "tables", Value: attribute.StringSliceValue(opts.Tables)},
		{Key: "exclude_tables", Value: attribute.StringSliceValue(opts.ExcludeTables)},
		{Key: "clean", Value: attribute.BoolValue(opts.Clean)},
	}...))
	defer otel.CloseSpan(span, err)
	return i.pgdumpFn(ctx, opts)
}

func (i *PGDumpRestore) PGDumpAll(ctx context.Context, opts pglib.PGDumpAllOptions) (dump []byte, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "pgdumpall", trace.WithAttributes([]attribute.KeyValue{
		{Key: "roles_only", Value: attribute.BoolValue(opts.RolesOnly)},
	}...))
	defer otel.CloseSpan(span, err)
	return i.pgdumpallFn(ctx, opts)
}

func (i *PGDumpRestore) PGRestore(ctx context.Context, opts pglib.PGRestoreOptions, dump []byte) (out string, err error) {
	ctx, span := otel.StartSpan(ctx, i.tracer, "pgrestore")
	defer otel.CloseSpan(span, err)
	return i.pgrestoreFn(ctx, opts, dump)
}
