// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/snapshot"
)

// SnapshotGenerator generates postgres schema snapshots using pg_dump and
// pg_restore
type SnapshotGenerator struct {
	sourceURL   string
	targetURL   string
	pgDumpFn    pgdumpFn
	pgRestoreFn pgrestoreFn
	targetConn  pglib.Querier
	logger      loglib.Logger
}

type Config struct {
	SourcePGURL string
	TargetPGURL string
}

type (
	pgdumpFn    func(pglib.PGDumpOptions) ([]byte, error)
	pgrestoreFn func(pglib.PGRestoreOptions, []byte) (string, error)
)

type Option func(s *SnapshotGenerator)

const publicSchema = "public"

// NewSnapshotGenerator will return a postgres schema snapshot generator that
// uses pg_dump and pg_restore to sync the schema of two postgres databases
func NewSnapshotGenerator(ctx context.Context, c *Config, opts ...Option) (*SnapshotGenerator, error) {
	targetConn, err := pglib.NewConnPool(ctx, c.TargetPGURL)
	if err != nil {
		return nil, err
	}
	sg := &SnapshotGenerator{
		sourceURL:   c.SourcePGURL,
		targetURL:   c.TargetPGURL,
		pgDumpFn:    pglib.RunPGDump,
		pgRestoreFn: pglib.RunPGRestore,
		targetConn:  targetConn,
		logger:      loglib.NewNoopLogger(),
	}

	for _, opt := range opts {
		opt(sg)
	}

	return sg, nil
}

func WithLogger(logger loglib.Logger) Option {
	return func(sg *SnapshotGenerator) {
		sg.logger = loglib.NewLogger(logger).WithFields(loglib.Fields{
			loglib.ModuleField: "postgres_schema_snapshot_generator",
		})
	}
}

func (s *SnapshotGenerator) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) error {
	s.logger.Info("creating schema snapshot", loglib.Fields{"schema": ss.SchemaName, "tables": ss.TableNames})
	// when only the schema filter is provided to pg_dump, it will try to create
	// it. If there are no tables in the snapshot request, we can skip the
	// snapshot for public schema, since it already exists by default.
	if ss.SchemaName == publicSchema && len(ss.TableNames) == 0 {
		return nil
	}
	dump, err := s.pgDumpFn(s.pgdumpOptions(ss))
	if err != nil {
		return err
	}

	// if we use table filtering in the pg_dump command, the schema creation
	// will not be dumped, so it needs to be created explicitly (except for
	// public schema)
	if len(ss.TableNames) > 0 && ss.SchemaName != publicSchema {
		_, err = s.targetConn.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", ss.SchemaName))
		if err != nil {
			return err
		}
	}

	_, err = s.pgRestoreFn(s.pgrestoreOptions(), dump)
	if err != nil {
		return err
	}

	return nil
}

func (s *SnapshotGenerator) Close() error {
	return s.targetConn.Close(context.Background())
}

func (s *SnapshotGenerator) pgdumpOptions(ss *snapshot.Snapshot) pglib.PGDumpOptions {
	opts := pglib.PGDumpOptions{
		ConnectionString: s.sourceURL,
		Format:           "c",
		SchemaOnly:       true,
		Schemas:          []string{ss.SchemaName},
	}

	for _, table := range ss.TableNames {
		opts.Tables = append(opts.Tables, ss.SchemaName+"."+table)
	}

	return opts
}

func (s *SnapshotGenerator) pgrestoreOptions() pglib.PGRestoreOptions {
	return pglib.PGRestoreOptions{
		ConnectionString: s.targetURL,
		SchemaOnly:       true,
	}
}
