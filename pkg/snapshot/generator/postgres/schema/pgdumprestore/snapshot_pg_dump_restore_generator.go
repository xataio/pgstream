// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	"github.com/xataio/pgstream/pkg/snapshot"
)

// SnapshotGenerator generates postgres schema snapshots using pg_dump and
// pg_restore
type SnapshotGenerator struct {
	sourceURL      string
	targetURL      string
	pgDumpFn       pgdumpFn
	pgRestoreFn    pgrestoreFn
	schemalogStore schemalog.Store
	connBuilder    pglib.QuerierBuilder
	logger         loglib.Logger
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
	sg := &SnapshotGenerator{
		sourceURL:   c.SourcePGURL,
		targetURL:   c.TargetPGURL,
		pgDumpFn:    pglib.RunPGDump,
		pgRestoreFn: pglib.RunPGRestore,
		connBuilder: pglib.ConnBuilder,
		logger:      loglib.NewNoopLogger(),
	}

	for _, opt := range opts {
		opt(sg)
	}

	if err := sg.initialiseSchemaLogStore(ctx); err != nil {
		return nil, err
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
		if err := s.createSchemaIfNotExists(ctx, ss.SchemaName); err != nil {
			return err
		}
	}

	_, err = s.pgRestoreFn(s.pgrestoreOptions(), dump)
	if err != nil {
		return err
	}

	// if we perform a schema snapshot using pg_dump/pg_restore, we need to make
	// sure the schema_log table is updated accordingly with the schema view so
	// that replication can work as expected if configured.
	if s.schemalogStore != nil {
		if _, err := s.schemalogStore.Insert(ctx, ss.SchemaName); err != nil {
			return fmt.Errorf("inserting schemalog entry after schema snapshot: %w", err)
		}
	}

	return nil
}

func (s *SnapshotGenerator) Close() error {
	if s.schemalogStore != nil {
		return s.schemalogStore.Close()
	}
	return nil
}

func (s *SnapshotGenerator) createSchemaIfNotExists(ctx context.Context, schemaName string) error {
	targetConn, err := s.connBuilder(ctx, s.targetURL)
	if err != nil {
		return err
	}
	defer targetConn.Close(context.Background())

	_, err = targetConn.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName))
	return err
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

func (s *SnapshotGenerator) initialiseSchemaLogStore(ctx context.Context) error {
	exists, err := s.schemalogExists(ctx)
	if err != nil {
		return err
	}

	if !exists {
		return nil
	}

	s.schemalogStore, err = schemalogpg.NewStore(ctx, schemalogpg.Config{URL: s.sourceURL})
	return err
}

func (s *SnapshotGenerator) schemalogExists(ctx context.Context) (bool, error) {
	conn, err := s.connBuilder(ctx, s.sourceURL)
	if err != nil {
		return false, err
	}
	defer conn.Close(context.Background())

	// check if the pgstream.schema_log table exists, if not, we can skip the initialisation
	// of the schemalog store
	existsQuery := "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)"
	var exists bool
	err = conn.QueryRow(ctx, existsQuery, schemalog.SchemaName, schemalog.TableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("checking schemalog table existence: %w", err)
	}

	return exists, nil
}
