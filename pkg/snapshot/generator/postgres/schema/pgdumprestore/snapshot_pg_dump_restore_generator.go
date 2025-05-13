// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

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
	pgDumpFn       pglib.PGDumpFn
	pgRestoreFn    pglib.PGRestoreFn
	schemalogStore schemalog.Store
	connBuilder    pglib.QuerierBuilder
	cleanTargetDB  bool
	logger         loglib.Logger
}

type Config struct {
	SourcePGURL   string
	TargetPGURL   string
	CleanTargetDB bool
}

type Option func(s *SnapshotGenerator)

const (
	publicSchema = "public"
	wildcard     = "*"
)

// NewSnapshotGenerator will return a postgres schema snapshot generator that
// uses pg_dump and pg_restore to sync the schema of two postgres databases
func NewSnapshotGenerator(ctx context.Context, c *Config, opts ...Option) (*SnapshotGenerator, error) {
	sg := &SnapshotGenerator{
		sourceURL:     c.SourcePGURL,
		targetURL:     c.TargetPGURL,
		pgDumpFn:      pglib.RunPGDump,
		pgRestoreFn:   pglib.RunPGRestore,
		connBuilder:   pglib.ConnBuilder,
		cleanTargetDB: c.CleanTargetDB,
		logger:        loglib.NewNoopLogger(),
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

	pgdumpOpts, err := s.pgdumpOptions(ctx, ss)
	if err != nil {
		return fmt.Errorf("preparing pg_dump options: %w", err)
	}

	dump, err := s.pgDumpFn(ctx, *pgdumpOpts)
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

	_, err = s.pgRestoreFn(ctx, s.pgrestoreOptions(), dump)
	pgrestoreErr := &pglib.PGRestoreErrors{}
	if err != nil {
		switch {
		case errors.As(err, &pgrestoreErr):
			if pgrestoreErr.HasCriticalErrors() {
				return err
			}
			ignoredErrors := pgrestoreErr.GetIgnoredErrors()
			s.logger.Warn(nil, fmt.Sprintf("pg_restore: %d errors ignored", len(ignoredErrors)), loglib.Fields{"errors_ignored": ignoredErrors})
		default:
			return err
		}
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

func (s *SnapshotGenerator) pgrestoreOptions() pglib.PGRestoreOptions {
	return pglib.PGRestoreOptions{
		ConnectionString: s.targetURL,
		SchemaOnly:       true,
		Clean:            s.cleanTargetDB,
		Format:           "p",
	}
}

func (s *SnapshotGenerator) pgdumpOptions(ctx context.Context, ss *snapshot.Snapshot) (*pglib.PGDumpOptions, error) {
	opts := &pglib.PGDumpOptions{
		ConnectionString: s.sourceURL,
		Format:           "p",
		SchemaOnly:       true,
		Schemas:          []string{pglib.QuoteIdentifier(ss.SchemaName)},
		Clean:            s.cleanTargetDB,
	}

	// wildcard means all tables in the schema, so no table filter required
	if slices.Contains(ss.TableNames, wildcard) {
		return opts, nil
	}

	// we use the excluded tables flag to make sure we still dump non table
	// objects for the schema in question. If we use the tables filter, only
	// those tables are dumped, and any related non table objects will not be
	// dumped, causing the restore to fail due to missing related objects.
	if len(ss.TableNames) > 0 {
		var err error
		opts.ExcludeTables, err = s.pgdumpExcludedTables(ctx, ss.SchemaName, ss.TableNames)
		if err != nil {
			return nil, err
		}
	}

	return opts, nil
}

func (s *SnapshotGenerator) pgdumpExcludedTables(ctx context.Context, schemaName string, includeTables []string) ([]string, error) {
	conn, err := s.connBuilder(ctx, s.sourceURL)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	paramRefs := make([]string, 0, len(includeTables))
	tableParams := make([]any, 0, len(includeTables))
	for i, table := range includeTables {
		tableParams = append(tableParams, table)
		paramRefs = append(paramRefs, fmt.Sprintf("$%d", i+1))
	}

	// get all tables in the schema that are not in the include list
	query := fmt.Sprintf("SELECT tablename FROM pg_tables WHERE schemaname = '%s' AND tablename NOT IN (%s)", schemaName, strings.Join(paramRefs, ","))
	rows, err := conn.Query(ctx, query, tableParams...)
	if err != nil {
		return nil, fmt.Errorf("retrieving tables from schema: %w", err)
	}
	defer rows.Close()

	excludeTables := []string{}
	for rows.Next() {
		tableName := ""
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("scanning table name: %w", err)
		}
		excludeTables = append(excludeTables, pglib.QuoteIdentifier(tableName))
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return excludeTables, nil
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
