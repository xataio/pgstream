// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
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
}

type Config struct {
	SourcePGURL string
	TargetPGURL string
}

type (
	pgdumpFn    func(pglib.PGDumpOptions) ([]byte, error)
	pgrestoreFn func(pglib.PGRestoreOptions, []byte) (string, error)
)

const publicSchema = "public"

// NewSnapshotGenerator will return a postgres schema snapshot generator that
// uses pg_dump and pg_restore to sync the schema of two postgres databases
func NewSnapshotGenerator(ctx context.Context, c *Config) (*SnapshotGenerator, error) {
	targetConn, err := pglib.NewConnPool(ctx, c.TargetPGURL)
	if err != nil {
		return nil, err
	}
	return &SnapshotGenerator{
		sourceURL:   c.SourcePGURL,
		targetURL:   c.TargetPGURL,
		pgDumpFn:    pglib.RunPGDump,
		pgRestoreFn: pglib.RunPGRestore,
		targetConn:  targetConn,
	}, nil
}

func (s *SnapshotGenerator) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) error {
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
