// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmigrations "github.com/xataio/pgstream/migrations/postgres"

	"github.com/jackc/pgx/v5"
)

type InitStatus struct {
	PgstreamSchema  *SchemaStatus
	Migration       *MigrationStatus
	ReplicationSlot *ReplicationSlotStatus
}

type MigrationStatus struct {
	Version uint
	Dirty   bool
	Errors  string
}

type ReplicationSlotStatus struct {
	Name     string
	Plugin   string
	Database string
	Errors   string
}

type SchemaStatus struct {
	SchemaExists         bool
	SchemaLogTableExists bool
	Errors               string
}

type migrator interface {
	Version() (uint, bool, error)
	Close() (error, error)
}

type StatusChecker struct {
	connBuilder     pglib.QuerierBuilder
	configParser    func(pgURL string) (*pgx.ConnConfig, error)
	migratorBuilder func(string) (migrator, error)
}

const wal2jsonPlugin = "wal2json"

var errNoPgstreamSchema = errors.New("pgstream schema does not exist in the configured postgres database")

func NewStatusChecker() *StatusChecker {
	return &StatusChecker{
		connBuilder:     pglib.ConnBuilder,
		configParser:    pgx.ParseConfig,
		migratorBuilder: func(pgURL string) (migrator, error) { return newPGMigrator(pgURL) },
	}
}

// InitStatus checks the initialisation status of pgstream in the provided
// postgres database. If the replicationSlotName is empty, it will check against
// the default (pgstream_<database>_slot).
func (s *StatusChecker) InitStatus(ctx context.Context, pgURL, replicationSlotName string) (*InitStatus, error) {
	if pgURL == "" {
		return nil, errMissingPostgresURL
	}

	initStatus := &InitStatus{}

	var err error
	initStatus.PgstreamSchema, err = s.validateSchemaStatus(ctx, pgURL)
	if err != nil {
		return nil, fmt.Errorf("validating pgstream schema: %w", err)
	}

	// the migrations table exists under the pgstream schema, if it doesn't exist
	// there's no need to check.
	if initStatus.PgstreamSchema != nil && initStatus.PgstreamSchema.SchemaExists {
		initStatus.Migration, err = s.validateMigrationStatus(pgURL)
		if err != nil {
			return nil, fmt.Errorf("validating pgstream migrations: %w", err)
		}
	}

	initStatus.ReplicationSlot, err = s.validateReplicationSlotStatus(ctx, pgURL, replicationSlotName)
	if err != nil {
		return nil, fmt.Errorf("validating replication slot: %w", err)
	}

	return initStatus, nil
}

// validateMigrationStatus checks the migration status of the pgstream schema,
// ensuring the number of migrations applied corresponds to the expected ones,
// and they are not dirty (unsuccessfully applied).
func (s *StatusChecker) validateMigrationStatus(pgURL string) (*MigrationStatus, error) {
	migrator, err := s.migratorBuilder(pgURL)
	if err != nil {
		switch {
		case strings.Contains(err.Error(), "failed to open database: no schema"):
			return &MigrationStatus{
				Errors: errNoPgstreamSchema.Error(),
			}, nil
		default:
			return nil, fmt.Errorf("error creating postgres migrator: %w", err)
		}
	}
	defer migrator.Close()

	version, dirty, err := migrator.Version()
	if err != nil {
		return nil, fmt.Errorf("error getting migration version: %w", err)
	}

	migrationErrs := func() error {
		var errs error
		migrationAssets := pgmigrations.AssetNames()
		if len(migrationAssets)/2 != int(version) {
			if errs == nil {
				errs = fmt.Errorf("migration version %d does not match the number of migration files %d", version, len(migrationAssets)/2)
			}
		}

		if dirty {
			if errs == nil {
				errs = fmt.Errorf("migration version %d is dirty", version)
			} else {
				errs = errors.Join(errs, fmt.Errorf("migration version %d is dirty", version))
			}
		}

		return errs
	}()

	errMsg := ""
	if migrationErrs != nil {
		errMsg = migrationErrs.Error()
	}

	return &MigrationStatus{
		Version: version,
		Dirty:   dirty,
		Errors:  errMsg,
	}, nil
}

// validateSchemaStatus checks if the pgstream schema and schema_log table exist
// in the configured postgres database.
func (s *StatusChecker) validateSchemaStatus(ctx context.Context, pgURL string) (*SchemaStatus, error) {
	conn, err := s.connBuilder(ctx, pgURL)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	var schemaExists bool
	err = conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgstream')").Scan(&schemaExists)
	if err != nil && !errors.Is(err, pglib.ErrNoRows) {
		return nil, fmt.Errorf("checking if pgstream schema exists: %w", err)
	}

	// if the schema doesn't exist there's no need to check for the schema_log
	// table as it won't exist either
	if !schemaExists {
		return &SchemaStatus{
			SchemaExists:         false,
			SchemaLogTableExists: false,
			Errors:               errNoPgstreamSchema.Error(),
		}, nil
	}

	var schemaLogTableExists bool
	err = conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgstream' AND table_name = 'schema_log')").Scan(&schemaLogTableExists)
	if err != nil && !errors.Is(err, pglib.ErrNoRows) {
		return nil, fmt.Errorf("checking if pgstream schema_log table exists: %w", err)
	}

	status := &SchemaStatus{
		SchemaExists:         schemaExists,
		SchemaLogTableExists: schemaLogTableExists,
	}

	if !schemaLogTableExists {
		status.Errors = "schema_log table does not exist in the pgstream schema"
	}

	return status, nil
}

// validateReplicationSlotStatus checks if the replication slot exists in the
// configured postgres database, and it's correctly configured.
func (s *StatusChecker) validateReplicationSlotStatus(ctx context.Context, pgURL, replicationSlotName string) (*ReplicationSlotStatus, error) {
	if replicationSlotName == "" {
		var err error
		replicationSlotName, err = getReplicationSlotName(pgURL)
		if err != nil {
			return nil, err
		}
	}

	// Check the replication slot exists in the configured database by parsing
	// and extracting the database name from the connection string.
	cfg, err := s.configParser(pgURL)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres connection string: %w", err)
	}
	if cfg.Database == "" {
		cfg.Database = "postgres"
	}

	conn, err := s.connBuilder(ctx, pgURL)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	var name, plugin, database string
	err = conn.QueryRow(ctx, "SELECT slot_name, plugin, database FROM pg_replication_slots WHERE slot_name = $1", replicationSlotName).
		Scan(&name, &plugin, &database)
	if err != nil && !errors.Is(err, pglib.ErrNoRows) {
		return nil, fmt.Errorf("retrieving replication slot information: %w", err)
	}

	replicationErrs := func() error {
		if name != replicationSlotName {
			return fmt.Errorf("replication slot %s does not exist in the configured database", replicationSlotName)
		}

		if database != cfg.Database {
			return fmt.Errorf("replication slot %s is not created on the configured database %s", replicationSlotName, cfg.Database)
		}

		if plugin != wal2jsonPlugin {
			return fmt.Errorf("replication slot %s is not using the wal2json plugin", replicationSlotName)
		}

		return nil
	}()

	var errMsg string
	if replicationErrs != nil {
		errMsg = replicationErrs.Error()
	}

	return &ReplicationSlotStatus{
		Name:     name,
		Plugin:   plugin,
		Database: database,
		Errors:   errMsg,
	}, nil
}

// GetErrors aggregates all errors from the initialisation status.
func (is *InitStatus) GetErrors() []string {
	errors := []string{}
	if is.PgstreamSchema != nil && is.PgstreamSchema.Errors != "" {
		errors = append(errors, is.PgstreamSchema.Errors)
	}

	if is.Migration != nil && is.Migration.Errors != "" {
		errors = append(errors, is.Migration.Errors)
	}

	if is.ReplicationSlot != nil && is.ReplicationSlot.Errors != "" {
		errors = append(errors, is.ReplicationSlot.Errors)
	}

	return errors
}
