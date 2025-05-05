// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmigrations "github.com/xataio/pgstream/migrations/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"

	"github.com/jackc/pgx/v5"
)

// StatusChecker is responsible for validating the status of the pgstream setup
// in a PostgreSQL database. It performs checks on the source database
// connection, initialization status (schema, migrations, replication slot), and
// transformation rules. It provides detailed status information, including
// errors, to help diagnose issues with the pgstream configuration and setup.
type StatusChecker struct {
	connBuilder          pglib.QuerierBuilder
	configParser         func(pgURL string) (*pgx.ConnConfig, error)
	migratorBuilder      func(string) (migrator, error)
	ruleValidatorBuilder func(context.Context, string) (ruleValidator, error)
}

type ruleValidator func(rules []transformer.TableRules) (map[string]transformer.ColumnTransformers, error)

type migrator interface {
	Version() (uint, bool, error)
	Close() (error, error)
}

const wal2jsonPlugin = "wal2json"

const (
	noPgstreamSchemaErrMsg         = "pgstream schema does not exist in the configured postgres database"
	noPgstreamSchemaLogTableErrMsg = "pgstream schema_log table does not exist in the configured postgres database"
	noMigrationsTableErrMsg        = "pgstream schema_migrations table does not exist in the configured postgres database"
	sourceNotProvided              = "source not provided"
	sourcePostgresNotReachable     = "source postgres not reachable"
)

func NewStatusChecker() *StatusChecker {
	return &StatusChecker{
		connBuilder:     pglib.ConnBuilder,
		configParser:    pgx.ParseConfig,
		migratorBuilder: func(pgURL string) (migrator, error) { return newPGMigrator(pgURL) },
		ruleValidatorBuilder: func(ctx context.Context, pgURL string) (ruleValidator, error) {
			validator, err := transformer.NewPostgresTransformerParser(ctx, pgURL)
			if err != nil {
				return nil, err
			}
			return validator.ParseAndValidate, nil
		},
	}
}

// Status retrieves the overall status of the pgstream setup, including the
// source database connection status, initialization status (schema, migrations,
// replication slot), and transformation rules validation status. It returns a
// detailed status report that includes any errors encountered during the
// checks, helping to diagnose issues with the pgstream configuration and setup.
func (s *StatusChecker) Status(ctx context.Context, config *Config) (*Status, error) {
	sourceStatus, err := s.sourceStatus(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("checking source status: %w", err)
	}

	initStatus, err := s.initStatus(ctx, config.SourcePostgresURL(), config.PostgresReplicationSlot())
	if err != nil {
		return nil, fmt.Errorf("checking init status: %w", err)
	}

	transformationRulesStatus, err := s.transformationRulesStatus(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("checking transformation rules status: %w", err)
	}

	return &Status{
		Init:                initStatus,
		Config:              s.configStatus(config),
		Source:              sourceStatus,
		TransformationRules: transformationRulesStatus,
	}, nil
}

// configStatus validates if the configuration provided is valid.
func (s *StatusChecker) configStatus(config *Config) *ConfigStatus {
	if err := config.IsValid(); err != nil {
		return &ConfigStatus{
			Valid:  false,
			Errors: []string{err.Error()},
		}
	}

	return &ConfigStatus{
		Valid: true,
	}
}

// sourceStatus validates if the source postgres database is reachable.
func (s *StatusChecker) sourceStatus(ctx context.Context, config *Config) (*SourceStatus, error) {
	sourcePostgresURL := config.SourcePostgresURL()
	if sourcePostgresURL == "" {
		return &SourceStatus{
			Reachable: false,
			Errors:    []string{sourceNotProvided},
		}, nil
	}

	conn, err := s.connBuilder(ctx, sourcePostgresURL)
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) {
			return &SourceStatus{
				Reachable: false,
				Errors:    []string{fmt.Sprintf("%s: %v", sourcePostgresNotReachable, err)},
			}, nil
		}
		return nil, err
	}
	defer conn.Close(context.Background())

	sourceStatus := &SourceStatus{
		Reachable: true,
	}
	if err := conn.Ping(ctx); err != nil {
		sourceStatus.Reachable = false
		sourceStatus.Errors = append(sourceStatus.Errors, err.Error())
	}

	return sourceStatus, nil
}

// initStatus checks the initialisation status of pgstream in the provided
// postgres database. If the replicationSlotName is empty, it will check against
// the default (pgstream_<database>_slot).
func (s *StatusChecker) initStatus(ctx context.Context, pgURL, replicationSlotName string) (*InitStatus, error) {
	if pgURL == "" {
		return nil, errMissingPostgresURL
	}

	initStatus := &InitStatus{}

	var err error
	initStatus.PgstreamSchema, err = s.validateSchemaStatus(ctx, pgURL)
	if err != nil {
		return nil, fmt.Errorf("validating pgstream schema: %w", err)
	}

	initStatus.Migration, err = s.validateMigrationStatus(pgURL)
	if err != nil {
		return nil, fmt.Errorf("validating pgstream migrations: %w", err)
	}

	initStatus.ReplicationSlot, err = s.validateReplicationSlotStatus(ctx, pgURL, replicationSlotName)
	if err != nil {
		return nil, fmt.Errorf("validating replication slot: %w", err)
	}

	return initStatus, nil
}

// transformationRulesStatus validates that the transformation rules provided in
// the configuration are valid, in line with the validation performed during the
// pgstream run/snapshot commands.
func (s *StatusChecker) transformationRulesStatus(ctx context.Context, config *Config) (*TransformationRulesStatus, error) {
	if config.Processor.Transformer == nil {
		return nil, nil
	}

	pgURL := config.SourcePostgresURL()
	if pgURL == "" {
		return &TransformationRulesStatus{
			Valid:  false,
			Errors: []string{fmt.Sprintf("cannot validate transformer rules: %s", sourceNotProvided)},
		}, nil
	}

	status := &TransformationRulesStatus{
		Valid: true,
	}
	validator, err := s.ruleValidatorBuilder(ctx, pgURL)
	if err != nil {
		return nil, err
	}
	if _, err := validator(config.Processor.Transformer.TransformerRules); err != nil {
		status.Valid = false
		switch {
		case errors.Is(err, syscall.ECONNREFUSED):
			status.Errors = append(status.Errors, fmt.Sprintf("cannot validate transformer rules: %s", sourcePostgresNotReachable))
		default:
			status.Errors = append(status.Errors, err.Error())
		}
	}

	return status, nil
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
				Errors: []string{noMigrationsTableErrMsg},
			}, nil
		case errors.Is(err, syscall.ECONNREFUSED):
			return &MigrationStatus{
				Errors: []string{fmt.Sprintf("cannot validate migration status: %s", sourcePostgresNotReachable)},
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

	migrationErrs := func() []string {
		var errs []string
		migrationAssets := pgmigrations.AssetNames()
		if len(migrationAssets)/2 != int(version) {
			errs = append(errs, fmt.Sprintf("migration version (%d) does not match the number of migration files (%d)", version, len(migrationAssets)/2))
		}

		if dirty {
			errs = append(errs, fmt.Sprintf("migration version %d is dirty", version))
		}

		return errs
	}()

	return &MigrationStatus{
		Version: version,
		Dirty:   dirty,
		Errors:  migrationErrs,
	}, nil
}

// validateSchemaStatus checks if the pgstream schema and schema_log table exist
// in the configured postgres database.
func (s *StatusChecker) validateSchemaStatus(ctx context.Context, pgURL string) (*SchemaStatus, error) {
	conn, err := s.connBuilder(ctx, pgURL)
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) {
			return &SchemaStatus{
				Errors: []string{fmt.Sprintf("cannot validate schema status: %s", sourcePostgresNotReachable)},
			}, nil
		}
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
			Errors:               append([]string{}, noPgstreamSchemaErrMsg),
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
		status.Errors = append(status.Errors, noPgstreamSchemaLogTableErrMsg)
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
		if errors.Is(err, syscall.ECONNREFUSED) {
			return &ReplicationSlotStatus{
				Errors: []string{fmt.Sprintf("cannot validate replication slot status: %s", sourcePostgresNotReachable)},
			}, nil
		}
		return nil, err
	}
	defer conn.Close(ctx)

	var name, plugin, database string
	err = conn.QueryRow(ctx, "SELECT slot_name, plugin, database FROM pg_replication_slots WHERE slot_name = $1", replicationSlotName).
		Scan(&name, &plugin, &database)
	if err != nil && !errors.Is(err, pglib.ErrNoRows) {
		return nil, fmt.Errorf("retrieving replication slot information: %w", err)
	}

	replicationErrs := func() []string {
		if database != cfg.Database {
			return append([]string{}, fmt.Sprintf("replication slot %s does not exist in the configured database", replicationSlotName))
		}

		if plugin != wal2jsonPlugin {
			return append([]string{}, fmt.Sprintf("replication slot %s is not using the wal2json plugin", replicationSlotName))
		}

		return nil
	}()

	return &ReplicationSlotStatus{
		Name:     name,
		Plugin:   plugin,
		Database: database,
		Errors:   replicationErrs,
	}, nil
}
