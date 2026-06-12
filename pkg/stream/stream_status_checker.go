// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"

	migratorlib "github.com/xataio/pgstream/internal/migrator"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/transformers/builder"
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
	migratorBuilder      func(*InitConfig) (migrator, error)
	ruleValidatorBuilder func(context.Context, string, []string) (ruleValidator, error)
}

type ruleValidator func(ctx context.Context, rules transformer.Rules) (*transformer.TransformerMap, error)

type migrator interface {
	Status() ([]migratorlib.MigrationStatus, error)
	Close()
}

const wal2jsonPlugin = "wal2json"

const (
	noPgstreamSchemaErrMsg     = "pgstream schema does not exist in the configured postgres database"
	noMigrationsTableErrMsg    = "pgstream schema migrations table does not exist in the configured postgres database"
	sourceNotProvided          = "source not provided"
	sourcePostgresNotReachable = "source postgres not reachable"
)

const schemaTableLimitQuery = "SELECT * FROM %s LIMIT 0"

const schemaReplicaIdentityQuery = "SELECT c.relreplident, EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary), EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisreplident) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'"

func NewStatusChecker() *StatusChecker {
	return &StatusChecker{
		connBuilder:  pglib.ConnBuilder,
		configParser: pglib.ParseConfig,
		migratorBuilder: func(cfg *InitConfig) (migrator, error) {
			migrationAssets := []*migratorlib.MigrationAssets{
				migratorlib.GetCoreMigrationAssets(),
			}
			if cfg.InjectorMigrationsEnabled {
				migrationAssets = append(migrationAssets, migratorlib.GetInjectorMigrationAssets())
			}
			return migratorlib.NewPGMigrator(cfg.PostgresURL, migrationAssets)
		},
		ruleValidatorBuilder: func(ctx context.Context, pgURL string, requiredTables []string) (ruleValidator, error) {
			validator, err := transformer.NewPostgresTransformerParser(ctx, pgURL, builder.NewTransformerBuilder(), requiredTables)
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

	initStatus, err := s.initStatus(ctx, config.GetInitConfig())
	if err != nil {
		return nil, fmt.Errorf("checking init status: %w", err)
	}

	transformationRulesStatus, err := s.TransformationRulesStatus(ctx, config)
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
func (s *StatusChecker) initStatus(ctx context.Context, cfg *InitConfig) (*InitStatus, error) {
	if cfg.PostgresURL == "" {
		return nil, errMissingPostgresURL
	}

	initStatus := &InitStatus{}

	var err error
	initStatus.PgstreamSchema, err = s.validateSchemaStatus(ctx, cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("validating pgstream schema: %w", err)
	}

	if !initStatus.PgstreamSchema.SchemaExists {
		initStatus.Migrations = []MigrationStatus{
			{
				Version: 0,
				Dirty:   true,
				Errors:  []string{noMigrationsTableErrMsg},
			},
		}
	} else {
		// only validate migrations if the pgstream schema exists
		initStatus.Migrations, err = s.validateMigrationStatus(cfg)
		if err != nil {
			return nil, fmt.Errorf("validating pgstream migrations: %w", err)
		}
	}

	initStatus.ReplicationSlot, err = s.validateReplicationSlotStatus(ctx, cfg.PostgresURL, cfg.ReplicationSlotName)
	if err != nil {
		return nil, fmt.Errorf("validating replication slot: %w", err)
	}

	return initStatus, nil
}

// TransformationRulesStatus validates that the transformation rules provided in
// the configuration are valid, in line with the validation performed during the
// pgstream run/snapshot commands.
func (s *StatusChecker) TransformationRulesStatus(ctx context.Context, config *Config) (*TransformationRulesStatus, error) {
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
	validator, err := s.ruleValidatorBuilder(ctx, pgURL, config.RequiredTables())
	if err != nil {
		return nil, err
	}
	rules := transformer.Rules{
		Transformers:   config.Processor.Transformer.TransformerRules,
		ValidationMode: config.Processor.Transformer.ValidationMode,
	}
	if _, err := validator(ctx, rules); err != nil {
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

// SchemaCompatibilityStatus validates the source PostgreSQL schema for tables
// that pgstream will read from, checking type resolution and replica identity
// safety before the first snapshot or replication event runs.
func (s *StatusChecker) SchemaCompatibilityStatus(ctx context.Context, config *Config) (*SchemaCompatibilityStatus, error) {
	pgURL := config.SourcePostgresURL()
	if pgURL == "" {
		return nil, errMissingPostgresURL
	}

	conn, err := s.connBuilder(ctx, pgURL)
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) {
			return &SchemaCompatibilityStatus{
				Valid: false,
				Tables: []SchemaTableStatus{
					{
						Errors: []string{fmt.Sprintf("cannot validate schema compatibility: %s", sourcePostgresNotReachable)},
					},
				},
			}, nil
		}
		return nil, err
	}
	defer conn.Close(ctx)

	tableNames, err := s.schemaCompatibilityTables(ctx, conn, config.RequiredTables())
	if err != nil {
		return nil, err
	}

	status := &SchemaCompatibilityStatus{
		Valid:  true,
		Tables: make([]SchemaTableStatus, 0, len(tableNames)),
	}
	mapper := pglib.NewMapper(conn)

	for _, tableName := range tableNames {
		schemaName, tableBaseName, err := splitQualifiedTableName(tableName)
		if err != nil {
			status.Valid = false
			status.Tables = append(status.Tables, SchemaTableStatus{
				Schema: "",
				Table:  tableName,
				Errors: []string{err.Error()},
			})
			continue
		}

		tableStatus := SchemaTableStatus{Schema: schemaName, Table: tableBaseName}
		if errs := s.validateSchemaTableTypes(ctx, conn, mapper, schemaName, tableBaseName); len(errs) > 0 {
			tableStatus.Errors = append(tableStatus.Errors, errs...)
		}

		if errs := s.validateSchemaTableReplicaIdentity(ctx, conn, schemaName, tableBaseName); len(errs) > 0 {
			tableStatus.Errors = append(tableStatus.Errors, errs...)
		}

		if len(tableStatus.Errors) > 0 {
			status.Valid = false
		}
		status.Tables = append(status.Tables, tableStatus)
	}

	return status, nil
}

// validateMigrationStatus checks the migration status of the pgstream schema,
// ensuring the number of migrations applied corresponds to the expected ones,
// and they are not dirty (unsuccessfully applied).
func (s *StatusChecker) validateMigrationStatus(cfg *InitConfig) ([]MigrationStatus, error) {
	migrator, err := s.migratorBuilder(cfg)
	if err != nil {
		switch {
		case strings.Contains(err.Error(), "failed to open database: no schema"):
			return []MigrationStatus{
				{
					Errors: []string{noMigrationsTableErrMsg},
				},
			}, nil
		case errors.Is(err, syscall.ECONNREFUSED):
			return []MigrationStatus{
				{
					Errors: []string{fmt.Sprintf("cannot validate migration status: %s", sourcePostgresNotReachable)},
				},
			}, nil
		default:
			return nil, fmt.Errorf("error creating postgres migrator: %w", err)
		}
	}
	defer migrator.Close()

	migrationStatusList, err := migrator.Status()
	if err != nil {
		return nil, fmt.Errorf("error getting migration status: %w", err)
	}

	migrationStatusResponseList := make([]MigrationStatus, 0, len(migrationStatusList))
	for _, status := range migrationStatusList {
		var errs []string
		if status.Version == 0 {
			errs = append(errs, fmt.Sprintf("migration table %s does not exist", status.TableName))
		}
		if status.Version != status.ExpectedMigrationCount {
			errs = append(errs, fmt.Sprintf("migration version (%d) does not match the number of migration files (%d) for table %s", status.Version, status.ExpectedMigrationCount, status.TableName))
		}
		if status.Dirty {
			errs = append(errs, fmt.Sprintf("migration version %d is dirty for table %s", status.Version, status.TableName))
		}

		migrationStatusResponseList = append(migrationStatusResponseList, MigrationStatus{
			TableName: status.TableName,
			Version:   status.Version,
			Dirty:     status.Dirty || status.Version == 0,
			Errors:    errs,
		})
	}

	return migrationStatusResponseList, nil
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
	err = conn.QueryRow(ctx, []any{&schemaExists}, "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgstream')")
	if err != nil && !errors.Is(err, pglib.ErrNoRows) {
		return nil, fmt.Errorf("checking if pgstream schema exists: %w", err)
	}

	// if the schema doesn't exist there's no need to check for the schema_log
	// table as it won't exist either
	if !schemaExists {
		return &SchemaStatus{
			SchemaExists: false,
			Errors:       append([]string{}, noPgstreamSchemaErrMsg),
		}, nil
	}

	return &SchemaStatus{
		SchemaExists: schemaExists,
	}, nil
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
	err = conn.QueryRow(ctx, []any{&name, &plugin, &database}, "SELECT slot_name, plugin, database FROM pg_replication_slots WHERE slot_name = $1", replicationSlotName)
	if err != nil && !errors.Is(err, pglib.ErrNoRows) {
		return nil, fmt.Errorf("retrieving replication slot information: %w", err)
	}

	replicationErrs := func() []string {
		if database != cfg.Database {
			return []string{fmt.Sprintf("replication slot %s does not exist in the configured database", replicationSlotName)}
		}

		if plugin != wal2jsonPlugin {
			return []string{fmt.Sprintf("replication slot %s is not using the wal2json plugin", replicationSlotName)}
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

func (s *StatusChecker) schemaCompatibilityTables(ctx context.Context, conn pglib.Querier, requiredTables []string) ([]string, error) {
	if len(requiredTables) > 0 {
		return expandQualifiedTableNames(ctx, conn, requiredTables)
	}

	schemas, err := pglib.DiscoverAllSchemas(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("discovering schemas for validation: %w", err)
	}

	tables := make([]string, 0)
	for _, schema := range schemas {
		schemaTables, err := pglib.DiscoverAllSchemaTables(ctx, conn, schema)
		if err != nil {
			return nil, fmt.Errorf("discovering tables for schema %s: %w", schema, err)
		}
		for _, table := range schemaTables {
			tables = append(tables, pglib.QuoteQualifiedIdentifier(schema, table))
		}
	}

	return tables, nil
}

func expandQualifiedTableNames(ctx context.Context, conn pglib.Querier, tables []string) ([]string, error) {
	expanded := make([]string, 0, len(tables))
	for _, table := range tables {
		if table == "*" {
			schemas, err := pglib.DiscoverAllSchemas(ctx, conn)
			if err != nil {
				return nil, fmt.Errorf("discovering schemas for wildcard validation: %w", err)
			}
			for _, schema := range schemas {
				schemaTables, err := pglib.DiscoverAllSchemaTables(ctx, conn, schema)
				if err != nil {
					return nil, fmt.Errorf("discovering tables for schema %s: %w", schema, err)
				}
				for _, schemaTable := range schemaTables {
					expanded = append(expanded, pglib.QuoteQualifiedIdentifier(schema, schemaTable))
				}
			}
			continue
		}

		schemaName, tableName, err := splitQualifiedTableName(table)
		if err != nil {
			return nil, err
		}

		switch {
		case schemaName == "*":
			schemas, err := pglib.DiscoverAllSchemas(ctx, conn)
			if err != nil {
				return nil, fmt.Errorf("discovering schemas for wildcard validation: %w", err)
			}
			for _, schema := range schemas {
				schemaTables, err := pglib.DiscoverAllSchemaTables(ctx, conn, schema)
				if err != nil {
					return nil, fmt.Errorf("discovering tables for schema %s: %w", schema, err)
				}
				for _, schemaTable := range schemaTables {
					expanded = append(expanded, pglib.QuoteQualifiedIdentifier(schema, schemaTable))
				}
			}
		case tableName == "*":
			schemaTables, err := pglib.DiscoverAllSchemaTables(ctx, conn, schemaName)
			if err != nil {
				return nil, fmt.Errorf("discovering tables for schema %s: %w", schemaName, err)
			}
			for _, schemaTable := range schemaTables {
				expanded = append(expanded, pglib.QuoteQualifiedIdentifier(schemaName, schemaTable))
			}
		default:
			expanded = append(expanded, pglib.QuoteQualifiedIdentifier(schemaName, tableName))
		}
	}
	return expanded, nil
}

func splitQualifiedTableName(tableName string) (string, string, error) {
	parts := strings.Split(tableName, ".")
	switch len(parts) {
	case 1:
		return "public", pglib.UnquoteIdentifier(parts[0]), nil
	case 2:
		return pglib.UnquoteIdentifier(parts[0]), pglib.UnquoteIdentifier(parts[1]), nil
	default:
		return "", "", fmt.Errorf("invalid table name, expected format schema.table or table: %s", tableName)
	}
}

func (s *StatusChecker) validateSchemaTableTypes(ctx context.Context, conn pglib.Querier, mapper *pglib.Mapper, schemaName, tableName string) []string {
	rows, err := conn.Query(ctx, fmt.Sprintf(schemaTableLimitQuery, pglib.QuoteQualifiedIdentifier(schemaName, tableName)))
	if err != nil {
		return []string{fmt.Sprintf("querying table %s.%s for compatibility checks: %v", schemaName, tableName, err)}
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	if err := rows.Err(); err != nil {
		return []string{fmt.Sprintf("loading table %s.%s field descriptions: %v", schemaName, tableName, err)}
	}

	errs := make([]string, 0)
	for _, field := range fields {
		typeName, err := mapper.TypeForOID(ctx, field.DataTypeOID)
		if err != nil {
			errs = append(errs, fmt.Sprintf("column %s.%s.%s has unsupported type OID %d: %v", schemaName, tableName, field.Name, field.DataTypeOID, err))
			continue
		}
		_ = typeName
	}

	return errs
}

func (s *StatusChecker) validateSchemaTableReplicaIdentity(ctx context.Context, conn pglib.Querier, schemaName, tableName string) []string {
	const identityErr = "table %s.%s does not have a usable replica identity for update/delete events"

	var relreplident string
	var hasPrimaryKey bool
	var hasReplicaIdentityIndex bool
	err := conn.QueryRow(ctx, []any{&relreplident, &hasPrimaryKey, &hasReplicaIdentityIndex}, schemaReplicaIdentityQuery, schemaName, tableName)
	if err != nil {
		if errors.Is(err, pglib.ErrNoRows) {
			return []string{fmt.Sprintf("table %s.%s does not exist in the source database", schemaName, tableName)}
		}
		return []string{fmt.Sprintf("checking replica identity for %s.%s: %v", schemaName, tableName, err)}
	}

	switch relreplident {
	case "f":
		return nil
	case "i":
		if hasReplicaIdentityIndex {
			return nil
		}
	case "d":
		if hasPrimaryKey {
			return nil
		}
	}

	return []string{fmt.Sprintf(identityErr, schemaName, tableName)}
}
