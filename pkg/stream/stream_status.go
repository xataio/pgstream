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

type Status struct {
	Init                *InitStatus
	Config              *ConfigStatus
	TransformationRules *TransformationRulesStatus
	Source              *SourceStatus
}

type ConfigStatus struct {
	Valid  bool
	Errors []string
}

type TransformationRulesStatus struct {
	Valid  bool
	Errors []string
}

type SourceStatus struct {
	Reachable bool
	Errors    []string
}

type InitStatus struct {
	PgstreamSchema  *SchemaStatus
	Migration       *MigrationStatus
	ReplicationSlot *ReplicationSlotStatus
}

type MigrationStatus struct {
	Version uint
	Dirty   bool
	Errors  []string
}

type ReplicationSlotStatus struct {
	Name     string
	Plugin   string
	Database string
	Errors   []string
}

type SchemaStatus struct {
	SchemaExists         bool
	SchemaLogTableExists bool
	Errors               []string
}

type migrator interface {
	Version() (uint, bool, error)
	Close() (error, error)
}

type StatusChecker struct {
	connBuilder          pglib.QuerierBuilder
	configParser         func(pgURL string) (*pgx.ConnConfig, error)
	migratorBuilder      func(string) (migrator, error)
	ruleValidatorBuilder func(context.Context, string) (ruleValidator, error)
}

type ruleValidator func(rules []transformer.TableRules) (map[string]transformer.ColumnTransformers, error)

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

type StatusErrors map[string][]string

func (se StatusErrors) Keys() []string {
	keys := make([]string, 0, len(se))
	for k := range se {
		keys = append(keys, k)
	}
	return keys
}

func (s *Status) GetErrors() StatusErrors {
	if s == nil {
		return nil
	}

	errors := map[string][]string{}
	if initErrs := s.Init.GetErrors(); len(initErrs) > 0 {
		errors["init"] = initErrs
	}

	if s.Source != nil && len(s.Source.Errors) > 0 {
		errors["source"] = s.Source.Errors
	}

	if s.Config != nil && len(s.Config.Errors) > 0 {
		errors["config"] = s.Config.Errors
	}

	if s.TransformationRules != nil && len(s.TransformationRules.Errors) > 0 {
		errors["transformation_rules"] = s.TransformationRules.Errors
	}

	return errors
}

func (s *Status) PrettyPrint() string {
	if s == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString(s.Init.PrettyPrint())
	prettyPrint.WriteByte('\n')
	prettyPrint.WriteString(s.Config.PrettyPrint())
	prettyPrint.WriteByte('\n')
	prettyPrint.WriteString(s.TransformationRules.PrettyPrint())
	prettyPrint.WriteByte('\n')
	prettyPrint.WriteString(s.Source.PrettyPrint())

	return prettyPrint.String()
}

// GetErrors aggregates all errors from the initialisation status.
func (is *InitStatus) GetErrors() []string {
	if is == nil {
		return nil
	}

	errors := []string{}
	if is.PgstreamSchema != nil && len(is.PgstreamSchema.Errors) > 0 {
		errors = append(errors, is.PgstreamSchema.Errors...)
	}

	if is.Migration != nil && len(is.Migration.Errors) > 0 {
		errors = append(errors, is.Migration.Errors...)
	}

	if is.ReplicationSlot != nil && len(is.ReplicationSlot.Errors) > 0 {
		errors = append(errors, is.ReplicationSlot.Errors...)
	}

	return errors
}

func (is *InitStatus) PrettyPrint() string {
	if is == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString("Initialisation status:\n")
	if is.PgstreamSchema != nil {
		prettyPrint.WriteString(fmt.Sprintf(" - Pgstream schema exists: %t\n", is.PgstreamSchema.SchemaExists))
		prettyPrint.WriteString(fmt.Sprintf(" - Pgstream schema_log table exists: %t\n", is.PgstreamSchema.SchemaLogTableExists))
		if len(is.PgstreamSchema.Errors) > 0 {
			prettyPrint.WriteString(fmt.Sprintf(" - Pgstream schema errors: %s\n", is.PgstreamSchema.Errors))
		}
	}

	if is.Migration != nil {
		prettyPrint.WriteString(fmt.Sprintf(" - Migration current version: %d\n", is.Migration.Version))
		prettyPrint.WriteString(fmt.Sprintf(" - Migration status: %s\n", migrationStatus(is.Migration.Dirty)))
		if len(is.Migration.Errors) > 0 {
			prettyPrint.WriteString(fmt.Sprintf(" - Migration errors: %s\n", is.Migration.Errors))
		}
	}

	if is.ReplicationSlot != nil {
		prettyPrint.WriteString(fmt.Sprintf(" - Replication slot name: %s\n", is.ReplicationSlot.Name))
		prettyPrint.WriteString(fmt.Sprintf(" - Replication slot plugin: %s\n", is.ReplicationSlot.Plugin))
		prettyPrint.WriteString(fmt.Sprintf(" - Replication slot database: %s\n", is.ReplicationSlot.Database))
		if len(is.ReplicationSlot.Errors) > 0 {
			prettyPrint.WriteString(fmt.Sprintf(" - Replication slot errors: %s\n", is.ReplicationSlot.Errors))
		}
	}

	// trim the last newline character
	return prettyPrint.String()[:len(prettyPrint.String())-1]
}

func (ss *SourceStatus) PrettyPrint() string {
	if ss == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString("Source status:\n")
	prettyPrint.WriteString(fmt.Sprintf(" - Reachable: %t\n", ss.Reachable))
	if len(ss.Errors) > 0 {
		prettyPrint.WriteString(fmt.Sprintf(" - Errors: %s\n", ss.Errors))
	}

	// trim the last newline character
	return prettyPrint.String()[:len(prettyPrint.String())-1]
}

func (cs *ConfigStatus) PrettyPrint() string {
	if cs == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString("Config status:\n")
	prettyPrint.WriteString(fmt.Sprintf(" - Valid: %t\n", cs.Valid))
	if len(cs.Errors) > 0 {
		prettyPrint.WriteString(fmt.Sprintf(" - Errors: %s\n", cs.Errors))
	}

	// trim the last newline character
	return prettyPrint.String()[:len(prettyPrint.String())-1]
}

func (trs *TransformationRulesStatus) PrettyPrint() string {
	if trs == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString("Transformation rules status:\n")
	prettyPrint.WriteString(fmt.Sprintf(" - Valid: %t\n", trs.Valid))
	if len(trs.Errors) > 0 {
		prettyPrint.WriteString(fmt.Sprintf(" - Errors: %s\n", trs.Errors))
	}

	// trim the last newline character
	return prettyPrint.String()[:len(prettyPrint.String())-1]
}

func migrationStatus(dirty bool) string {
	if !dirty {
		return "success"
	}
	return "failed"
}
