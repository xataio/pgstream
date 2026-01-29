// SPDX-License-Identifier: Apache-2.0

package migrator

import (
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	bindata "github.com/golang-migrate/migrate/v4/source/go_bindata"
	pgcoremigrations "github.com/xataio/pgstream/migrations/postgres/core"
	pginjectormigrations "github.com/xataio/pgstream/migrations/postgres/injector"
)

type Migrator struct {
	migrators []*migrate.Migrate
	assets    []*MigrationAssets
}

type MigrationAssets struct {
	AssetNames []string
	AssetFn    func(name string) ([]byte, error)
	TableName  string
}

type MigrationStatus struct {
	TableName              string
	Version                uint
	Dirty                  bool
	ExpectedMigrationCount uint
}

var (
	ErrNoChange         = errors.New("no change")
	ErrNoAssetsProvided = errors.New("no migration assets provided")
	ErrNoMigration      = errors.New("no migration found")
)

// NewPGMigrator creates a new Migrator instance for the provided Postgres URL
// and migration assets. The provided assets will be used to create separate
// migrators, each managing its own schema_migrations table under the pgstream
// schema. They will be applied in the order they are provided.
func NewPGMigrator(pgURL string, migrationAssets []*MigrationAssets) (*Migrator, error) {
	if len(migrationAssets) == 0 {
		return nil, ErrNoAssetsProvided
	}

	migrators := make([]*migrate.Migrate, 0, len(migrationAssets))
	for _, assets := range migrationAssets {
		src := bindata.Resource(assets.AssetNames, assets.AssetFn)
		d, err := bindata.WithInstance(src)
		if err != nil {
			return nil, err
		}

		// Use x-migrations-table-quoted parameter to specify schema-qualified table name
		// The value 1 means the table name should be used as-is with proper quoting
		// This ensures the migrate library uses pgstream.schema_migrations,
		// preventing issues when the search path is not honored by the provider
		// URL-encode the double quotes as %22
		var url string
		if strings.Contains(pgURL, "?") {
			url = pgURL + `&x-migrations-table=%22pgstream%22.%22` + assets.TableName + `%22&x-migrations-table-quoted=1`
		} else {
			url = pgURL + `?x-migrations-table=%22pgstream%22.%22` + assets.TableName + `%22&x-migrations-table-quoted=1`
		}

		m, err := migrate.NewWithSourceInstance("go-bindata", d, url)
		if err != nil {
			return nil, err
		}
		migrators = append(migrators, m)
	}

	return &Migrator{migrators: migrators, assets: migrationAssets}, nil
}

// Up will apply all the migrations provided in the migration assets.
func (m *Migrator) Up() error {
	for _, migrator := range m.migrators {
		if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return mapError(err)
		}
	}
	return nil
}

// Down will revert the migrations provided in the migration assets. They will
// be reverted in reverse order.
func (m *Migrator) Down() error {
	for i := len(m.migrators) - 1; i >= 0; i-- {
		migrator := m.migrators[i]
		if err := migrator.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return mapError(err)
		}
	}
	return nil
}

func (m *Migrator) Close() {
	for _, migrator := range m.migrators {
		_, _ = migrator.Close()
	}
}

func (m *Migrator) Status() ([]MigrationStatus, error) {
	statuses := make([]MigrationStatus, 0, len(m.migrators))
	for i, migrator := range m.migrators {
		version, dirty, err := migrator.Version()
		if err != nil {
			if errors.Is(err, migrate.ErrNilVersion) {
				statuses = append(statuses, MigrationStatus{
					TableName: m.assets[i].TableName,
					Version:   0,
					Dirty:     false,
					// each migration has an up and down file
					ExpectedMigrationCount: uint(len(m.assets[i].AssetNames) / 2),
				})
				continue
			}
			return nil, fmt.Errorf("getting migration version: %w", mapError(err))
		}

		statuses = append(statuses, MigrationStatus{
			TableName: m.assets[i].TableName,
			Version:   version,
			Dirty:     dirty,
			// each migration has an up and down file
			ExpectedMigrationCount: uint(len(m.assets[i].AssetNames) / 2),
		})
	}

	return statuses, nil
}

func GetInjectorMigrationAssets() *MigrationAssets {
	return &MigrationAssets{
		AssetNames: pginjectormigrations.AssetNames(),
		AssetFn:    pginjectormigrations.Asset,
		TableName:  "schema_migrations_injector",
	}
}

func GetCoreMigrationAssets() *MigrationAssets {
	return &MigrationAssets{
		AssetNames: pgcoremigrations.AssetNames(),
		AssetFn:    pgcoremigrations.Asset,
		TableName:  "schema_migrations_core",
	}
}

func mapError(err error) error {
	if errors.Is(err, migrate.ErrNilVersion) {
		return ErrNoMigration
	}
	if errors.Is(err, migrate.ErrNoChange) {
		return ErrNoChange
	}
	return err
}
