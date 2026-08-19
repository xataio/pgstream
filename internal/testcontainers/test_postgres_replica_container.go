// SPDX-License-Identifier: Apache-2.0

package testcontainers

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	replicaSetupUser     = "postgres"
	replicaSetupPassword = "postgres"
	replicaSetupDB       = "testdb"

	// primaryNetworkAlias is the hostname the standby uses to reach the
	// primary over the shared docker network.
	primaryNetworkAlias = "primary"

	// physicalSlotName is the physical replication slot the standby creates on
	// the primary, so the primary retains the WAL the standby still needs.
	physicalSlotName = "pgstream_test_physical_slot"
)

// standbySetupScript runs as the standby container's entrypoint. It takes a
// base backup of the primary, turns the result into a standby, and then execs
// postgres in its place.
//
// The two settings appended to postgresql.auto.conf are both load bearing:
//   - wal_level=logical is NOT inherited from the primary. The primary sets it
//     via a command line flag, which never reaches postgresql.conf and so is
//     not copied by the base backup. Without it the standby comes up at
//     wal_level=replica and logical slot creation fails with
//     `logical decoding requires "wal_level" >= "logical"`.
//   - hot_standby_feedback=on stops the primary from vacuuming away catalog
//     rows the logical decoder on the standby still needs. Without it the
//     standby's logical slot is invalidated by recovery conflicts.
//   - output_plugin_libraries names wal2json as a trusted output plugin. Recent
//     Postgres minors refuse to load any plugin absent from this allowlist,
//     with `library "wal2json" may not be used as an output plugin`. It shares
//     wal_level's problem: the primary sets it on the command line, so the base
//     backup never sees it.
//
// The PG_VERSION guard keeps the script idempotent, so a container restart
// re-execs postgres against the existing data directory rather than trying to
// take a second base backup (which would fail on the already-existing slot).
const standbySetupScript = `
set -e
until pg_isready -h ` + primaryNetworkAlias + ` -U ` + replicaSetupUser + ` -q; do sleep 1; done
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  rm -rf "$PGDATA"/* || true
  chown postgres:postgres "$PGDATA"
  chmod 0700 "$PGDATA"
  gosu postgres pg_basebackup \
    -h ` + primaryNetworkAlias + ` -p 5432 -U ` + replicaSetupUser + ` \
    -D "$PGDATA" -Fp -Xs -R -S ` + physicalSlotName + ` -C
  {
    echo "wal_level = logical"
    echo "hot_standby_feedback = on"
    echo "output_plugin_libraries = 'wal2json'"
  } >> "$PGDATA/postgresql.auto.conf"
fi
exec gosu postgres postgres
`

// SetupPostgresPrimaryReplica starts a Postgres primary together with a
// physical streaming replica of it, on a shared docker network, and writes
// their connection strings to primaryURL and replicaURL.
//
// Both run Postgres 17 with wal2json, built from testdata/pg-wal2json. That
// combination is required and not otherwise available in this repo: logical
// decoding on a standby needs Postgres 16+, while the debezium/postgres images
// used by the rest of the suite only ship wal2json up to their 14 tag.
//
// The returned cleanup tears down both containers and the network.
func SetupPostgresPrimaryReplica(ctx context.Context, primaryURL, replicaURL *string) (cleanup, error) {
	cleanups := []cleanup{}
	cleanupAll := func() error {
		var err error
		// unwind in reverse order, so the network outlives the containers
		// attached to it
		for i := len(cleanups) - 1; i >= 0; i-- {
			if cerr := cleanups[i](); cerr != nil && err == nil {
				err = cerr
			}
		}
		return err
	}
	// on any failure below, release whatever was already started
	failed := true
	defer func() {
		if failed {
			cleanupAll() //nolint:errcheck
		}
	}()

	nw, err := network.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating docker network: %w", err)
	}
	cleanups = append(cleanups, func() error { return nw.Remove(ctx) })

	dockerfileDir, err := walReplicaDockerfileDir()
	if err != nil {
		return nil, err
	}

	primary, err := runPostgresReplicaContainer(ctx, testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    dockerfileDir,
			Dockerfile: "Dockerfile",
			KeepImage:  true,
		},
		ExposedPorts:   []string{"5432/tcp"},
		Networks:       []string{nw.Name},
		NetworkAliases: map[string][]string{nw.Name: {primaryNetworkAlias}},
		Env: map[string]string{
			"POSTGRES_USER":     replicaSetupUser,
			"POSTGRES_PASSWORD": replicaSetupPassword,
			"POSTGRES_DB":       replicaSetupDB,
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
			"-c", "output_plugin_libraries=wal2json",
			"-c", "max_wal_senders=10",
			"-c", "max_replication_slots=10",
			"-c", "hot_standby=on",
		},
		WaitingFor: wait.
			ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(2 * time.Minute),
	})
	if err != nil {
		return nil, fmt.Errorf("starting primary postgres container: %w", err)
	}
	cleanups = append(cleanups, func() error { return primary.Terminate(ctx) })

	// the stock pg_hba.conf has no entry for replication connections, so
	// pg_basebackup from the standby would be rejected
	if err := allowReplicationConnections(ctx, primary); err != nil {
		return nil, err
	}

	standby, err := runPostgresReplicaContainer(ctx, testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    dockerfileDir,
			Dockerfile: "Dockerfile",
			KeepImage:  true,
		},
		ExposedPorts: []string{"5432/tcp"},
		Networks:     []string{nw.Name},
		Env:          map[string]string{"PGPASSWORD": replicaSetupPassword},
		Entrypoint:   []string{"bash", "-c", standbySetupScript},
		WaitingFor: wait.
			ForLog("database system is ready to accept read-only connections").
			WithStartupTimeout(2 * time.Minute),
	})
	if err != nil {
		return nil, fmt.Errorf("starting standby postgres container: %w", err)
	}
	cleanups = append(cleanups, func() error { return standby.Terminate(ctx) })

	if *primaryURL, err = postgresReplicaURL(ctx, primary); err != nil {
		return nil, fmt.Errorf("retrieving primary connection string: %w", err)
	}
	if *replicaURL, err = postgresReplicaURL(ctx, standby); err != nil {
		return nil, fmt.Errorf("retrieving standby connection string: %w", err)
	}

	failed = false
	return cleanupAll, nil
}

func runPostgresReplicaContainer(ctx context.Context, req testcontainers.ContainerRequest) (testcontainers.Container, error) {
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func allowReplicationConnections(ctx context.Context, ctr testcontainers.Container) error {
	appendHBA := []string{
		"bash", "-c",
		`echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"`,
	}
	if code, _, err := ctr.Exec(ctx, appendHBA); err != nil || code != 0 {
		return fmt.Errorf("appending replication entry to pg_hba.conf (exit %d): %w", code, err)
	}

	reload := []string{"psql", "-U", replicaSetupUser, "-tAc", "select pg_reload_conf()"}
	if code, _, err := ctr.Exec(ctx, reload); err != nil || code != 0 {
		return fmt.Errorf("reloading postgres configuration (exit %d): %w", code, err)
	}
	return nil
}

func postgresReplicaURL(ctx context.Context, ctr testcontainers.Container) (string, error) {
	host, err := ctr.Host(ctx)
	if err != nil {
		return "", err
	}
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		replicaSetupUser, replicaSetupPassword, host, port.Port(), replicaSetupDB), nil
}

// walReplicaDockerfileDir resolves the build context relative to this source
// file, so the helper works regardless of the test's working directory.
func walReplicaDockerfileDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolving testcontainers source path")
	}
	return filepath.Join(filepath.Dir(thisFile), "testdata", "pg-wal2json"), nil
}
