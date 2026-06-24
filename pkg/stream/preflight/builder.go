// SPDX-License-Identifier: Apache-2.0

package preflight

import "github.com/xataio/pgstream/pkg/stream"

// Builder turns a stream.Config into the concrete checks for a category. Each
// new category adds an entry to Builders and a matching CLI flag in
// cmd/root_cmd.go.
type Builder struct {
	Category Category
	Flag     string
	Build    func(*stream.Config) []Check
}

// Builders is the registry of category builders. Adding a new category = one
// Builder entry here + one flag declaration on checkCmd.
var Builders = []Builder{
	{CategoryConnectivity, "connectivity", BuildConnectivityChecks},
	{CategoryReplication, "replication", BuildReplicationChecks},
}

// BuildConnectivityChecks returns the connectivity checks applicable to cfg.
// A source check is added when a source postgres URL is configured; a target
// check is added when a postgres target is configured.
func BuildConnectivityChecks(cfg *stream.Config) []Check {
	checks := []Check{}
	if url := cfg.SourcePostgresURL(); url != "" {
		checks = append(checks, &ConnectivityCheck{Label: "source", URL: url})
	}
	if cfg.Processor.Postgres != nil {
		if url := cfg.Processor.Postgres.BatchWriter.URL; url != "" {
			checks = append(checks, &ConnectivityCheck{Label: "target", URL: url})
		}
	}
	return checks
}

// BuildReplicationChecks returns the replication-preflight checks applicable
// to cfg. Replication checks only apply when the source is configured with a
// replication slot (i.e. the run is doing logical replication, not a one-shot
// snapshot).
func BuildReplicationChecks(cfg *stream.Config) []Check {
	if cfg.PostgresReplicationSlot() == "" {
		return nil
	}
	url := cfg.SourcePostgresURL()
	if url == "" {
		return nil
	}
	return []Check{
		&WALLevelCheck{URL: url},
		&WAL2JSONCheck{URL: url},
		&ReplicationSlotHeadroomCheck{URL: url},
		&ReplicationRoleAttrCheck{URL: url},
	}
}

// BuildChecks returns the concrete checks for the selected categories,
// preserving the registration order in Builders. An empty selection runs every
// registered category.
func BuildChecks(cfg *stream.Config, selected []Category) []Check {
	want := make(map[Category]bool, len(selected))
	for _, c := range selected {
		want[c] = true
	}
	checks := []Check{}
	for _, b := range Builders {
		if len(want) == 0 || want[b.Category] {
			checks = append(checks, b.Build(cfg)...)
		}
	}
	return checks
}
