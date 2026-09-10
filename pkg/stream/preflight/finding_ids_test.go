// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
)

// findingIDPattern is the shape every finding id must have: lowercase letters
// and digits, grouped by single underscores. It admits no character a value
// read from the database could carry, so an interpolated id fails the test.
var findingIDPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)

func TestFindingIDs_PatternAndUniqueness(t *testing.T) {
	t.Parallel()

	ids := declaredFindingIDs(t)
	require.NotEmpty(t, ids)

	seen := map[string]string{}
	for name, id := range ids {
		require.Regexpf(t, findingIDPattern, id, "finding id %q (%s) does not match the id pattern", id, name)
		other, duplicate := seen[id]
		require.Falsef(t, duplicate, "finding id %q is declared by both %s and %s", id, other, name)
		seen[id] = name
	}
}

// TestFindingIDs_EveryConstructionSiteIsComplete reads the package source and
// asserts that every Finding built by a check declares an id, a title, a detail
// and a message, that the id is a declared constant rather than a computed
// string, and that the title is a plain string literal. The last two rules are
// what keep data read from the database out of ID and Title: neither field can
// interpolate a value it never receives.
func TestFindingIDs_EveryConstructionSiteIsComplete(t *testing.T) {
	t.Parallel()

	declared := declaredFindingIDs(t)
	used := map[string]int{}

	for _, lit := range findingLiterals(t) {
		fields := map[string]ast.Expr{}
		for _, elt := range lit.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			require.True(t, ok, "a Finding literal uses positional fields")
			key, ok := kv.Key.(*ast.Ident)
			require.True(t, ok)
			fields[key.Name] = kv.Value
		}

		for _, field := range []string{"ID", "Title", "Detail", "Message"} {
			value, ok := fields[field]
			require.Truef(t, ok, "a Finding literal does not set %s", field)
			require.NotEmptyf(t, literalText(value), "a Finding literal sets an empty %s", field)
		}

		title, ok := fields["Title"].(*ast.BasicLit)
		require.True(t, ok, "a Finding literal computes its Title instead of writing a string literal")
		require.Equal(t, token.STRING, title.Kind, "a Finding literal's Title is not a string literal")

		id, ok := fields["ID"].(*ast.Ident)
		require.True(t, ok, "a Finding literal computes its ID instead of naming a constant")
		require.Containsf(t, declared, id.Name, "%s is not a declared finding id constant", id.Name)
		used[id.Name]++
	}

	for name := range declared {
		require.Equalf(t, 1, used[name], "%s names %d finding construction sites, expected 1", name, used[name])
	}
}

// TestFindings_SourceDataStaysOutOfIDAndTitle proves a finding keeps the data
// it read from the database in Detail and Message only, so a consumer can count
// findings by ID and show Title without exposing a customer schema.
func TestFindings_SourceDataStaysOutOfIDAndTitle(t *testing.T) {
	t.Parallel()

	const (
		distinctiveSchema   = "customer_billing_eu"
		distinctiveTable    = "invoice_line_item_archive"
		distinctiveColumn   = "settlement_window"
		distinctiveType     = "money_amount_v2"
		distinctiveRole     = "acme_replication_reader"
		distinctiveSequence = "invoice_line_item_archive_id_seq"
	)

	tests := []struct {
		name     string
		check    Check
		wantID   string
		wantData []string
	}{
		{
			name: "replica identity names the table in the detail only",
			check: &ReplicaIdentityCheck{
				Source: replicaIdentitySource(t, []replicaIdentityRow{
					{Schema: distinctiveSchema, Name: distinctiveTable, Relreplident: "d"},
				}),
			},
			wantID:   FindingIDReplicaIdentityNoPrimaryKey,
			wantData: []string{distinctiveSchema, distinctiveTable},
		},
		{
			name: "unsupported column type names the column in the detail only",
			check: &SchemaTypeCompatibilityCheck{
				Source: sourceWithColumns(t, []schemaColumnRow{{
					Schema: distinctiveSchema, Table: distinctiveTable, Column: distinctiveColumn,
					BaseOID: oidUnknown, TypeName: distinctiveType, TypeKind: "c",
				}}),
			},
			wantID:   FindingIDUnsupportedColumnType,
			wantData: []string{distinctiveSchema, distinctiveTable, distinctiveColumn, distinctiveType},
		},
		{
			name: "missing table privilege names the table and role in the detail only",
			check: &SourceTableSelectPrivilegesCheck{
				Source: sourceWithRows(t, []sourceTableSelectPrivilegeRow{
					{Role: distinctiveRole, Schema: distinctiveSchema, Table: distinctiveTable, HasSelect: false},
				}),
			},
			wantID:   FindingIDSourceTableSelectPrivilegeMissing,
			wantData: []string{distinctiveSchema, distinctiveTable, distinctiveRole},
		},
		{
			name: "missing sequence privilege names the sequence in the detail only",
			check: &SourceSequenceSelectPrivilegesCheck{
				Source: sourceWithSequenceRows(t, []sourceSequenceSelectPrivilegeRow{{
					Role: distinctiveRole, TableSchema: distinctiveSchema, Table: distinctiveTable,
					SequenceSchema: distinctiveSchema, Sequence: distinctiveSequence, HasSelect: false,
				}}),
			},
			wantID:   FindingIDSourceSequenceSelectPrivilegeMissing,
			wantData: []string{distinctiveSchema, distinctiveSequence, distinctiveRole},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			findings, err := tc.check.Run(context.Background())
			require.NoError(t, err)
			require.Len(t, findings, 1)

			finding := findings[0]
			require.Equal(t, tc.wantID, finding.ID)
			require.NotEmpty(t, finding.Title)
			require.NotEmpty(t, finding.Detail)
			require.NotEmpty(t, finding.Message)

			for _, data := range tc.wantData {
				require.NotContainsf(t, finding.ID, data, "the id carries source data")
				require.NotContainsf(t, finding.Title, data, "the title carries source data")
				require.Contains(t, finding.Detail, data)
				require.Contains(t, finding.Message, data)
			}
		})
	}
}

// TestFindings_MessageIsStable pins the exact Message of every finding kind
// whose text is deterministic. Message is the line `pgstream check` prints, so
// changing one of these strings changes what a user reads. Detail restates the
// same facts in the prose a consumer's UI renders, and the two are written side
// by side at every construction site — this test is what stops an edit meant
// for Detail from landing in Message unnoticed.
//
// The two connectivity kinds are absent: their Message is a fixed prefix
// followed by the driver's error text, which a unit test cannot pin.
func TestFindings_MessageIsStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantID      string
		wantMessage string
		build       func(t *testing.T) Finding
	}{
		{
			wantID:      FindingIDSourceTableSelectPrivilegeMissing,
			wantMessage: `source role "pgstream_user" lacks SELECT on public.orders; run GRANT SELECT ON TABLE "public"."orders" TO "pgstream_user"`,
			build: func(*testing.T) Finding {
				return sourceTableSelectPrivilegeFinding(sourceTableSelectPrivilegeRow{
					Role: "pgstream_user", Schema: "public", Table: "orders",
				})
			},
		},
		{
			wantID:      FindingIDSourceSequenceSelectPrivilegeMissing,
			wantMessage: `source role "pgstream_user" lacks SELECT on sequence public.orders_id_seq; run GRANT SELECT ON SEQUENCE "public"."orders_id_seq" TO "pgstream_user"`,
			build: func(*testing.T) Finding {
				return sourceSequenceSelectPrivilegeFinding(sourceSequenceSelectPrivilegeRow{
					Role: "pgstream_user", SequenceSchema: "public", Sequence: "orders_id_seq",
				})
			},
		},
		{
			wantID:      FindingIDTargetCreateDBPrivilegeMissing,
			wantMessage: `target role "pgstreamtarget" lacks CREATEDB; run ALTER ROLE "pgstreamtarget" CREATEDB`,
			build:       func(*testing.T) Finding { return targetCreateDBPrivilegeFinding("pgstreamtarget") },
		},
		{
			wantID:      FindingIDTargetCreateRolePrivilegeMissing,
			wantMessage: `target role "pgstreamtarget" lacks CREATEROLE; run ALTER ROLE "pgstreamtarget" CREATEROLE`,
			build:       func(*testing.T) Finding { return targetCreateRolePrivilegeFinding("pgstreamtarget") },
		},
		{
			wantID:      FindingIDReplicaIdentityNoPrimaryKey,
			wantMessage: `"public"."audit_log": REPLICA IDENTITY=default but no PRIMARY KEY; UPDATE/DELETE WAL events will be skipped — add a PRIMARY KEY, set REPLICA IDENTITY FULL, or REPLICA IDENTITY USING INDEX <unique non-partial NOT-NULL index>`,
			build: func(*testing.T) Finding {
				finding, _ := assessReplicaIdentity(replicaIdentityRow{Schema: "public", Name: "audit_log", Relreplident: "d"})
				return finding
			},
		},
		{
			wantID:      FindingIDReplicaIdentityNothing,
			wantMessage: `"public"."events": REPLICA IDENTITY=nothing; UPDATE/DELETE WAL events will be skipped — set REPLICA IDENTITY DEFAULT / FULL / USING INDEX`,
			build: func(*testing.T) Finding {
				finding, _ := assessReplicaIdentity(replicaIdentityRow{Schema: "public", Name: "events", Relreplident: "n", HasPK: true})
				return finding
			},
		},
		{
			wantID:      FindingIDReplicaIdentityIndexUnusable,
			wantMessage: `"public"."t": REPLICA IDENTITY=index but the chosen index is invalid, non-unique, partial, or includes nullable columns — pick a different index or use REPLICA IDENTITY FULL`,
			build: func(*testing.T) Finding {
				finding, _ := assessReplicaIdentity(replicaIdentityRow{Schema: "public", Name: "t", Relreplident: "i"})
				return finding
			},
		},
		{
			wantID:      FindingIDReplicaIdentityUnknown,
			wantMessage: `"public"."t": unknown REPLICA IDENTITY="z" on this Postgres version`,
			build: func(*testing.T) Finding {
				finding, _ := assessReplicaIdentity(replicaIdentityRow{Schema: "public", Name: "t", Relreplident: "z"})
				return finding
			},
		},
		{
			wantID:      FindingIDUnsupportedColumnType,
			wantMessage: `"public"."t"."c": type "geometry" unknown type; Cast the column to a supported type or exclude the table from the migration. To request support, open an issue in the repo: https://github.com/xataio/pgstream/issues/new`,
			build: func(*testing.T) Finding {
				return unsupportedColumnTypeFinding(schemaColumnRow{
					Schema: "public", Table: "t", Column: "c", TypeName: "geometry", TypeKind: "b",
				})
			},
		},
		{
			wantID:      FindingIDUnsupportedRangeType,
			wantMessage: `"public"."t"."c": range type "numrange" unknown type; pgstream only encodes int4range, int8range, and tstzrange values. Cast the column to a supported type or exclude the table from the migration. To request support, open an issue in the repo: https://github.com/xataio/pgstream/issues/new`,
			build: func(*testing.T) Finding {
				finding, _ := unsupportedRangeTypeFinding(schemaColumnRow{
					Schema: "public", Table: "t", Column: "c", TypeName: "numrange", TypeKind: "r",
				})
				return finding
			},
		},
		{
			wantID:      FindingIDTargetExtensionMissing,
			wantMessage: `1 extension installed on source but missing on the target: "postgis" (source schema "public"); run the following on the target before migrating, or the schema will fail to apply: CREATE EXTENSION IF NOT EXISTS "postgis";`,
			build: func(*testing.T) Finding {
				return missingExtensionsFinding([]missingExtension{{name: "postgis", schema: "public"}})
			},
		},
		{
			wantID:      FindingIDWALLevelNotLogical,
			wantMessage: `wal_level="replica" on source; set wal_level=logical in postgresql.conf and restart for logical replication`,
			build: func(t *testing.T) Finding {
				return singleFinding(t, &WALLevelCheck{Source: queryRowSource(t, func(t *testing.T, dest []any) {
					level, ok := dest[0].(*string)
					require.True(t, ok)
					*level = "replica"
				})})
			},
		},
		{
			wantID:      FindingIDWAL2JSONUnavailable,
			wantMessage: `wal2json output plugin not available on source; install the wal2json package, and on postgres 17.11+ add wal2json to output_plugin_libraries (it defaults to "pgoutput, test_decoding") and reload the server — that allowlist is checked before the library is loaded, so an installed wal2json is still refused while it is missing from it`,
			build: func(t *testing.T) Finding {
				return singleFinding(t, &WAL2JSONCheck{Source: func(context.Context) (postgres.Querier, error) {
					return &mocks.Querier{
						QueryRowFn: func(context.Context, []any, string, ...any) error {
							return errors.New(`could not access file "wal2json"`)
						},
					}, nil
				}})
			},
		},
		{
			wantID:      FindingIDReplicationSlotHeadroomExhausted,
			wantMessage: "no replication slot headroom: 10/10 slots in use; raise max_replication_slots (requires restart) or drop unused slots",
			build: func(t *testing.T) Finding {
				return singleFinding(t, &ReplicationSlotHeadroomCheck{Source: queryRowSource(t, func(t *testing.T, dest []any) {
					maxSlots, ok := dest[0].(*int)
					require.True(t, ok)
					usedSlots, ok := dest[1].(*int)
					require.True(t, ok)
					*maxSlots, *usedSlots = 10, 10
				})})
			},
		},
		{
			wantID:      FindingIDReplicationRoleAttributeMissing,
			wantMessage: `source role "pgstream_user" lacks the REPLICATION attribute; run ALTER ROLE pgstream_user REPLICATION as a superuser`,
			build: func(t *testing.T) Finding {
				return singleFinding(t, &ReplicationRoleAttrCheck{Source: queryRowSource(t, func(t *testing.T, dest []any) {
					roleName, ok := dest[0].(*string)
					require.True(t, ok)
					hasReplication, ok := dest[1].(*bool)
					require.True(t, ok)
					*roleName, *hasReplication = "pgstream_user", false
				})})
			},
		},
		{
			wantID:      FindingIDSnapshotConnectionHeadroomInsufficient,
			wantMessage: "snapshot needs 16 concurrent connections (snapshot_workers × table_workers) but source has only 7 available (max_connections=100, superuser_reserved_connections=3, 90 in use); lower snapshot_workers/table_workers, raise max_connections (requires restart), or reduce existing connections",
			build: func(t *testing.T) Finding {
				return singleFinding(t, &SnapshotConnectionsCheck{Source: sourceWithConnLimits(t, 100, 3, 90), Demand: 16})
			},
		},
		{
			wantID:      FindingIDSourceMultipleInstances,
			wantMessage: "source appears to be load-balanced across multiple Postgres instances: an exported snapshot was not visible on 3 of 8 probe connections. Parallel data snapshotting requires every connection to reach the same instance — point the source at a single-instance / writer endpoint (Aurora/RDS reader endpoints and instance-spanning poolers are unsupported for snapshots).",
			build: func(t *testing.T) Finding {
				return singleFinding(t, &SourceSnapshotInstanceCheck{
					Probe:  func(context.Context, int) (int, error) { return 3, nil },
					Probes: 8,
				})
			},
		},
		{
			wantID:      FindingIDTargetVersionOlderThanSource,
			wantMessage: "source is PostgreSQL 18.4, target is PostgreSQL 17.4; restoring a dump from a newer server into an older one is unsupported and may fail. Use a target running PostgreSQL 18 or newer.",
			build: func(t *testing.T) Finding {
				return singleFinding(t, &PostgresVersionCheck{Source: versionConn(t, 18), Target: versionConn(t, 17)})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.wantID, func(t *testing.T) {
			t.Parallel()

			finding := tc.build(t)
			require.Equal(t, tc.wantID, finding.ID)
			require.Equal(t, tc.wantMessage, finding.Message)
		})
	}
}

// singleFinding runs a check and returns the one finding it is expected to
// report.
func singleFinding(t *testing.T, check Check) Finding {
	t.Helper()

	findings, err := check.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, findings, 1)
	return findings[0]
}

// queryRowSource returns an AcquireFunc whose Querier answers every QueryRow by
// letting fill populate the scan destinations, for the checks that read a
// single row of settings.
func queryRowSource(t *testing.T, fill func(t *testing.T, dest []any)) postgres.AcquireFunc {
	return func(context.Context) (postgres.Querier, error) {
		return &mocks.Querier{
			QueryRowFn: func(_ context.Context, dest []any, _ string, _ ...any) error {
				fill(t, dest)
				return nil
			},
		}, nil
	}
}

// declaredFindingIDs returns every finding id constant declared in
// finding_ids.go, keyed by constant name.
func declaredFindingIDs(t *testing.T) map[string]string {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), "finding_ids.go", nil, 0)
	require.NoError(t, err)

	ids := map[string]string{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			require.True(t, ok)
			require.Len(t, value.Values, 1)
			lit, ok := value.Values[0].(*ast.BasicLit)
			require.True(t, ok, "a finding id constant is not a string literal")
			id, err := strconv.Unquote(lit.Value)
			require.NoError(t, err)
			ids[value.Names[0].Name] = id
		}
	}
	return ids
}

// findingLiterals returns every non-empty Finding composite literal in the
// package's non-test sources. An empty literal is the "no finding" sentinel a
// helper returns beside a false result, so it is skipped.
func findingLiterals(t *testing.T) []*ast.CompositeLit {
	t.Helper()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	var literals []*ast.CompositeLit
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		require.NoError(t, err)

		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok || len(lit.Elts) == 0 {
				return true
			}
			switch typ := lit.Type.(type) {
			case *ast.Ident:
				if typ.Name == "Finding" {
					literals = append(literals, lit)
				}
			case *ast.ArrayType:
				elem, ok := typ.Elt.(*ast.Ident)
				if !ok || elem.Name != "Finding" {
					return true
				}
				for _, elt := range lit.Elts {
					inner, ok := elt.(*ast.CompositeLit)
					if ok && inner.Type == nil && len(inner.Elts) > 0 {
						literals = append(literals, inner)
					}
				}
			}
			return true
		})
	}
	return literals
}

// literalText returns the text of a string literal expression, and the empty
// string for anything else. A composed expression (fmt.Sprintf, concatenation)
// therefore reads as non-empty, which is what the completeness test needs.
func literalText(expr ast.Expr) string {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "composed"
	}
	text, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "composed"
	}
	return text
}
