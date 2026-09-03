// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseDump_exoticShapes runs a real pg_dump --schema-only output through
// the parser and the block partitioning that the concurrent index restore
// depends on. The fixture is captured verbatim from postgres and covers the
// shapes whose classification is not obvious from the code: an exclusion
// constraint, a partial index with a multi-condition WHERE clause, a long
// expression index, a constraint whose index is reused (UNIQUE), CLUSTER ON,
// a table partition attachment and index partition attachments.
//
// Two properties matter for the parallel restore and neither is visible from
// the parser alone: a statement must land in the group that can run it, and no
// statement may be dropped or merged into another on the way there.
func TestParseDump_exoticShapes(t *testing.T) {
	t.Parallel()

	dump, err := os.ReadFile("testdata/exotic_schema_dump.sql")
	require.NoError(t, err)

	sg := SnapshotGenerator{objectTypeFilter: &objectTypeFilter{}}
	parsed := sg.parseDump(dump)
	connectBlocks, indexBlocks, otherBlocks := partitionDumpBlocks(parsed.indicesAndConstraints, isIndexStatement)
	require.Empty(t, connectBlocks)

	// pg_dump emits every CREATE INDEX on a single line, however long the
	// expression or WHERE clause, so each one becomes its own block and can be
	// restored on its own connection
	require.ElementsMatch(t, []string{
		"CREATE INDEX parts_val_idx ON ONLY public.parts USING btree (val);",
		"CREATE INDEX parts_0_val_idx ON public.parts_0 USING btree (val);",
		"CREATE INDEX parts_1_val_idx ON public.parts_1 USING btree (val);",
		"CREATE INDEX rooms_expr_idx ON public.rooms USING btree (lower(name), upper(email), ((val * (2)::numeric)), COALESCE(name, email, 'a-fairly-long-default-value'::text));",
		"CREATE INDEX rooms_partial_idx ON public.rooms USING btree (name) WHERE ((val > (100)::numeric) AND (name IS NOT NULL) AND (email IS NOT NULL));",
	}, indexBlocks)

	// everything that builds its own index or references one has to wait for
	// the index phase, and keeps its relative order
	require.Equal(t, []string{
		"ALTER TABLE ONLY public.rooms\n    ADD CONSTRAINT rooms_email_key UNIQUE (email);",
		"ALTER TABLE public.rooms CLUSTER ON rooms_email_key;",
		"ALTER TABLE ONLY public.rooms\n    ADD CONSTRAINT rooms_no_overlap EXCLUDE USING gist (room WITH =, during WITH &&);",
		"COMMENT ON INDEX public.rooms_partial_idx IS 'partial';",
		"ALTER INDEX public.parts_val_idx ATTACH PARTITION public.parts_0_val_idx;",
		"ALTER INDEX public.parts_val_idx ATTACH PARTITION public.parts_1_val_idx;",
	}, otherBlocks)

	// a table partition attachment is not an index dependency and stays in the
	// schema dump, which is restored before the data
	require.Contains(t, string(parsed.filtered), "ALTER TABLE ONLY public.parts ATTACH PARTITION public.parts_0 FOR VALUES FROM (0) TO (10);")

	// no index or constraint statement may be lost or glued to its neighbour
	// on the way into a group. Statements outside that domain are excluded:
	// the parser deliberately drops some of them (ownership of excluded roles,
	// legacy PL/pgSQL handlers, filtered security labels).
	restored := string(parsed.filtered) + string(parsed.indicesAndConstraints) + string(parsed.views)
	for _, line := range strings.Split(string(dump), "\n") {
		line = strings.TrimSpace(line)
		if !isIndexStatement(line) && !strings.HasPrefix(line, "ADD CONSTRAINT") &&
			!isAttachPartitionIndexStatement(line) && !strings.HasPrefix(line, "COMMENT ON INDEX") &&
			!isClusterOnAlterTable(line) {
			continue
		}
		require.Contains(t, restored, line, "statement dropped by the dump parsing")
	}
}

// TestParseDump_singleLineAddConstraint covers the shape the block splitting
// would be most sensitive to: an ALTER TABLE ... ADD CONSTRAINT on one line.
// Real pg_dump wraps these onto two lines (see the fixture above), so this
// exercises the defensive branch rather than an observed output, and pins that
// consecutive constraints stay separate blocks instead of merging into one.
func TestParseDump_singleLineAddConstraint(t *testing.T) {
	t.Parallel()

	sg := SnapshotGenerator{objectTypeFilter: &objectTypeFilter{}}
	parsed := sg.parseDump([]byte(
		"ALTER TABLE public.a ADD CONSTRAINT a_check CHECK (v > 0);\n" +
			"ALTER TABLE public.b ADD CONSTRAINT b_check CHECK (v > 0);\n" +
			"CREATE INDEX a_idx ON public.a USING btree (v);\n"))

	_, indexBlocks, otherBlocks := partitionDumpBlocks(parsed.indicesAndConstraints, isIndexStatement)
	require.Equal(t, []string{"CREATE INDEX a_idx ON public.a USING btree (v);"}, indexBlocks)
	require.Equal(t, []string{
		"ALTER TABLE public.a ADD CONSTRAINT a_check CHECK (v > 0);",
		"ALTER TABLE public.b ADD CONSTRAINT b_check CHECK (v > 0);",
	}, otherBlocks)
}
