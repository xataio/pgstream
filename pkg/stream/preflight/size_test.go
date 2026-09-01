// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPrettySize pins prettySize to pg_size_pretty. The expectations were read
// off a live PostgreSQL 16 (SELECT pg_size_pretty(b::bigint)), so a drift in
// the Go implementation shows up as a mismatch with the server that formats the
// per-row strings printed beside these totals.
func TestPrettySize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		bytes int64
		want  string
	}{
		{-10485760, "-10 MB"},
		{-1024, "-1024 bytes"},
		{0, "0 bytes"},
		{1, "1 bytes"},
		{512, "512 bytes"},
		{1023, "1023 bytes"},
		{1024, "1024 bytes"},
		{10239, "10239 bytes"},
		{10240, "10 kB"},
		{10241, "10 kB"},
		{524288, "512 kB"},
		{1048576, "1024 kB"},
		{1384448, "1352 kB"},
		{2752512, "2688 kB"},
		{4857856, "4744 kB"},
		{10485759, "10 MB"},
		{10485760, "10 MB"},
		{10485761, "10 MB"},
		{1073741824, "1024 MB"},
		{5000000000, "4768 MB"},
		{10737418239, "10 GB"},
		{10737418240, "10 GB"},
		{1099511627776, "1024 GB"},
		{10995116277760, "10 TB"},
		{10995116277761, "10 TB"},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprint(tc.bytes), func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, prettySize(tc.bytes))
		})
	}
}
