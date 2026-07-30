// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestWriteCopyTextValue_EmptyStringRepro(t *testing.T) {
	tm := pgtype.NewMap()
	buf := &bytes.Buffer{}

	cases := []struct {
		name string
		oid  uint32
		v    any
		want string
	}{
		{"empty string text", pgtype.TextOID, "", ""},
		{"empty string varchar", pgtype.VarcharOID, "", ""},
		{"empty byte slice text col", pgtype.TextOID, []byte{}, ""},
		{"nil", pgtype.TextOID, nil, `\N`},
		{"typed nil byte slice", pgtype.ByteaOID, []byte(nil), `\N`},
		{"invalid pgtype.Text", pgtype.TextOID, pgtype.Text{Valid: false}, `\N`},
		{"non-empty", pgtype.TextOID, "x", "x"},
		{"empty bytea", pgtype.ByteaOID, []byte{}, `\\x`},
	}
	for _, c := range cases {
		buf.Reset()
		if err := writeCopyTextValue(buf, tm, c.oid, c.v); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if buf.String() != c.want {
			t.Errorf("%s: got %q, want %q", c.name, buf.String(), c.want)
		}
	}
}
