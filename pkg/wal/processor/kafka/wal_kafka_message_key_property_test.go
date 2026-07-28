// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"fmt"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/xataio/pgstream/pkg/wal"
)

// Property-based test for the primary key message key encoding.
//
// The message key decides partition placement, so the encoding has to be
// injective: two row identities differing in any component must never produce
// the same key. When they collide the rows share a partition and lose their
// separate ordering guarantees, and on a compacted topic one silently shadows
// the other.
//
// The property checked here is that the encoding round-trips — decoding a key
// recovers exactly the schema, table and values it was built from. That is
// strictly stronger than injectivity (a decodable encoding cannot map two
// inputs to one output) and, unlike comparing two generated identities, it
// fails on a single input rather than needing the generator to stumble onto a
// colliding pair. It catches any transformation that loses information, which
// includes both the delimiter ambiguity and lossy re-encoding of bytes that
// are not valid UTF-8.

// keyComponent generates strings biased towards the characters that carry
// structural meaning in the encoding, plus an invalid UTF-8 byte, so that
// collisions are actually reachable rather than astronomically unlikely.
func keyComponent(t *rapid.T, label string) string {
	alphabet := []string{"a", "b", ",", `\`, ".", ":", "\xff", "\xfe", "é", ""}
	return rapid.Map(
		rapid.SliceOfN(rapid.SampledFrom(alphabet), 0, 6),
		func(parts []string) string { return strings.Join(parts, "") },
	).Draw(t, label)
}

type rowIdentity struct {
	schema string
	table  string
	values []string
}

func (r rowIdentity) messageKey() string {
	cols := make([]wal.Column, 0, len(r.values))
	colIDs := make([]string, 0, len(r.values))
	for i, v := range r.values {
		id := fmt.Sprintf("col-%d", i)
		cols = append(cols, wal.Column{ID: id, Name: id, Type: "text", Value: v})
		colIDs = append(colIDs, id)
	}

	return string(primaryKeyMessageKey(&wal.Data{
		Schema:   r.schema,
		Table:    r.table,
		Columns:  cols,
		Metadata: wal.Metadata{InternalColIDs: colIDs},
	}))
}

// decodeMessageKey is the inverse of the encoding in primaryKeyMessageKey. It
// exists only in the test: nothing in production decodes a message key, but
// being able to decode one is what proves the encoding loses no information.
//
// It scans byte-wise, treating "\" as escaping whatever follows: the schema
// ends at the first unescaped ".", the table at the first unescaped ":", and
// the remaining values are separated by unescaped ",".
func decodeMessageKey(key string) (rowIdentity, error) {
	var (
		id       rowIdentity
		current  []byte
		haveSep  bool
		haveCol  bool
		decoded  []string
		escaping bool
	)

	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case escaping:
			current = append(current, c)
			escaping = false
		case c == '\\':
			escaping = true
		case c == '.' && !haveSep:
			id.schema = string(current)
			current, haveSep = nil, true
		case c == ':' && haveSep && !haveCol:
			id.table = string(current)
			current, haveCol = nil, true
		case c == ',' && haveCol:
			decoded = append(decoded, string(current))
			current = nil
		default:
			current = append(current, c)
		}
	}

	if escaping {
		return id, fmt.Errorf("key ends with a dangling escape character: %q", key)
	}
	if !haveSep || !haveCol {
		return id, fmt.Errorf("key is missing the schema/table/value separators: %q", key)
	}

	// the trailing component is not followed by a separator, so it is only
	// flushed here
	decoded = append(decoded, string(current))
	id.values = decoded
	return id, nil
}

func (r rowIdentity) equal(other rowIdentity) bool {
	if r.schema != other.schema || r.table != other.table || len(r.values) != len(other.values) {
		return false
	}
	for i, v := range r.values {
		if v != other.values[i] {
			return false
		}
	}
	return true
}

func genRowIdentity(t *rapid.T, label string) rowIdentity {
	// arity varies so the encoding is also exercised across a primary key
	// definition changing under DDL, not just within one fixed table
	arity := rapid.IntRange(1, 3).Draw(t, label+"-arity")
	values := make([]string, 0, arity)
	for i := range arity {
		values = append(values, keyComponent(t, fmt.Sprintf("%s-value-%d", label, i)))
	}

	return rowIdentity{
		schema: keyComponent(t, label+"-schema"),
		table:  keyComponent(t, label+"-table"),
		values: values,
	}
}

func TestPrimaryKeyMessageKey_RoundTrips(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		want := genRowIdentity(t, "row")
		key := want.messageKey()

		got, err := decodeMessageKey(key)
		if err != nil {
			t.Fatalf("decoding key %q built from %#v: %v", key, want, err)
		}

		if !got.equal(want) {
			t.Fatalf("key %q does not round-trip:\n  encoded %#v\n  decoded %#v", key, want, got)
		}
	})
}

// TestPrimaryKeyMessageKey_Injective checks injectivity directly on generated
// pairs. Round-tripping already implies it, so this is a cheap backstop against
// the decoder and the encoder being wrong in the same direction.
func TestPrimaryKeyMessageKey_Injective(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		a := genRowIdentity(t, "a")
		b := genRowIdentity(t, "b")

		sameKey := a.messageKey() == b.messageKey()
		sameIdentity := a.equal(b)

		if sameKey != sameIdentity {
			t.Fatalf("injectivity violated:\n  a = %#v -> %q\n  b = %#v -> %q\n  same key = %v, same identity = %v",
				a, a.messageKey(), b, b.messageKey(), sameKey, sameIdentity)
		}
	})
}
