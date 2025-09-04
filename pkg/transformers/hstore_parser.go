// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

func parseHstore(s string) (pgtype.Hstore, error) {
	p := newHSP(s)

	// This is an over-estimate of the number of key/value pairs. Use '>' because I am guessing it
	// is less likely to occur in keys/values than '=' or ','.
	numPairsEstimate := strings.Count(s, ">")
	// makes one allocation of strings for the entire Hstore, rather than one allocation per value.
	valueStrings := make([]string, 0, numPairsEstimate)
	result := make(pgtype.Hstore, numPairsEstimate)
	first := true
	for !p.atEnd() {
		if !first {
			err := p.consumePairSeparator()
			if err != nil {
				return nil, err
			}
		} else {
			first = false
		}

		err := p.consumeExpectedByte('"')
		if err != nil {
			return nil, err
		}

		key, err := p.consumeDoubleQuoted()
		if err != nil {
			return nil, err
		}

		err = p.consumeKVSeparator()
		if err != nil {
			return nil, err
		}

		value, err := p.consumeDoubleQuotedOrNull()
		if err != nil {
			return nil, err
		}
		if value.Valid {
			valueStrings = append(valueStrings, value.String)
			result[key] = &valueStrings[len(valueStrings)-1]
		} else {
			result[key] = nil
		}
	}

	return result, nil
}

type hstoreParser struct {
	str           string
	pos           int
	nextBackslash int
}

func newHSP(in string) *hstoreParser {
	return &hstoreParser{
		pos:           0,
		str:           in,
		nextBackslash: strings.IndexByte(in, '\\'),
	}
}

func (p *hstoreParser) atEnd() bool {
	return p.pos >= len(p.str)
}

// consume returns the next byte of the string, or end if the string is done.
func (p *hstoreParser) consume() (b byte, end bool) {
	if p.pos >= len(p.str) {
		return 0, true
	}
	b = p.str[p.pos]
	p.pos++
	return b, false
}

func unexpectedByteErr(actualB byte, expectedB byte) error {
	return fmt.Errorf("expected '%c' ('%#v'); found '%c' ('%#v')", expectedB, expectedB, actualB, actualB)
}

// consumeExpectedByte consumes expectedB from the string, or returns an error.
func (p *hstoreParser) consumeExpectedByte(expectedB byte) error {
	nextB, end := p.consume()
	if end {
		return fmt.Errorf("expected '%c' ('%#v'); found end", expectedB, expectedB)
	}
	if nextB != expectedB {
		return unexpectedByteErr(nextB, expectedB)
	}
	return nil
}

// consumeExpected2 consumes two expected bytes or returns an error.
// This was a bit faster than using a string argument (better inlining? Not sure).
func (p *hstoreParser) consumeExpected2(one byte, two byte) error {
	if p.pos+2 > len(p.str) {
		return fmt.Errorf("unexpected end of string")
	}
	if p.str[p.pos] != one {
		return unexpectedByteErr(p.str[p.pos], one)
	}
	if p.str[p.pos+1] != two {
		return unexpectedByteErr(p.str[p.pos+1], two)
	}
	p.pos += 2
	return nil
}

var errEOSInQuoted = errors.New(`found end before closing double-quote ('"')`)

// consumeDoubleQuoted consumes a double-quoted string from p. The double quote must have been
// parsed already. This copies the string from the backing string so it can be garbage collected.
func (p *hstoreParser) consumeDoubleQuoted() (string, error) {
	// fast path: assume most keys/values do not contain escapes
	nextDoubleQuote := strings.IndexByte(p.str[p.pos:], '"')
	if nextDoubleQuote == -1 {
		return "", errEOSInQuoted
	}
	nextDoubleQuote += p.pos
	if p.nextBackslash == -1 || p.nextBackslash > nextDoubleQuote {
		// clone the string from the source string to ensure it can be garbage collected separately
		// TODO: use strings.Clone on Go 1.20; this could get optimized away
		s := strings.Clone(p.str[p.pos:nextDoubleQuote])
		p.pos = nextDoubleQuote + 1
		return s, nil
	}

	// slow path: string contains escapes
	s, err := p.consumeDoubleQuotedWithEscapes(p.nextBackslash)
	p.nextBackslash = strings.IndexByte(p.str[p.pos:], '\\')
	if p.nextBackslash != -1 {
		p.nextBackslash += p.pos
	}
	return s, err
}

// consumeDoubleQuotedWithEscapes consumes a double-quoted string containing escapes, starting
// at p.pos, and with the first backslash at firstBackslash. This copies the string so it can be
// garbage collected separately.
func (p *hstoreParser) consumeDoubleQuotedWithEscapes(firstBackslash int) (string, error) {
	// copy the prefix that does not contain backslashes
	var builder strings.Builder
	builder.WriteString(p.str[p.pos:firstBackslash])

	// skip to the backslash
	p.pos = firstBackslash

	// copy bytes until the end, unescaping backslashes
	for {
		nextB, end := p.consume()
		if end {
			return "", errEOSInQuoted
		} else if nextB == '"' {
			break
		} else if nextB == '\\' {
			// escape: skip the backslash and copy the char
			nextB, end = p.consume()
			if end {
				return "", errEOSInQuoted
			}
			if nextB != '\\' && nextB != '"' {
				return "", fmt.Errorf("unexpected escape in quoted string: found '%#v'", nextB)
			}
			builder.WriteByte(nextB)
		} else {
			// normal byte: copy it
			builder.WriteByte(nextB)
		}
	}
	return builder.String(), nil
}

// consumePairSeparator consumes the Hstore pair separator ", " or returns an error.
func (p *hstoreParser) consumePairSeparator() error {
	return p.consumeExpected2(',', ' ')
}

// consumeKVSeparator consumes the Hstore key/value separator "=>" or returns an error.
func (p *hstoreParser) consumeKVSeparator() error {
	return p.consumeExpected2('=', '>')
}

// consumeDoubleQuotedOrNull consumes the Hstore key/value separator "=>" or returns an error.
func (p *hstoreParser) consumeDoubleQuotedOrNull() (pgtype.Text, error) {
	// peek at the next byte
	if p.atEnd() {
		return pgtype.Text{}, errors.New("found end instead of value")
	}
	next := p.str[p.pos]
	if next == 'N' {
		// must be the exact string NULL: use consumeExpected2 twice
		err := p.consumeExpected2('N', 'U')
		if err != nil {
			return pgtype.Text{}, err
		}
		err = p.consumeExpected2('L', 'L')
		if err != nil {
			return pgtype.Text{}, err
		}
		return pgtype.Text{String: "", Valid: false}, nil
	} else if next != '"' {
		return pgtype.Text{}, unexpectedByteErr(next, '"')
	}

	// skip the double quote
	p.pos += 1
	s, err := p.consumeDoubleQuoted()
	if err != nil {
		return pgtype.Text{}, err
	}
	return pgtype.Text{String: s, Valid: true}, nil
}
