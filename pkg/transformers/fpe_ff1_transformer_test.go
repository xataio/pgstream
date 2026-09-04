// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// a fixed test key of 128 bits: the bytes 0x00 to 0x0f
const testFPEFF1KeyHex = "000102030405060708090a0b0c0d0e0f"

func TestNewFPEFF1Transformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  ParameterValues
		wantErr error
	}{
		{
			name:    "ok - valid key",
			params:  ParameterValues{"key_hex": testFPEFF1KeyHex},
			wantErr: nil,
		},
		{
			name: "ok - all parameters",
			params: ParameterValues{
				"key_hex":         testFPEFF1KeyHex,
				"associated_data": "public.persons.phone_number",
				"alphabet":        "alphanumeric",
				"passthrough":     "error",
				"keep_prefix":     2,
				"keep_suffix":     1,
				"min_length":      8,
			},
			wantErr: nil,
		},
		{
			name:    "ok - 192 bit key",
			params:  ParameterValues{"key_hex": strings.Repeat("ab", 24)},
			wantErr: nil,
		},
		{
			name:    "ok - 256 bit key",
			params:  ParameterValues{"key_hex": strings.Repeat("ab", 32)},
			wantErr: nil,
		},
		{
			name:    "ok - literal alphabet",
			params:  ParameterValues{"key_hex": testFPEFF1KeyHex, "alphabet": "ABCDEF"},
			wantErr: nil,
		},
		{
			name:    "error - key_hex missing",
			params:  ParameterValues{},
			wantErr: errFPEFF1KeyNotFound,
		},
		{
			name:    "error - key_hex not a string",
			params:  ParameterValues{"key_hex": 123},
			wantErr: ErrInvalidParameters,
		},
		{
			name:    "error - key_hex not valid hex",
			params:  ParameterValues{"key_hex": strings.Repeat("zx", 16)},
			wantErr: errFPEFF1KeyInvalid,
		},
		{
			name:    "error - key_hex wrong length",
			params:  ParameterValues{"key_hex": strings.Repeat("ab", 20)},
			wantErr: errFPEFF1KeyInvalid,
		},
		{
			name:    "error - associated_data not a string",
			params:  ParameterValues{"key_hex": testFPEFF1KeyHex, "associated_data": 1},
			wantErr: ErrInvalidParameters,
		},
		{
			name:    "error - alphabet too small",
			params:  ParameterValues{"key_hex": testFPEFF1KeyHex, "alphabet": "0"},
			wantErr: errFPEFF1AlphabetTooSmall,
		},
		{
			name:    "error - alphabet with duplicate characters",
			params:  ParameterValues{"key_hex": testFPEFF1KeyHex, "alphabet": "01230"},
			wantErr: errFPEFF1AlphabetDuplicate,
		},
		{
			name:    "ok - preserve_from",
			params:  ParameterValues{"key_hex": testFPEFF1KeyHex, "alphabet": "alphanumeric", "preserve_from": "@"},
			wantErr: nil,
		},
		{
			name:    "error - preserve_from overlaps the alphabet",
			params:  ParameterValues{"key_hex": testFPEFF1KeyHex, "alphabet": "alphanumeric", "preserve_from": "x"},
			wantErr: errFPEFF1PreserveFromInAlphabet,
		},
		{
			name:    "error - preserve_from not a string",
			params:  ParameterValues{"key_hex": testFPEFF1KeyHex, "preserve_from": 1},
			wantErr: ErrInvalidParameters,
		},
		{
			name:    "error - unknown passthrough",
			params:  ParameterValues{"key_hex": testFPEFF1KeyHex, "passthrough": "skip"},
			wantErr: errFPEFF1PassthroughValue,
		},
		{
			name:    "error - negative keep_prefix",
			params:  ParameterValues{"key_hex": testFPEFF1KeyHex, "keep_prefix": -1},
			wantErr: errFPEFF1KeepNegative,
		},
		{
			name:    "error - negative keep_suffix",
			params:  ParameterValues{"key_hex": testFPEFF1KeyHex, "keep_suffix": -1},
			wantErr: errFPEFF1KeepNegative,
		},
		{
			name:    "error - keep_prefix not an integer",
			params:  ParameterValues{"key_hex": testFPEFF1KeyHex, "keep_prefix": "2"},
			wantErr: ErrInvalidParameters,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformer, err := NewFPEFF1Transformer(tc.params)
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr != nil {
				return
			}
			require.Equal(t, FPEFF1, transformer.Type())
			require.False(t, transformer.IsDynamic())
			require.Equal(t, UniquenessPreserved, transformer.Uniqueness())
			require.Equal(t, fpeFF1CompatibleTypes, transformer.CompatibleTypes())
			require.NoError(t, transformer.Close())
		})
	}
}

// min_length increases the minimum that FF1 imposes. it cannot decrease it,
// because an attacker can try all values of a smaller set
func TestFPEFF1Transformer_minLength(t *testing.T) {
	t.Parallel()

	require.Equal(t, 6, fpeFF1SecureMinLength(10))
	require.Equal(t, 4, fpeFF1SecureMinLength(52))
	require.Equal(t, 4, fpeFF1SecureMinLength(62))

	t.Run("error - below the FF1 floor", func(t *testing.T) {
		t.Parallel()

		_, err := NewFPEFF1Transformer(ParameterValues{"key_hex": testFPEFF1KeyHex, "min_length": 5})
		require.ErrorContains(t, err, "min_length 5 is below the 6 characters")
	})

	t.Run("ok - defaults to the FF1 floor", func(t *testing.T) {
		t.Parallel()

		transformer, err := NewFPEFF1Transformer(ParameterValues{"key_hex": testFPEFF1KeyHex})
		require.NoError(t, err)
		require.Equal(t, 6, transformer.minLength)
	})

	t.Run("error - raised floor rejects a shorter value", func(t *testing.T) {
		t.Parallel()

		transformer, err := NewFPEFF1Transformer(ParameterValues{"key_hex": testFPEFF1KeyHex, "min_length": 9})
		require.NoError(t, err)

		_, err = transformer.Transform(context.Background(), NewValue("12345678", "text", nil))
		require.ErrorIs(t, err, errFPEFF1ValueTooShort)

		got, err := transformer.Transform(context.Background(), NewValue("123456789", "text", nil))
		require.NoError(t, err)
		require.Len(t, got, 9)
	})
}

func TestFPEFF1Transformer_Transform(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		params   ParameterValues
		input    any
		validate func(*testing.T, string)
		wantErr  error
	}{
		{
			name:   "ok - digits keep their format",
			params: ParameterValues{"key_hex": testFPEFF1KeyHex},
			input:  "1234567890",
			validate: func(t *testing.T, got string) {
				require.Len(t, got, 10)
				require.NotEqual(t, "1234567890", got)
				for _, r := range got {
					require.Contains(t, fpeFF1AlphabetDigits, string(r))
				}
			},
		},
		{
			name: "ok - prefix and non alphabet characters survive",
			params: ParameterValues{
				"key_hex":     testFPEFF1KeyHex,
				"keep_prefix": 5,
			},
			input: "+36301234567",
			validate: func(t *testing.T, got string) {
				require.Len(t, got, len("+36301234567"))
				require.True(t, strings.HasPrefix(got, "+3630"))
				require.NotEqual(t, "+36301234567", got)
			},
		},
		{
			name: "ok - suffix is preserved",
			params: ParameterValues{
				"key_hex":     testFPEFF1KeyHex,
				"keep_suffix": 3,
			},
			input: "1234567890",
			validate: func(t *testing.T, got string) {
				require.True(t, strings.HasSuffix(got, "890"))
				require.NotEqual(t, "1234567890", got)
			},
		},
		{
			name: "ok - separators keep their positions",
			params: ParameterValues{
				"key_hex": testFPEFF1KeyHex,
			},
			input: "123-456-7890",
			validate: func(t *testing.T, got string) {
				require.Len(t, got, len("123-456-7890"))
				require.Equal(t, "-", string(got[3]))
				require.Equal(t, "-", string(got[7]))
			},
		},
		{
			name: "ok - letters alphabet",
			params: ParameterValues{
				"key_hex":  testFPEFF1KeyHex,
				"alphabet": "letters",
			},
			input: "Acme Trading",
			validate: func(t *testing.T, got string) {
				require.Len(t, got, len("Acme Trading"))
				require.Equal(t, " ", string(got[4]))
				require.NotEqual(t, "Acme Trading", got)
			},
		},
		{
			name: "ok - multibyte alphabet",
			params: ParameterValues{
				"key_hex":  testFPEFF1KeyHex,
				"alphabet": "áéíóúüőű",
			},
			input: "áéíóúüőű",
			validate: func(t *testing.T, got string) {
				require.Equal(t, 8, len([]rune(got)))
				require.NotEqual(t, "áéíóúüőű", got)
				for _, r := range got {
					require.Contains(t, "áéíóúüőű", string(r))
				}
			},
		},
		{
			name: "ok - preserve_from keeps the domain",
			params: ParameterValues{
				"key_hex":       testFPEFF1KeyHex,
				"alphabet":      "alphanumeric",
				"preserve_from": "@",
			},
			input: "john.doe@example.com",
			validate: func(t *testing.T, got string) {
				require.Len(t, got, len("john.doe@example.com"))
				require.True(t, strings.HasSuffix(got, "@example.com"))
				require.NotEqual(t, "john.doe@example.com", got)
			},
		},
		{
			name: "ok - preserve_from keeps only the tld",
			params: ParameterValues{
				"key_hex":       testFPEFF1KeyHex,
				"alphabet":      "alphanumeric",
				"preserve_from": ".",
			},
			input: "john.doe@example.com",
			validate: func(t *testing.T, got string) {
				require.Len(t, got, len("john.doe@example.com"))
				require.True(t, strings.HasSuffix(got, ".com"))
				// the transformer still encrypts the domain name
				require.NotContains(t, got, "example")
				require.Equal(t, "@", string(got[8]))
			},
		},
		{
			name: "ok - preserve_from absent from the value encrypts everything",
			params: ParameterValues{
				"key_hex":       testFPEFF1KeyHex,
				"alphabet":      "alphanumeric",
				"preserve_from": "@",
			},
			input: "notanemail",
			validate: func(t *testing.T, got string) {
				require.Len(t, got, len("notanemail"))
				require.NotEqual(t, "notanemail", got)
				require.NotContains(t, got, "@")
			},
		},
		{
			name: "error - preserve_from leaves too little to encrypt",
			params: ParameterValues{
				"key_hex":       testFPEFF1KeyHex,
				"alphabet":      "alphanumeric",
				"preserve_from": "@",
			},
			input:   "abc@example.com",
			wantErr: errFPEFF1ValueTooShort,
		},
		{
			name:    "error - value too short",
			params:  ParameterValues{"key_hex": testFPEFF1KeyHex},
			input:   "12345",
			wantErr: errFPEFF1ValueTooShort,
		},
		{
			name: "error - too few characters in the alphabet",
			params: ParameterValues{
				"key_hex": testFPEFF1KeyHex,
			},
			input:   "12-34-5-abcd",
			wantErr: errFPEFF1ValueTooShort,
		},
		{
			name: "error - prefix and suffix leave nothing to encrypt",
			params: ParameterValues{
				"key_hex":     testFPEFF1KeyHex,
				"keep_prefix": 3,
				"keep_suffix": 3,
			},
			input:   "12345678",
			wantErr: errFPEFF1ValueTooShort,
		},
		{
			name: "error - passthrough error rejects a foreign character",
			params: ParameterValues{
				"key_hex":     testFPEFF1KeyHex,
				"passthrough": fpeFF1PassthroughError,
			},
			input:   "123-456-7890",
			wantErr: errFPEFF1CharNotInAlphabet,
		},
		{
			name:    "error - unsupported value type",
			params:  ParameterValues{"key_hex": testFPEFF1KeyHex},
			input:   1234567890,
			wantErr: ErrUnsupportedValueType,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transformer, err := NewFPEFF1Transformer(tc.params)
			require.NoError(t, err)

			got, err := transformer.Transform(context.Background(), NewValue(tc.input, "text", nil))
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr != nil {
				return
			}

			gotStr, ok := got.(string)
			require.True(t, ok, "expected string, got %T", got)
			tc.validate(t, gotStr)
		})
	}
}

// the error must give the minimum. the user then selects between a larger
// min_length and an on_error policy
func TestFPEFF1Transformer_tooShortErrorNamesTheMinimum(t *testing.T) {
	t.Parallel()

	transformer, err := NewFPEFF1Transformer(ParameterValues{"key_hex": testFPEFF1KeyHex})
	require.NoError(t, err)

	_, err = transformer.Transform(context.Background(), NewValue("12345", "text", nil))
	require.ErrorContains(t, err, "but 6 are necessary")
}

// separators must not increase the keep_prefix count. one count must serve
// each format of the same phone number
func TestFPEFF1Transformer_keepCountsAlphabetCharactersOnly(t *testing.T) {
	t.Parallel()

	transformer, err := NewFPEFF1Transformer(ParameterValues{
		"key_hex":     testFPEFF1KeyHex,
		"keep_prefix": 4,
	})
	require.NoError(t, err)

	// the same number in four formats. each output keeps the country code 36
	// and the operator code 30
	for _, tc := range []struct {
		input      string
		wantPrefix string
	}{
		{"+36301234567", "+3630"},
		{"+36 30 123 4567", "+36 30"},
		{"+36-30-123-4567", "+36-30"},
		{"0036301234567", "0036"},
	} {
		got, err := transformer.Transform(context.Background(), NewValue(tc.input, "text", nil))
		require.NoError(t, err)

		gotStr, ok := got.(string)
		require.True(t, ok, "expected string, got %T", got)
		require.Len(t, gotStr, len(tc.input))
		require.True(t, strings.HasPrefix(gotStr, tc.wantPrefix),
			"%q: expected prefix %q, got %q", tc.input, tc.wantPrefix, gotStr)
	}
}

// keep_suffix counts in the same way. it keeps a check digit after a separator
func TestFPEFF1Transformer_keepSuffixCountsAlphabetCharactersOnly(t *testing.T) {
	t.Parallel()

	transformer, err := NewFPEFF1Transformer(ParameterValues{
		"key_hex":     testFPEFF1KeyHex,
		"keep_suffix": 2,
	})
	require.NoError(t, err)

	got, err := transformer.Transform(context.Background(), NewValue("123456789-42", "text", nil))
	require.NoError(t, err)

	gotStr, ok := got.(string)
	require.True(t, ok, "expected string, got %T", got)
	require.True(t, strings.HasSuffix(gotStr, "-42"), "expected the check digits to survive, got %q", gotStr)
}

func TestFPEFF1Transformer_deterministic(t *testing.T) {
	t.Parallel()

	transformer, err := NewFPEFF1Transformer(ParameterValues{"key_hex": testFPEFF1KeyHex})
	require.NoError(t, err)

	first, err := transformer.Transform(context.Background(), NewValue("1234567890", "text", nil))
	require.NoError(t, err)

	second, err := transformer.Transform(context.Background(), NewValue("1234567890", "text", nil))
	require.NoError(t, err)
	require.Equal(t, first, second)

	// a second transformer with the same parameters gives the same output.
	// the mapping is thus stable between runs
	other, err := NewFPEFF1Transformer(ParameterValues{"key_hex": testFPEFF1KeyHex})
	require.NoError(t, err)

	third, err := other.Transform(context.Background(), NewValue("1234567890", "text", nil))
	require.NoError(t, err)
	require.Equal(t, first, third)
}

func TestFPEFF1Transformer_associatedDataSeparatesColumns(t *testing.T) {
	t.Parallel()

	transformerA, err := NewFPEFF1Transformer(ParameterValues{
		"key_hex":         testFPEFF1KeyHex,
		"associated_data": "public.persons.phone_number",
	})
	require.NoError(t, err)

	transformerB, err := NewFPEFF1Transformer(ParameterValues{
		"key_hex":         testFPEFF1KeyHex,
		"associated_data": "public.persons.fax_number",
	})
	require.NoError(t, err)

	gotA, err := transformerA.Transform(context.Background(), NewValue("1234567890", "text", nil))
	require.NoError(t, err)

	gotB, err := transformerB.Transform(context.Background(), NewValue("1234567890", "text", nil))
	require.NoError(t, err)

	require.NotEqual(t, gotA, gotB)
}

// the transformer reports UniquenessPreserved. pgstream validate rules uses
// this report for a column with a unique index
func TestFPEFF1Transformer_injective(t *testing.T) {
	t.Parallel()

	transformer, err := NewFPEFF1Transformer(ParameterValues{"key_hex": testFPEFF1KeyHex})
	require.NoError(t, err)

	const (
		from = 1000000
		to   = 1010000
	)

	outputs := make(map[string]int, to-from)
	for i := from; i < to; i++ {
		input := strconv.Itoa(i)

		got, err := transformer.Transform(context.Background(), NewValue(input, "text", nil))
		require.NoError(t, err)

		gotStr, ok := got.(string)
		require.True(t, ok, "expected string, got %T", got)
		require.Len(t, gotStr, len(input))

		if collision, found := outputs[gotStr]; found {
			t.Fatalf("inputs %d and %d both encrypt to %q", collision, i, gotStr)
		}
		outputs[gotStr] = i
	}
	require.Len(t, outputs, to-from)
}

// the preserved end of the value has a variable length. the outputs stay
// unique because the delimiter is not in the alphabet. the encryption cannot
// create or remove such a delimiter, thus the transformer finds the same
// position in the output
func TestFPEFF1Transformer_injectiveWithPreserveFrom(t *testing.T) {
	t.Parallel()

	transformer, err := NewFPEFF1Transformer(ParameterValues{
		"key_hex":       testFPEFF1KeyHex,
		"alphabet":      "alphanumeric",
		"preserve_from": "@",
	})
	require.NoError(t, err)

	inputs := []string{
		"john.doe@example.com",
		"john.doe@example.org",
		"john.doe@examples.com",
		"jane.doe@example.com",
		"johndoe.@example.com",
		"john.doe@ex.ample.com",
	}

	outputs := make(map[string]string, len(inputs))
	for _, input := range inputs {
		got, err := transformer.Transform(context.Background(), NewValue(input, "text", nil))
		require.NoError(t, err)

		gotStr, ok := got.(string)
		require.True(t, ok, "expected string, got %T", got)
		require.Len(t, gotStr, len(input))

		collision, found := outputs[gotStr]
		require.False(t, found, "inputs %q and %q both encrypt to %q", collision, input, gotStr)
		outputs[gotStr] = input
	}
}

// with passthrough keep, the characters that are not in the alphabet stay in
// their positions. two values that are different only in those positions must
// give two different outputs
func TestFPEFF1Transformer_injectiveWithPassthrough(t *testing.T) {
	t.Parallel()

	transformer, err := NewFPEFF1Transformer(ParameterValues{"key_hex": testFPEFF1KeyHex})
	require.NoError(t, err)

	first, err := transformer.Transform(context.Background(), NewValue("123-4567890", "text", nil))
	require.NoError(t, err)

	second, err := transformer.Transform(context.Background(), NewValue("1234-567890", "text", nil))
	require.NoError(t, err)

	require.NotEqual(t, first, second)
}
