// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"

	ubiq "gitlab.com/ubiqsecurity/ubiq-fpe-go"
)

// FPEFF1Transformer encrypts values with FF1 (NIST SP 800-38G). The output
// contains the same characters as the input, and it has the same length. Two
// different inputs always give two different outputs, thus the transformer is
// safe on a column with a unique index. The transformer copies the characters
// that are not in the alphabet to the same positions in the output. A person
// who has the key can decrypt the values.
type FPEFF1Transformer struct {
	ciphers          *sync.Pool
	alphabet         map[rune]struct{}
	minLength        int
	keepPrefix       int
	keepSuffix       int
	preserveFrom     []rune
	errOnNonAlphabet bool
}

const (
	fpeFF1AlphabetDigits       = "0123456789"
	fpeFF1AlphabetLetters      = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	fpeFF1AlphabetAlphanumeric = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	fpeFF1PassthroughKeep  = "keep"
	fpeFF1PassthroughError = "error"

	fpeFF1MinDomainDigits = 6
)

var (
	errFPEFF1KeyNotFound            = errors.New("fpe_ff1_transformer: key_hex parameter not found")
	errFPEFF1KeyInvalid             = errors.New("fpe_ff1_transformer: key_hex must be 16, 24 or 32 hex-encoded bytes (128, 192 or 256 bit AES key)")
	errFPEFF1AlphabetTooSmall       = errors.New("fpe_ff1_transformer: alphabet must contain at least 2 characters")
	errFPEFF1AlphabetDuplicate      = errors.New("fpe_ff1_transformer: alphabet must not contain duplicate characters")
	errFPEFF1PassthroughValue       = fmt.Errorf("fpe_ff1_transformer: passthrough must be %q or %q", fpeFF1PassthroughKeep, fpeFF1PassthroughError)
	errFPEFF1KeepNegative           = errors.New("fpe_ff1_transformer: keep_prefix and keep_suffix must not be negative")
	errFPEFF1PreserveFromInAlphabet = errors.New("fpe_ff1_transformer: preserve_from must not contain characters from the alphabet, because the transformer must find the delimiter in the output")
	errFPEFF1ValueTooShort          = errors.New("fpe_ff1_transformer: value is too short to encrypt")
	errFPEFF1CharNotInAlphabet      = errors.New("fpe_ff1_transformer: value contains a character that is not in the alphabet")
	errFPEFF1CipherUnavailable      = errors.New("fpe_ff1_transformer: failed to build FF1 cipher")

	fpeFF1NamedAlphabets = map[string]string{
		"digits":       fpeFF1AlphabetDigits,
		"letters":      fpeFF1AlphabetLetters,
		"alphanumeric": fpeFF1AlphabetAlphanumeric,
	}

	fpeFF1CompatibleTypes = []SupportedDataType{
		StringDataType,
	}

	fpeFF1Params = []Parameter{
		{
			Name:          "key_hex",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
		{
			Name:          "associated_data",
			SupportedType: "string",
			Default:       "",
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "alphabet",
			SupportedType: "string",
			Default:       "digits",
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "passthrough",
			SupportedType: "string",
			Default:       fpeFF1PassthroughKeep,
			Dynamic:       false,
			Required:      false,
			Values:        []any{fpeFF1PassthroughKeep, fpeFF1PassthroughError},
		},
		{
			Name:          "keep_prefix",
			SupportedType: "int",
			Default:       0,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "keep_suffix",
			SupportedType: "int",
			Default:       0,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "min_length",
			SupportedType: "int",
			Default:       0,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "preserve_from",
			SupportedType: "string",
			Default:       "",
			Dynamic:       false,
			Required:      false,
		},
	}
)

func NewFPEFF1Transformer(params ParameterValues) (*FPEFF1Transformer, error) {
	keyHex, found, err := FindParameter[string](params, "key_hex")
	if err != nil {
		return nil, fmt.Errorf("fpe_ff1_transformer: key_hex must be a string: %w", err)
	}
	if !found {
		return nil, errFPEFF1KeyNotFound
	}
	key, err := hex.DecodeString(keyHex)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errFPEFF1KeyInvalid, err)
	}
	switch len(key) {
	case 16, 24, 32:
	default:
		return nil, fmt.Errorf("%w: got %d bytes", errFPEFF1KeyInvalid, len(key))
	}

	aad, err := FindParameterWithDefault(params, "associated_data", "")
	if err != nil {
		return nil, fmt.Errorf("fpe_ff1_transformer: associated_data must be a string: %w", err)
	}

	alphabetParam, err := FindParameterWithDefault(params, "alphabet", "digits")
	if err != nil {
		return nil, fmt.Errorf("fpe_ff1_transformer: alphabet must be a string: %w", err)
	}
	alphabet, alphabetSet, err := resolveFPEFF1Alphabet(alphabetParam)
	if err != nil {
		return nil, err
	}

	passthrough, err := FindParameterWithDefault(params, "passthrough", fpeFF1PassthroughKeep)
	if err != nil {
		return nil, fmt.Errorf("fpe_ff1_transformer: passthrough must be a string: %w", err)
	}
	if passthrough != fpeFF1PassthroughKeep && passthrough != fpeFF1PassthroughError {
		return nil, fmt.Errorf("%w: got %q", errFPEFF1PassthroughValue, passthrough)
	}

	keepPrefix, err := FindParameterWithDefault(params, "keep_prefix", 0)
	if err != nil {
		return nil, fmt.Errorf("fpe_ff1_transformer: keep_prefix must be an integer: %w", err)
	}
	keepSuffix, err := FindParameterWithDefault(params, "keep_suffix", 0)
	if err != nil {
		return nil, fmt.Errorf("fpe_ff1_transformer: keep_suffix must be an integer: %w", err)
	}
	if keepPrefix < 0 || keepSuffix < 0 {
		return nil, errFPEFF1KeepNegative
	}

	preserveFromParam, err := FindParameterWithDefault(params, "preserve_from", "")
	if err != nil {
		return nil, fmt.Errorf("fpe_ff1_transformer: preserve_from must be a string: %w", err)
	}
	preserveFrom, err := parseFPEFF1PreserveFrom(preserveFromParam, alphabetSet)
	if err != nil {
		return nil, err
	}

	// FF1 does not encrypt a set of possible values that is smaller than
	// 10^6, because an attacker can try all of them. min_length increases
	// this minimum, but it cannot decrease it.
	secureMinLength := fpeFF1SecureMinLength(len(alphabetSet))
	minLength, err := FindParameterWithDefault(params, "min_length", 0)
	if err != nil {
		return nil, fmt.Errorf("fpe_ff1_transformer: min_length must be an integer: %w", err)
	}
	switch {
	case minLength == 0:
		minLength = secureMinLength
	case minLength < secureMinLength:
		return nil, fmt.Errorf("fpe_ff1_transformer: min_length %d is below the %d characters FF1 requires for an alphabet of %d characters",
			minLength, secureMinLength, len(alphabetSet))
	}

	ciphers := &sync.Pool{
		New: func() any {
			// the ubiq FF1 context changes its own scratch state during
			// encryption. two transformations cannot share one cipher.
			cipher, err := newFPEFF1Cipher(key, []byte(aad), alphabet)
			if err != nil {
				return nil
			}
			return cipher
		},
	}

	// build one cipher now. a bad key or alphabet then stops the pipeline at
	// start-up, and not at the first row.
	if _, err := newFPEFF1Cipher(key, []byte(aad), alphabet); err != nil {
		return nil, fmt.Errorf("%w: %w", errFPEFF1CipherUnavailable, err)
	}

	return &FPEFF1Transformer{
		ciphers:          ciphers,
		alphabet:         alphabetSet,
		minLength:        minLength,
		keepPrefix:       keepPrefix,
		keepSuffix:       keepSuffix,
		preserveFrom:     preserveFrom,
		errOnNonAlphabet: passthrough == fpeFF1PassthroughError,
	}, nil
}

func newFPEFF1Cipher(key, tweak []byte, alphabet string) (*ubiq.FF1, error) {
	// 0 and 0 remove the limits on the tweak length. the tweak is
	// configuration from the operator, not input from an attacker.
	return ubiq.NewFF1(key, tweak, 0, 0, len([]rune(alphabet)), alphabet)
}

// resolveFPEFF1Alphabet reads a named alphabet or a set of characters. It
// returns the characters, and a set that tests membership.
func resolveFPEFF1Alphabet(param string) (string, map[rune]struct{}, error) {
	alphabet, found := fpeFF1NamedAlphabets[param]
	if !found {
		alphabet = param
	}

	runes := []rune(alphabet)
	if len(runes) < 2 {
		return "", nil, fmt.Errorf("%w: got %d", errFPEFF1AlphabetTooSmall, len(runes))
	}

	alphabetSet := make(map[rune]struct{}, len(runes))
	for _, r := range runes {
		if _, duplicate := alphabetSet[r]; duplicate {
			return "", nil, fmt.Errorf("%w: %q appears more than once", errFPEFF1AlphabetDuplicate, r)
		}
		alphabetSet[r] = struct{}{}
	}

	return alphabet, alphabetSet, nil
}

// parseFPEFF1PreserveFrom validates the delimiter that marks the start of the
// preserved end of the value. Each character of the delimiter must not be in
// the alphabet. The transformer copies such characters to the same position in
// the output, thus it finds the delimiter again. The encryption can create or
// remove a delimiter that is in the alphabet, and the outputs are then not
// unique.
func parseFPEFF1PreserveFrom(param string, alphabetSet map[rune]struct{}) ([]rune, error) {
	if param == "" {
		return nil, nil
	}

	delimiter := []rune(param)
	for _, r := range delimiter {
		if _, inAlphabet := alphabetSet[r]; inAlphabet {
			return nil, fmt.Errorf("%w: %q", errFPEFF1PreserveFromInAlphabet, r)
		}
	}

	return delimiter, nil
}

// fpeFF1SecureMinLength returns the shortest input that FF1 encrypts in the
// given radix. This is the smallest n where radix^n is 10^6 or more.
func fpeFF1SecureMinLength(radix int) int {
	return int(math.Ceil(float64(fpeFF1MinDomainDigits) / math.Log10(float64(radix))))
}

func (ft *FPEFF1Transformer) Transform(_ context.Context, v Value) (any, error) {
	val, ok := v.TransformValue.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T: %w", v.TransformValue, ErrUnsupportedValueType)
	}
	return ft.transform(val)
}

func (ft *FPEFF1Transformer) transform(val string) (string, error) {
	runes := []rune(val)

	tailStart := len(runes)
	if from := ft.preservedTailStart(runes); from >= 0 {
		tailStart = from
	}

	// keep_prefix and keep_suffix count only the characters in the alphabet.
	// the transformer does not encrypt separators, thus separators must not
	// increase the count. keep_prefix 4 then keeps the first four digits of
	// "+36301234567" and of "+36 30 123 4567".
	positions := make([]int, 0, tailStart)
	for i := range tailStart {
		if _, inAlphabet := ft.alphabet[runes[i]]; inAlphabet {
			positions = append(positions, i)
		}
	}

	required := ft.keepPrefix + ft.keepSuffix + ft.minLength
	if len(positions) < required {
		return "", fmt.Errorf("%w: it has %d characters in the alphabet, but %d are necessary",
			errFPEFF1ValueTooShort, len(positions), required)
	}
	positions = positions[ft.keepPrefix : len(positions)-ft.keepSuffix]

	if ft.errOnNonAlphabet {
		for i := positions[0]; i < positions[len(positions)-1]; i++ {
			if _, inAlphabet := ft.alphabet[runes[i]]; !inAlphabet {
				return "", fmt.Errorf("%w: %q", errFPEFF1CharNotInAlphabet, runes[i])
			}
		}
	}

	plaintext := strings.Builder{}
	for _, pos := range positions {
		plaintext.WriteRune(runes[pos])
	}

	ciphertext, err := ft.encrypt(plaintext.String())
	if err != nil {
		return "", fmt.Errorf("fpe_ff1_transformer: %w", err)
	}

	encrypted := []rune(ciphertext)
	if len(encrypted) != len(positions) {
		return "", fmt.Errorf("fpe_ff1_transformer: FF1 returned %d characters for an input of %d", len(encrypted), len(positions))
	}
	for i, pos := range positions {
		runes[pos] = encrypted[i]
	}

	return string(runes), nil
}

// preservedTailStart returns the index where the preserved end of the value
// starts. This is the last occurrence of the preserve_from delimiter. The
// function returns -1 when preserve_from is empty, or when the value does not
// contain the delimiter.
func (ft *FPEFF1Transformer) preservedTailStart(runes []rune) int {
	if len(ft.preserveFrom) == 0 {
		return -1
	}
	for i := len(runes) - len(ft.preserveFrom); i >= 0; i-- {
		if slices.Equal(runes[i:i+len(ft.preserveFrom)], ft.preserveFrom) {
			return i
		}
	}
	return -1
}

func (ft *FPEFF1Transformer) encrypt(plaintext string) (string, error) {
	cipher, _ := ft.ciphers.Get().(*ubiq.FF1)
	if cipher == nil {
		return "", errFPEFF1CipherUnavailable
	}
	defer ft.ciphers.Put(cipher)

	// a nil tweak selects the associated_data from the constructor
	return cipher.Encrypt(plaintext, nil)
}

func (ft *FPEFF1Transformer) CompatibleTypes() []SupportedDataType {
	return fpeFF1CompatibleTypes
}

func (ft *FPEFF1Transformer) Type() TransformerType {
	return FPEFF1
}

func (ft *FPEFF1Transformer) IsDynamic() bool {
	return false
}

func (ft *FPEFF1Transformer) Uniqueness() Uniqueness {
	return UniquenessPreserved
}

func (ft *FPEFF1Transformer) Close() error {
	return nil
}

func FPEFF1TransformerDefinition() *Definition {
	return &Definition{
		SupportedTypes: fpeFF1CompatibleTypes,
		Parameters:     fpeFF1Params,
		Uniqueness:     UniquenessPreserved,
	}
}
