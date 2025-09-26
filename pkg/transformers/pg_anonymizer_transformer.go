// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pglib "github.com/xataio/pgstream/internal/postgres"
)

type PGAnonymizerTransformer struct {
	conn            pglib.Querier
	anonFn          string
	salt            string
	hashAlgorithm   string
	interval        string
	ratio           string
	sigma           string
	mask            string
	maskPrefixCount int
	maskSuffixCount int
}

var (
	pgAnonymizerCompatibleTypes = []SupportedDataType{
		AllDataTypes,
	}

	pgAnonymizerParams = []Parameter{
		{
			Name:          "anon_function",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
		{
			Name:          "postgres_url",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
		{
			Name:          "salt",
			SupportedType: "string",
			Default:       "",
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "hash_algorithm",
			SupportedType: "string",
			Default:       "sha256",
			Dynamic:       false,
			Required:      false,
			Values: []any{
				"md5",
				"sha224",
				"sha256",
				"sha384",
				"sha512",
			},
		},
		{
			Name:          "interval",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "ratio",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "sigma",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "mask",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "mask_prefix_count",
			SupportedType: "int",
			Default:       0,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "mask_suffix_count",
			SupportedType: "int",
			Default:       0,
			Dynamic:       false,
			Required:      false,
		},
	}

	supportedHashAlgorithms = map[string]struct{}{
		"md5":    {},
		"sha224": {},
		"sha256": {},
		"sha384": {},
		"sha512": {},
	}
)

var (
	errAnonFunctionNotFound = fmt.Errorf("pg_anonymizer_transformer: anon_function parameter not found")
	errPGURLNotFound        = fmt.Errorf("pg_anonymizer_transformer: postgres_url parameter not found")

	// Validation errors
	errAnonFunctionInvalid         = errors.New("pg_anonymizer_transformer: anon_function must start with 'anon.'")
	errDigestHashAlgorithmRequired = errors.New("pg_anonymizer_transformer: hash_algorithm is required for anon.digest function")
	errDigestSaltRequired          = errors.New("pg_anonymizer_transformer: salt is required for anon.digest function")
	errNoiseRatioRequired          = errors.New("pg_anonymizer_transformer: ratio is required for anon.noise function")
	errDnoiseIntervalRequired      = errors.New("pg_anonymizer_transformer: interval is required for anon.dnoise function")
	errPartialMaskRequired         = errors.New("pg_anonymizer_transformer: mask is required for anon.partial function")
	errPartialPrefixInvalid        = errors.New("pg_anonymizer_transformer: mask_prefix_count must be non-negative for anon.partial function")
	errPartialSuffixInvalid        = errors.New("pg_anonymizer_transformer: mask_suffix_count must be non-negative for anon.partial function")
	errImageBlurSigmaRequired      = errors.New("pg_anonymizer_transformer: sigma is required for anon.image_blur function")
)

// NewPGAnonymizerTransformer creates a new transformer that supports pg_anonymizer functions.
// Unsupported functions:
// - anon.ternary (conditional masking)
// - anon.generalize... (data generalization - only makes sense with views)
func NewPGAnonymizerTransformer(params ParameterValues) (*PGAnonymizerTransformer, error) {
	anonFn, found, err := FindParameter[string](params, "anon_function")
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: anon_function must be a string: %w", err)
	}
	if !found {
		return nil, errAnonFunctionNotFound
	}

	salt, err := FindParameterWithDefault(params, "salt", "")
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: salt must be a string: %w", err)
	}

	hashAlgorithm, err := FindParameterWithDefault(params, "hash_algorithm", "sha256")
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: hash_algorithm must be a string: %w", err)
	}

	if _, ok := supportedHashAlgorithms[hashAlgorithm]; !ok {
		return nil, fmt.Errorf("pg_anonymizer_transformer: unsupported hash_algorithm: %s", hashAlgorithm)
	}

	interval, err := FindParameterWithDefault(params, "interval", "")
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: interval must be a string: %w", err)
	}

	ratio, err := FindParameterWithDefault(params, "ratio", "")
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: ratio must be a string: %w", err)
	}

	sigma, err := FindParameterWithDefault(params, "sigma", "")
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: sigma must be a string: %w", err)
	}

	mask, err := FindParameterWithDefault(params, "mask", "")
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: mask must be a string: %w", err)
	}

	maskPrefixCount, err := FindParameterWithDefault(params, "mask_prefix_count", 0)
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: mask_prefix_count must be an integer: %w", err)
	}

	maskSuffixCount, err := FindParameterWithDefault(params, "mask_suffix_count", 0)
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: mask_suffix_count must be an integer: %w", err)
	}

	url, found, err := FindParameter[string](params, "postgres_url")
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: postgres_url must be a string: %w", err)
	}
	if !found {
		return nil, errPGURLNotFound
	}

	pool, err := pglib.NewConnPool(context.Background(), url)
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: failed to create connection pool: %w", err)
	}

	t := &PGAnonymizerTransformer{
		conn:            pool,
		anonFn:          anonFn,
		salt:            salt,
		hashAlgorithm:   hashAlgorithm,
		ratio:           ratio,
		interval:        interval,
		sigma:           sigma,
		mask:            mask,
		maskPrefixCount: maskPrefixCount,
		maskSuffixCount: maskSuffixCount,
	}

	if err := t.validateAnonFunction(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *PGAnonymizerTransformer) Transform(ctx context.Context, value Value) (any, error) {
	anonFn := t.getAnonFunction(value.TransformValue, value.TransformType)

	var transformedValue any
	err := t.conn.QueryRow(ctx, fmt.Sprintf("SELECT %s", anonFn)).Scan(&transformedValue)
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: failed to call anonymizer function: %w", err)
	}
	return transformedValue, nil
}

func (t *PGAnonymizerTransformer) CompatibleTypes() []SupportedDataType {
	return pgAnonymizerCompatibleTypes
}

func (t *PGAnonymizerTransformer) Type() TransformerType {
	return PGAnonymizer
}

func (t *PGAnonymizerTransformer) Close() error {
	return t.conn.Close(context.Background())
}

func (t *PGAnonymizerTransformer) getAnonFunction(value any, valueType string) string {
	fnName, _, _ := strings.Cut(t.anonFn, "(")
	switch {
	case strings.HasPrefix(t.anonFn, "anon.pseudo_"):
		// for pseudo functions, we need to cast the value, since it
		// receives a polymorphic type and would otherwise error
		fnValue := fmt.Sprintf("'%v'::%s", value, valueType)

		// pseudo anon function takes an optional salt parameter
		if t.salt != "" {
			return fmt.Sprintf("%s(%s, '%v')", fnName, fnValue, t.salt)
		}
		return fmt.Sprintf("%s(%s)", fnName, fnValue)

	case strings.HasPrefix(t.anonFn, "anon.random_hash"),
		strings.HasPrefix(t.anonFn, "anon.hash"),
		strings.HasPrefix(t.anonFn, "anon.partial_email"):
		// functions that take a value parameter
		return fmt.Sprintf("%s('%v')", fnName, value)

	case strings.HasPrefix(t.anonFn, "anon.digest"):
		// anon.digest(value, salt, algorithm)
		return fmt.Sprintf("%s('%v', '%v', '%s')", fnName, value, t.salt, t.hashAlgorithm)

	case strings.HasPrefix(t.anonFn, "anon.noise"):
		return fmt.Sprintf("%s(%v, %s)", fnName, value, t.ratio)

	case strings.HasPrefix(t.anonFn, "anon.dnoise"):
		// receives a polymorphic type, so we need to cast the value
		return fmt.Sprintf("%s('%v'::%s, '%s')", fnName, value, valueType, t.interval)

	case strings.HasPrefix(t.anonFn, "anon.image_blur"):
		return fmt.Sprintf("%s('%v', %s)", fnName, value, t.sigma)

	case strings.HasPrefix(t.anonFn, "anon.partial"):
		return fmt.Sprintf("%s('%v', %d, '%s', %d)", fnName, value, t.maskPrefixCount, t.mask, t.maskSuffixCount)

	default:
		// functions that do not take any parameters (constant input)

		// make sure the function has parentheses if the user did not provide
		// them
		if !strings.Contains(t.anonFn, "(") {
			return fmt.Sprintf("%s()", t.anonFn)
		}
		return t.anonFn
	}
}

func (t *PGAnonymizerTransformer) validateAnonFunction() error {
	// basic validation to check if the function starts with "anon."
	if len(t.anonFn) < 5 || t.anonFn[:5] != "anon." {
		return errAnonFunctionInvalid
	}

	if strings.HasPrefix(t.anonFn, "anon.digest") {
		if t.hashAlgorithm == "" {
			return errDigestHashAlgorithmRequired
		}
		if t.salt == "" {
			return errDigestSaltRequired
		}
	}

	if strings.HasPrefix(t.anonFn, "anon.noise") {
		if t.ratio == "" {
			return errNoiseRatioRequired
		}
	}

	if strings.HasPrefix(t.anonFn, "anon.dnoise") {
		if t.interval == "" {
			return errDnoiseIntervalRequired
		}
	}

	if strings.HasPrefix(t.anonFn, "anon.partial") {
		if t.mask == "" {
			return errPartialMaskRequired
		}
		if t.maskPrefixCount < 0 {
			return errPartialPrefixInvalid
		}
		if t.maskSuffixCount < 0 {
			return errPartialSuffixInvalid
		}
	}

	if strings.HasPrefix(t.anonFn, "anon.image_blur") {
		if t.sigma == "" {
			return errImageBlurSigmaRequired
		}
	}

	return nil
}

func PGAnonymizerTransformerDefinition() *Definition {
	return &Definition{
		SupportedTypes: pgAnonymizerCompatibleTypes,
		Parameters:     pgAnonymizerParams,
	}
}
