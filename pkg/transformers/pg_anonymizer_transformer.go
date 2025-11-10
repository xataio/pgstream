// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	pglib "github.com/xataio/pgstream/internal/postgres"
)

type PGAnonymizerTransformer struct {
	conn            pglib.Querier
	anonFn          string
	salt            string
	hashAlgorithm   string
	interval        string
	ratio           float64
	sigma           float64
	mask            string
	maskPrefixCount int
	maskSuffixCount int
	min             string
	max             string
	rangeStr        string
	locale          string
	count           int
	unit            string
	prefix          string
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
			SupportedType: "float",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "sigma",
			SupportedType: "float",
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
		{
			Name:          "min",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "max",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "range",
			SupportedType: "string",
			Default:       "",
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "locale",
			SupportedType: "string",
			Default:       "en_US",
			Dynamic:       false,
			Required:      false,
			Values: []any{
				"ar_SA",
				"en_US",
				"fr_FR",
				"ja_JP",
				"pt_BR",
				"zh_CN",
				"zh_TW",
			},
		},
		{
			Name:          "count",
			SupportedType: "int",
			Default:       0,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "unit",
			SupportedType: "string",
			Default:       "paragraph",
			Dynamic:       false,
			Required:      false,
			Values: []any{
				"character",
				"word",
				"paragraph",
			},
		},
		{
			Name:          "prefix",
			SupportedType: "string",
			Default:       "",
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

const (
	dateFormat        = "2006-01-02"
	timestampTZFormat = "2006-01-02T15:04:05.000Z"
	timestampFormat   = "2006-01-02T15:04:05.000"
)

var (
	errAnonFunctionNotFound = errors.New("pg_anonymizer_transformer: anon_function parameter not found")
	errPGURLNotFound        = errors.New("pg_anonymizer_transformer: postgres_url parameter not found")

	// Validation errors
	errAnonFunctionNotAllowed      = errors.New("pg_anonymizer_transformer: anon_function is not allowed")
	errAnonFunctionInvalid         = errors.New("pg_anonymizer_transformer: anon_function must start with 'anon.' and contain no semicolons")
	errDigestHashAlgorithmRequired = errors.New("pg_anonymizer_transformer: hash_algorithm is required for anon.digest function")
	errDigestSaltRequired          = errors.New("pg_anonymizer_transformer: salt is required for anon.digest function")
	errNoiseRatioRequired          = errors.New("pg_anonymizer_transformer: ratio is required for anon.noise function")
	errDnoiseIntervalRequired      = errors.New("pg_anonymizer_transformer: interval is required for anon.dnoise function")
	errPartialMaskRequired         = errors.New("pg_anonymizer_transformer: mask is required for anon.partial function")
	errPartialPrefixInvalid        = errors.New("pg_anonymizer_transformer: mask_prefix_count must be non-negative for anon.partial function")
	errPartialSuffixInvalid        = errors.New("pg_anonymizer_transformer: mask_suffix_count must be non-negative for anon.partial function")
	errImageBlurSigmaRequired      = errors.New("pg_anonymizer_transformer: sigma is required for anon.image_blur function")
)

var allowedFunctionPrefixes = []string{
	"anon.fake_",
	"anon.pseudo_",
	"anon.random_",
	"anon.hash",
	"anon.digest",
	"anon.noise",
	"anon.dnoise",
	"anon.image_blur",
	"anon.partial",
	"anon.lorem_ipsum",
	"anon.dummy",
}

// NewPGAnonymizerTransformer creates a new transformer that supports pg_anonymizer functions.
// Unsupported functions:
// - destruction ('MASKED WITH VALUE ”CONFIDENTIAL” )
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
	if interval != "" && !isValidInterval(interval) {
		return nil, fmt.Errorf("pg_anonymizer_transformer: interval must be a valid PostgreSQL interval: %w", ErrInvalidParameters)
	}

	ratio, err := FindParameterWithDefault(params, "ratio", 0.0)
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: ratio must be a float: %w", err)
	}

	sigma, err := FindParameterWithDefault(params, "sigma", 0.0)
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: sigma must be a float: %w", err)
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

	min, err := FindParameterWithDefault(params, "min", "")
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: min must be a string: %w", err)
	}

	max, err := FindParameterWithDefault(params, "max", "")
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: max must be a string: %w", err)
	}

	if (min != "" && max == "") || (min == "" && max != "") {
		return nil, fmt.Errorf("pg_anonymizer_transformer: both min and max must be provided together: %w", ErrInvalidParameters)
	}

	rangeStr, err := FindParameterWithDefault(params, "range", "")
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: range must be a string: %w", err)
	}

	locale, err := FindParameterWithDefault(params, "locale", "en_US")
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: locale must be a string: %w", err)
	}

	count, err := FindParameterWithDefault(params, "count", 0)
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: count must be an integer: %w", err)
	}

	unit, err := FindParameterWithDefault(params, "unit", "paragraphs")
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: unit must be a string: %w", err)
	}

	if unit != "characters" && unit != "words" && unit != "paragraphs" {
		return nil, fmt.Errorf("pg_anonymizer_transformer: unit must be one of 'characters', 'words', or 'paragraphs': %w", ErrInvalidParameters)
	}

	prefix, err := FindParameterWithDefault(params, "prefix", "")
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: prefix must be a string: %w", err)
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
		max:             max,
		min:             min,
		rangeStr:        rangeStr,
		locale:          locale,
		count:           count,
		unit:            unit,
		prefix:          prefix,
	}

	if err := t.validateAnonFunction(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *PGAnonymizerTransformer) Transform(ctx context.Context, value Value) (any, error) {
	query, args := t.buildParameterizedQuery(value.TransformValue, value.TransformType)

	var transformedValue any
	err := t.conn.QueryRow(ctx, []any{&transformedValue}, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pg_anonymizer_transformer: failed to call anonymizer function %s: %w", t.anonFn, err)
	}
	return transformedValue, nil
}

func (t *PGAnonymizerTransformer) CompatibleTypes() []SupportedDataType {
	return pgAnonymizerCompatibleTypes
}

func (t *PGAnonymizerTransformer) Type() TransformerType {
	return PGAnonymizer
}

func (t *PGAnonymizerTransformer) IsDynamic() bool {
	return false
}

func (t *PGAnonymizerTransformer) Close() error {
	return t.conn.Close(context.Background())
}

// parsing the function name and building the parameterized query ensures safety against SQL injection
func (t *PGAnonymizerTransformer) buildParameterizedQuery(value any, valueType string) (string, []any) {
	fnName, _, _ := strings.Cut(t.anonFn, "(")

	switch fnName {
	case "anon.random_hash",
		"anon.hash",
		"anon.partial_email":
		return fmt.Sprintf("SELECT %s($1)", fnName), []any{value}

	case "anon.digest":
		return fmt.Sprintf("SELECT %s($1, $2, $3)", fnName), []any{value, t.salt, t.hashAlgorithm}

	case "anon.noise":
		// Convert ratio string to float for proper parameter binding
		return fmt.Sprintf("SELECT %s($1::%s, $2)", fnName, valueType), []any{value, t.ratio}

	case "anon.dnoise":
		// Format the value properly for PostgreSQL date/timestamp types
		formattedValue := t.formatValueForPostgres(value, valueType)
		return fmt.Sprintf("SELECT %s($1::%s, $2::interval)", fnName, valueType), []any{formattedValue, t.interval}

	case "anon.image_blur":
		return fmt.Sprintf("SELECT %s($1, $2)", fnName), []any{value, t.sigma}

	case "anon.partial":
		return fmt.Sprintf("SELECT %s($1, $2, $3, $4)", fnName), []any{value, t.maskPrefixCount, t.mask, t.maskSuffixCount}

	case "anon.random_in_int4range",
		"anon.random_in_int8range",
		"anon.random_in_daterange",
		"anon.random_in_numrange",
		"anon.random_in_tsrange",
		"anon.random_in_tstzrange",
		"anon.random_in_enum",
		"anon.random_in":

		if strings.Contains(t.rangeStr, "ARRAY") {
			// If the user provided an ARRAY, we need to use it as is
			return fmt.Sprintf("SELECT %s(%s)", fnName, t.rangeStr), nil
		}

		return fmt.Sprintf("SELECT %s($1)", fnName), []any{t.rangeStr}

	case "anon.random_string":
		return fmt.Sprintf("SELECT %s($1)", fnName), []any{t.count}

	case "anon.random_phone":
		if t.prefix != "" {
			return fmt.Sprintf("SELECT %s($1)", fnName), []any{t.prefix}
		}
		return fmt.Sprintf("SELECT %s()", fnName), nil

	case "anon.random_date_between",
		"anon.random_int_between",
		"anon.random_bigint_between":

		return fmt.Sprintf("SELECT %s($1, $2)", fnName), []any{t.min, t.max}

	case "anon.lorem_ipsum":
		if t.count != 0 {
			return fmt.Sprintf("SELECT %s(%s := $1)", fnName, t.unit), []any{t.count}
		}
		return fmt.Sprintf("SELECT %s()", fnName), []any{}

	default:
		// generic function handling
		switch {
		case strings.HasPrefix(t.anonFn, "anon.pseudo_"):
			// for pseudo functions, we need to cast the value
			if t.salt != "" {
				return fmt.Sprintf("SELECT %s($1::%s, $2)", fnName, valueType), []any{value, t.salt}
			}
			return fmt.Sprintf("SELECT %s($1::%s)", fnName, valueType), []any{value}

		case strings.HasPrefix(t.anonFn, "anon.dummy") && strings.HasSuffix(fnName, "_locale"):
			return fmt.Sprintf("SELECT %s($1)", fnName), []any{t.locale}

		}
		// functions that do not take any parameters
		if strings.HasSuffix(t.anonFn, "()") {
			return fmt.Sprintf("SELECT %s", t.anonFn), []any{}
		}

		return fmt.Sprintf("SELECT %s()", fnName), []any{}
	}
}

func (t *PGAnonymizerTransformer) validateAnonFunction() error {
	// basic validation to check if the function starts with "anon."
	if len(t.anonFn) < 5 || t.anonFn[:5] != "anon." {
		return errAnonFunctionInvalid
	}

	if strings.Contains(t.anonFn, ";") {
		return errAnonFunctionInvalid
	}

	if !isAllowedFunction(t.anonFn) {
		return errAnonFunctionNotAllowed
	}

	if strings.HasPrefix(t.anonFn, "anon.digest") {
		if t.hashAlgorithm == "" {
			return errDigestHashAlgorithmRequired
		}
		if t.salt == "" {
			return errDigestSaltRequired
		}
	}

	if strings.HasPrefix(t.anonFn, "anon.noise") && t.ratio == 0 {
		return errNoiseRatioRequired
	}

	if strings.HasPrefix(t.anonFn, "anon.dnoise") && t.interval == "" {
		return errDnoiseIntervalRequired
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

	if strings.HasPrefix(t.anonFn, "anon.image_blur") && t.sigma == 0 {
		return errImageBlurSigmaRequired
	}

	if strings.HasPrefix(t.anonFn, "anon.random_") && strings.Contains(t.anonFn, "between") {
		if t.min == "" || t.max == "" {
			return fmt.Errorf("pg_anonymizer_transformer: both min and max are required for %s function: %w", t.anonFn, ErrInvalidParameters)
		}
	}

	if (strings.HasPrefix(t.anonFn, "anon.random_in_") && strings.Contains(t.anonFn, "range")) ||
		strings.HasPrefix(t.anonFn, "anon.random_in_enum") {
		if t.rangeStr == "" {
			return fmt.Errorf("pg_anonymizer_transformer: range is required for %s function: %w", t.anonFn, ErrInvalidParameters)
		}
	}

	if (strings.HasPrefix(t.anonFn, "anon.random_string") || strings.HasPrefix(t.anonFn, "anon.random_phone")) && t.count == 0 {
		return fmt.Errorf("pg_anonymizer_transformer: count is required for %s function: %w", t.anonFn, ErrInvalidParameters)
	}

	return nil
}

func (t *PGAnonymizerTransformer) formatValueForPostgres(value any, valueType string) string {
	switch valueType {
	case "date":
		var d pgtype.Date
		if err := d.Scan(value); err == nil {
			return d.Time.Format(dateFormat)
		}
	case "timestamp", "timestamp without time zone":
		var ts pgtype.Timestamp
		if err := ts.Scan(value); err == nil {
			return ts.Time.Format(timestampFormat)
		}
	case "timestamptz", "timestamp with time zone":
		var ts pgtype.Timestamptz
		if err := ts.Scan(value); err == nil {
			return ts.Time.Format(timestampTZFormat)
		}
	}
	return fmt.Sprintf("%v", value)
}

func PGAnonymizerTransformerDefinition() *Definition {
	return &Definition{
		SupportedTypes: pgAnonymizerCompatibleTypes,
		Parameters:     pgAnonymizerParams,
	}
}

func isAllowedFunction(anonFn string) bool {
	for _, prefix := range allowedFunctionPrefixes {
		if strings.HasPrefix(anonFn, prefix) {
			return true
		}
	}
	return false
}

func isValidInterval(interval string) bool {
	var i pgtype.Interval
	if err := i.Scan(interval); err != nil {
		return false
	}
	return true
}
