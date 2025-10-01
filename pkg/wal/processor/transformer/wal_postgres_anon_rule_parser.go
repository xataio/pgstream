// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/transformers"
	"gopkg.in/yaml.v3"
)

// AnonRuleParser is a wrapper around a transformer rule parser that fetches and
// converts anon extension security labels into transformation rules before
// validation is performed
type AnonRuleParser struct {
	parser     ParseFn
	conn       pglib.Querier
	logger     loglib.Logger
	marshaler  func(v any) ([]byte, error)
	dumpToFile bool
}

type maskingRule struct {
	schema   string
	table    string
	column   string
	function string
}

var (
	errMissingParameters = errors.New("missing required parameters for anon masking function")
	errTooManyParameters = errors.New("too many parameters for anon masking function")
	errInvalidParameter  = errors.New("invalid parameter for anon masking function")
)

const anonRulesFile = "inferred_anon_transformation_rules.yaml"

// NewAnonRuleParser is a wrapper around a parser that fetches and converts anon
// extension security labels into transformation rules before validation is
// performed
func NewAnonRuleParser(ctx context.Context, pgURL string, dumpToFile bool, logger loglib.Logger, parser ParseFn) (*AnonRuleParser, error) {
	conn, err := pglib.NewConnPool(ctx, pgURL)
	if err != nil {
		return nil, err
	}
	return &AnonRuleParser{
		conn:       conn,
		logger:     logger,
		parser:     parser,
		marshaler:  yaml.Marshal,
		dumpToFile: dumpToFile,
	}, nil
}

// ParseAndValidate will fetch the anon masking rules from the source database
// and convert them into transformation rules before passing them to the wrapped
// parser for validation.
func (a *AnonRuleParser) ParseAndValidate(ctx context.Context, rules Rules) (map[string]ColumnTransformers, error) {
	var err error
	rules.Transformers, err = a.parseAnonMaskingRules(ctx)
	if err != nil {
		return nil, err
	}

	if a.dumpToFile {
		a.logger.Debug("dumping inferred anon transformation rules to file", loglib.Fields{"file": anonRulesFile, "rules_count": len(rules.Transformers)})
		if err := a.dumpRulesToFile(anonRulesFile, rules); err != nil {
			a.logger.Error(err, "failed to dump inferred anon transformation rules to file", loglib.Fields{"file": anonRulesFile})
		}
	}

	return a.parser(ctx, rules)
}

func (a *AnonRuleParser) parseAnonMaskingRules(ctx context.Context) ([]TableRules, error) {
	maskingRules, err := a.getMaskingRules(ctx)
	if err != nil {
		return nil, err
	}

	if len(maskingRules) == 0 {
		return nil, nil
	}

	rules := make([]TableRules, 0, len(maskingRules))
	// use a map to group all column rules for a table together
	schemaTableRuleMap := make(map[string]TableRules, len(maskingRules))
	for _, rule := range maskingRules {
		ruleLogFields := loglib.Fields{
			"schema":   rule.schema,
			"table":    rule.table,
			"column":   rule.column,
			"function": rule.function,
		}
		if rule.schema == "" || rule.table == "" || rule.column == "" || rule.function == "" {
			a.logger.Warn(nil, "skipping invalid anon masking rule, schema, table, column and function must be defined", ruleLogFields)
			continue
		}
		tableKey := schemaTableKey(rule.schema, rule.table)
		tableRules, exists := schemaTableRuleMap[tableKey]
		if !exists {
			tableRules = TableRules{
				Schema:      rule.schema,
				Table:       rule.table,
				ColumnRules: make(map[string]TransformerRules),
			}
		}

		transformerRules, err := a.anonMaskingToTransformerRules(rule)
		if err != nil {
			a.logger.Warn(err, "skipping invalid anon masking rule", ruleLogFields)
			continue
		}

		tableRules.ColumnRules[rule.column] = transformerRules
		schemaTableRuleMap[tableKey] = tableRules
	}

	for _, tableRules := range schemaTableRuleMap {
		rules = append(rules, tableRules)
	}

	return rules, nil
}

func (a *AnonRuleParser) Close() error {
	return a.conn.Close(context.Background())
}

const anonMaskingRulesQuery = "SELECT relnamespace,relname,attname,masking_function FROM anon.pg_masking_rules;"

func (a *AnonRuleParser) getMaskingRules(ctx context.Context) ([]maskingRule, error) {
	rows, err := a.conn.Query(ctx, anonMaskingRulesQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	maskingRules := []maskingRule{}

	for rows.Next() {
		var maskingRule maskingRule
		if err := rows.Scan(&maskingRule.schema, &maskingRule.table, &maskingRule.column, &maskingRule.function); err != nil {
			return nil, err
		}
		maskingRules = append(maskingRules, maskingRule)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return maskingRules, nil
}

func (a *AnonRuleParser) anonMaskingToTransformerRules(rule maskingRule) (TransformerRules, error) {
	parametersList := []string{}
	fnName, parameters, found := strings.Cut(rule.function, "(")
	if found {
		parameters = strings.TrimSuffix(parameters, ")")
		if parameters != "" {
			parametersList = strings.Split(parameters, ",")
		}
	}

	switch fnName {
	case "anon.random_hash",
		"anon.hash",
		"anon.partial_email":
		return TransformerRules{
			Name: string(transformers.PGAnonymizer),
			Parameters: map[string]any{
				"anon_function": fnName,
			},
		}, nil

	case "anon.digest":
		// extract the salt and algorithm
		if len(parametersList) != 3 {
			return TransformerRules{}, fmt.Errorf("anon.digest(value, salt, algorithm): %w", errMissingParameters)
		}
		salt := trimParameter(parametersList[1])
		algorithm := trimParameter(parametersList[2])
		return TransformerRules{
			Name: string(transformers.PGAnonymizer),
			Parameters: map[string]any{
				"anon_function": fnName,
				"salt":          salt,
				"algorithm":     algorithm,
			},
		}, nil

	case "anon.noise":
		// extract the noise ratio
		if len(parametersList) != 2 {
			return TransformerRules{}, fmt.Errorf("anon.noise(original_value, ratio): %w", errMissingParameters)
		}
		ratio, err := strconv.ParseFloat(trimParameter(parametersList[1]), 64)
		if err != nil {
			return TransformerRules{}, fmt.Errorf("parsing ratio parameter to float: %w: %w", err, errInvalidParameter)
		}
		return TransformerRules{
			Name: string(transformers.PGAnonymizer),
			Parameters: map[string]any{
				"anon_function": fnName,
				"ratio":         ratio,
			},
		}, nil

	case "anon.dnoise":
		// extract the noise interval
		if len(parametersList) != 2 {
			return TransformerRules{}, fmt.Errorf("anon.dnoise(original_value, interval): %w", errMissingParameters)
		}
		interval := trimParameter(parametersList[1])
		return TransformerRules{
			Name: string(transformers.PGAnonymizer),
			Parameters: map[string]any{
				"anon_function": fnName,
				"interval":      interval,
			},
		}, nil

	case "anon.image_blur":
		// extract the blur sigma
		if len(parametersList) != 2 {
			return TransformerRules{}, fmt.Errorf("anon.image_blur(data, sigma): %w", errMissingParameters)
		}
		sigma, err := strconv.ParseFloat(trimParameter(parametersList[1]), 64)
		if err != nil {
			return TransformerRules{}, fmt.Errorf("parsing sigma parameter to float: %w: %w", err, errInvalidParameter)
		}
		return TransformerRules{
			Name: string(transformers.PGAnonymizer),
			Parameters: map[string]any{
				"anon_function": fnName,
				"sigma":         sigma,
			},
		}, nil

	case "anon.partial":
		if len(parametersList) != 4 {
			return TransformerRules{}, fmt.Errorf("anon.partial(value, mask_prefix_count, mask, mask_suffix_count): %w", errMissingParameters)
		}

		prefixCount, err := strconv.ParseInt(trimParameter(parametersList[1]), 10, 64)
		if err != nil {
			return TransformerRules{}, fmt.Errorf("parsing mask_prefix_count parameter to int: %w: %w", err, errInvalidParameter)
		}
		suffixCount, err := strconv.ParseInt(trimParameter(parametersList[3]), 10, 64)
		if err != nil {
			return TransformerRules{}, fmt.Errorf("parsing mask_suffix_count parameter to int: %w: %w", err, errInvalidParameter)
		}
		mask := trimParameter(parametersList[2])

		return TransformerRules{
			Name: string(transformers.PGAnonymizer),
			Parameters: map[string]any{
				"anon_function":     fnName,
				"mask_prefix_count": int(prefixCount),
				"mask":              mask,
				"mask_suffix_count": int(suffixCount),
			},
		}, nil

	case "anon.random_in":
		if !strings.Contains(parameters, "ARRAY") {
			return TransformerRules{}, fmt.Errorf("anon.random_in(ARRAY[...]): %w", errMissingParameters)
		}

		return TransformerRules{
			Name: string(transformers.PGAnonymizer),
			Parameters: map[string]any{
				"anon_function": fnName,
				"range":         parameters,
			},
		}, nil

	case "anon.random_in_int4range",
		"anon.random_in_int8range",
		"anon.random_in_daterange",
		"anon.random_in_numrange",
		"anon.random_in_tsrange",
		"anon.random_in_tstzrange":
		// Make sure the range contains 2 parameters only.
		// Examples:
		// anon.random_in_int4range('[5,6)')
		if len(parametersList) != 2 {
			return TransformerRules{}, fmt.Errorf("%s(range): %w", fnName, errMissingParameters)
		}
		rangeParam := trimParameter(parameters)
		return TransformerRules{
			Name: string(transformers.PGAnonymizer),
			Parameters: map[string]any{
				"anon_function": fnName,
				"range":         rangeParam,
			},
		}, nil

	case "anon.random_in_enum":
		if len(parametersList) != 1 {
			return TransformerRules{}, fmt.Errorf("anon.random_in_enum(NULL::enum_type): %w", errMissingParameters)
		}
		return TransformerRules{
			Name: string(transformers.PGAnonymizer),
			Parameters: map[string]any{
				"anon_function": fnName,
				"range":         parameters,
			},
		}, nil

	case "anon.random_string":
		if len(parametersList) != 1 {
			return TransformerRules{}, fmt.Errorf("anon.random_string(length): %w", errMissingParameters)
		}
		count, err := strconv.ParseInt(trimParameter(parametersList[0]), 10, 64)
		if err != nil {
			return TransformerRules{}, fmt.Errorf("parsing count parameter to int: %w: %w", err, errInvalidParameter)
		}
		return TransformerRules{
			Name: string(transformers.PGAnonymizer),
			Parameters: map[string]any{
				"anon_function": fnName,
				"count":         int(count),
			},
		}, nil

	case "anon.random_phone":
		if len(parametersList) == 1 {
			prefix := trimParameter(parametersList[0])
			return TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": fnName,
					"prefix":        prefix,
				},
			}, nil
		}
		return TransformerRules{
			Name: string(transformers.PGAnonymizer),
			Parameters: map[string]any{
				"anon_function": fnName,
			},
		}, nil

	case "anon.random_date_between",
		"anon.random_int_between",
		"anon.random_bigint_between":
		if len(parametersList) != 2 {
			return TransformerRules{}, fmt.Errorf("anon.random_*_between(min, max): %w", errMissingParameters)
		}
		min := trimParameter(parametersList[0])
		max := trimParameter(parametersList[1])
		return TransformerRules{
			Name: string(transformers.PGAnonymizer),
			Parameters: map[string]any{
				"anon_function": fnName,
				"min":           min,
				"max":           max,
			},
		}, nil
	case "anon.lorem_ipsum":
		if len(parametersList) == 0 {
			return TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": fnName,
				},
			}, nil
		}

		param := trimParameter(parametersList[0])

		paramValues := strings.Split(param, " := ")
		switch len(paramValues) {
		case 1:
			count, err := strconv.ParseInt(trimParameter(paramValues[0]), 10, 64)
			if err != nil {
				return TransformerRules{}, fmt.Errorf("parsing count parameter to int: %w: %w", err, errInvalidParameter)
			}

			return TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": fnName,
					"count":         int(count),
				},
			}, nil
		case 2:
			count, err := strconv.ParseInt(trimParameter(paramValues[1]), 10, 64)
			if err != nil {
				return TransformerRules{}, fmt.Errorf("parsing count parameter to int: %w: %w", err, errInvalidParameter)
			}
			return TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": fnName,
					"unit":          trimParameter(paramValues[0]),
					"count":         int(count),
				},
			}, nil
		default:
			return TransformerRules{}, fmt.Errorf("anon.lorem_ipsum(count) or anon.lorem_ipsum(unit := count): %w", errTooManyParameters)
		}

	default:
		switch {
		case strings.HasPrefix(fnName, "anon.pseudo_"):
			// extract the salt if present
			salt := ""
			if len(parametersList) > 1 {
				salt = trimParameter(parametersList[1])
			}

			return TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": fnName,
					"salt":          salt,
				},
			}, nil

		case strings.HasPrefix(fnName, "anon.dummy") && strings.HasSuffix(fnName, "_locale"):
			// extract the locale if present
			locale := ""
			if len(parametersList) == 1 {
				locale = trimParameter(parametersList[0])
			}
			return TransformerRules{
				Name: string(transformers.PGAnonymizer),
				Parameters: map[string]any{
					"anon_function": fnName,
					"locale":        locale,
				},
			}, nil
		}

		return TransformerRules{
			Name: string(transformers.PGAnonymizer),
			Parameters: map[string]any{
				"anon_function": strings.TrimSuffix(rule.function, "()"),
			},
		}, nil
	}
}

func (a *AnonRuleParser) dumpRulesToFile(filename string, rules Rules) error {
	yamlRules, err := a.marshaler(rules)
	if err != nil {
		return fmt.Errorf("marshaling inferred anon transformation rules to yaml: %w", err)
	}

	if err := os.WriteFile(filename, yamlRules, 0o600); err != nil {
		return fmt.Errorf("writing inferred anon transformation rules to file %q: %w", filename, err)
	}

	return nil
}

func trimParameter(param string) string {
	return strings.Trim(strings.TrimSpace(param), "'")
}
