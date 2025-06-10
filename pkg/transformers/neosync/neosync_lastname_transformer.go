// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"context"
	"errors"
	"fmt"

	neosynctransformers "github.com/nucleuscloud/neosync/worker/pkg/benthos/transformers"
	transformer_utils "github.com/nucleuscloud/neosync/worker/pkg/benthos/transformers/utils"
	"github.com/nucleuscloud/neosync/worker/pkg/rng"
	"github.com/xataio/pgstream/pkg/transformers"
)

type LastNameTransformer struct {
	*transformer[string]
	randomizer rng.Rand
}

var (
	lastNameParams = []transformers.Parameter{
		{
			Name:          "seed",
			SupportedType: "int",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "preserve_length",
			SupportedType: "boolean",
			Default:       false,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "max_length",
			SupportedType: "int",
			Default:       100,
			Dynamic:       false,
			Required:      false,
		},
	}
	lastNameCompatibleTypes = []transformers.SupportedDataType{
		transformers.StringDataType,
	}
	errLastNameLengthMustBeGreaterThanZero = errors.New("neosync_lastname: max_length must be greater than 0")
)

func NewLastNameTransformer(params transformers.ParameterValues) (*LastNameTransformer, error) {
	preserveLength, err := findParameter[bool](params, "preserve_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_lastname: preserve_length must be a boolean: %w", err)
	}

	maxLength, err := findParameter[int](params, "max_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_lastname: max_length must be an integer: %w", err)
	}

	if maxLength != nil && *maxLength < 1 {
		return nil, errLastNameLengthMustBeGreaterThanZero
	}

	seedParam, err := findParameter[int](params, "seed")
	if err != nil {
		return nil, fmt.Errorf("neosync_firstname: seed must be an integer: %w", err)
	}

	opts, err := neosynctransformers.NewTransformLastNameOpts(toInt64Ptr(maxLength), preserveLength, toInt64Ptr(seedParam))
	if err != nil {
		return nil, err
	}

	seed, err := transformer_utils.GetSeedOrDefault(toInt64Ptr(seedParam))
	if err != nil {
		// not expected, but handle it gracefully
		return nil, fmt.Errorf("neosync_firstname: unable to generate seed: %w", err)
	}

	return &LastNameTransformer{
		transformer: New[string](neosynctransformers.NewTransformLastName(), opts),
		randomizer:  rng.New(seed),
	}, nil
}

func (t *LastNameTransformer) Transform(ctx context.Context, value transformers.Value) (any, error) {
	transformedValue, err := t.transformer.Transform(ctx, value)
	if err != nil {
		var errSingleChar *errSingleCharName
		if errors.As(err, &errSingleChar) {
			return string(uppercaseLetters[t.randomizer.Intn(len(uppercaseLetters))]), nil
		}
	}
	return transformedValue, err
}

func (t *LastNameTransformer) CompatibleTypes() []transformers.SupportedDataType {
	return lastNameCompatibleTypes
}

func (t *LastNameTransformer) Type() transformers.TransformerType {
	return transformers.NeosyncLastName
}

func LastNameTransformerDefinition() *transformers.Definition {
	return &transformers.Definition{
		SupportedTypes: lastNameCompatibleTypes,
		Parameters:     lastNameParams,
	}
}
