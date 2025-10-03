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

type FirstNameTransformer struct {
	*transformer[string]
	randomizer rng.Rand
}

var (
	firstNameParams = []transformers.Parameter{
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
	firstNameCompatibleTypes = []transformers.SupportedDataType{
		transformers.StringDataType,
	}

	errFirstNameLengthMustBeGreaterThanZero = errors.New("neosync_firstname: max_length must be greater than 0")
)

func NewFirstNameTransformer(params transformers.ParameterValues) (*FirstNameTransformer, error) {
	preserveLength, err := findParameter[bool](params, "preserve_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_firstname: preserve_length must be a boolean: %w", err)
	}

	maxLength, err := findParameter[int](params, "max_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_firstname: max_length must be an integer: %w", err)
	}

	if maxLength != nil && *maxLength < 1 {
		return nil, errFirstNameLengthMustBeGreaterThanZero
	}

	seedParam, err := findParameter[int](params, "seed")
	if err != nil {
		return nil, fmt.Errorf("neosync_firstname: seed must be an integer: %w", err)
	}

	opts, err := neosynctransformers.NewTransformFirstNameOpts(toInt64Ptr(maxLength), preserveLength, toInt64Ptr(seedParam))
	if err != nil {
		return nil, err
	}

	seed, err := transformer_utils.GetSeedOrDefault(toInt64Ptr(seedParam))
	if err != nil {
		// not expected, but handle it gracefully
		return nil, fmt.Errorf("neosync_firstname: unable to generate seed: %w", err)
	}

	return &FirstNameTransformer{
		transformer: New[string](neosynctransformers.NewTransformFirstName(), opts),
		randomizer:  rng.New(seed),
	}, nil
}

func (t *FirstNameTransformer) PostCreate(param any) error {
	return nil
}

func (t *FirstNameTransformer) Transform(ctx context.Context, value transformers.Value) (any, error) {
	transformedValue, err := t.transformer.Transform(ctx, value)
	if err != nil {
		var errSingleChar *errSingleCharName
		if errors.As(err, &errSingleChar) {
			return string(uppercaseLetters[t.randomizer.Intn(len(uppercaseLetters))]), nil
		}
	}
	return transformedValue, err
}

func (t *FirstNameTransformer) CompatibleTypes() []transformers.SupportedDataType {
	return firstNameCompatibleTypes
}

func (t *FirstNameTransformer) Type() transformers.TransformerType {
	return transformers.NeosyncFirstName
}

func (t *FirstNameTransformer) Close() error {
	return nil
}

func FirstNameTransformerDefinition() *transformers.Definition {
	return &transformers.Definition{
		SupportedTypes: firstNameCompatibleTypes,
		Parameters:     firstNameParams,
	}
}
