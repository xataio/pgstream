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

type FullNameTransformer struct {
	*transformer[string]
	randomizer     rng.Rand
	maxLength      int
	preserveLength bool
}

var (
	fullNameParams = []transformers.Parameter{
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
	fullNameCompatibleTypes = []transformers.SupportedDataType{
		transformers.StringDataType,
	}
	errFullNameLengthMustBeGreaterThanTwo = errors.New("neosync_fullname: max_length must be greater than 0")
)

func NewFullNameTransformer(params transformers.ParameterValues) (*FullNameTransformer, error) {
	preserveLengthParam, err := findParameter[bool](params, "preserve_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_fullname: preserve_length must be a boolean: %w", err)
	}
	preserveLength := bool(false)
	if preserveLengthParam != nil {
		preserveLength = *preserveLengthParam
	}

	maxLengthParam, err := findParameter[int](params, "max_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_fullname: max_length must be an integer: %w", err)
	}

	maxLength := int(100)
	if maxLengthParam != nil {
		maxLength = *maxLengthParam
	}

	if maxLength < 3 {
		return nil, errFullNameLengthMustBeGreaterThanTwo
	}

	seedParam, err := findParameter[int](params, "seed")
	if err != nil {
		return nil, fmt.Errorf("neosync_fullname: seed must be an integer: %w", err)
	}

	opts, err := neosynctransformers.NewTransformFullNameOpts(toInt64Ptr(maxLengthParam), preserveLengthParam, toInt64Ptr(seedParam))
	if err != nil {
		return nil, err
	}

	seed, err := transformer_utils.GetSeedOrDefault(toInt64Ptr(seedParam))
	if err != nil {
		// not expected, but handle it gracefully
		return nil, fmt.Errorf("neosync_fullname: unable to generate seed: %w", err)
	}
	return &FullNameTransformer{
		transformer:    New[string](neosynctransformers.NewTransformFullName(), opts),
		randomizer:     rng.New(seed),
		maxLength:      maxLength,
		preserveLength: preserveLength,
	}, nil
}

func (t *FullNameTransformer) PostCreate(param any) error {
	return nil
}

func (t *FullNameTransformer) Transform(ctx context.Context, value transformers.Value) (any, error) {
	var retVal string
	maxLength := t.maxLength
	if t.preserveLength {
		// If preserveLength is true, we need to ensure the generated name has the same length as the original value.
		valueStr, ok := value.TransformValue.(string)
		if !ok {
			// not expected, but handle it gracefully
			return nil, errors.New("neosync_fullname: transform value is not a string")
		}
		maxLength = len(valueStr)
	}

	if maxLength > 2 && maxLength < 5 {
		retVal = generateRandomStringAsFullName(t.maxLength, t.randomizer)
	} else {
		val, err := t.transformer.Transform(ctx, value)
		if err != nil {
			return val, err
		}
		retVal, _ = val.(string)
	}

	// In some edge cases, neosync may generate a full name that is longer than the required length.
	// To prevent that, we need to truncate it to the required length.
	if len(retVal) > maxLength {
		retVal = retVal[:maxLength]
	}
	return retVal, nil
}

func (t *FullNameTransformer) CompatibleTypes() []transformers.SupportedDataType {
	return fullNameCompatibleTypes
}

func (t *FullNameTransformer) Type() transformers.TransformerType {
	return transformers.NeosyncFullName
}

func (t *FullNameTransformer) Close() error {
	return nil
}

func FullNameTransformerDefinition() *transformers.Definition {
	return &transformers.Definition{
		SupportedTypes: fullNameCompatibleTypes,
		Parameters:     fullNameParams,
	}
}

// generateRandomStringAsFullName generates a random full name string based on the specified length.
// The length can be 3 or 4.
// If it's 3, it generates a first and last name with one letter each.
// If it's 4, it generates either a 3 or 4 character full name, equally likely to be either.
// If it decides to generate a 4 character name, a first and last name with one letter each and adds an extra letter to either the first or last name, randomly.
// If the length is not in this range, it returns an empty string. It's the caller's responsibility to ensure the length is valid.
func generateRandomStringAsFullName(length int, randomizer rng.Rand) string {
	if length < 3 || length > 4 {
		return ""
	}
	first := string(uppercaseLetters[randomizer.Intn(len(uppercaseLetters))])
	last := string(uppercaseLetters[randomizer.Intn(len(uppercaseLetters))])
	if length == 4 && randomizer.Intn(2) == 0 {
		extraLetter := string(lowercaseLetters[randomizer.Intn(len(uppercaseLetters))])
		if randomizer.Intn(2) == 0 {
			first += extraLetter
		} else {
			last += extraLetter
		}
	}
	return first + " " + last
}
