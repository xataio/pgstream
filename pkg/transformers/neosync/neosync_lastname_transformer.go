// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"fmt"

	neosynctransformers "github.com/nucleuscloud/neosync/worker/pkg/benthos/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

type LastNameTransformer struct {
	*transformer[string]
}

var lastNameTransformerParams = []string{"preserve_length", "max_length", "seed"}

func NewLastNameTransformer(params transformers.Parameters) (*LastNameTransformer, error) {
	if err := transformers.ValidateParameters(params, lastNameTransformerParams); err != nil {
		return nil, err
	}

	preserveLength, err := findParameter[bool](params, "preserve_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_lastname: preserve_length must be a boolean: %w", err)
	}

	maxLength, err := findParameter[int](params, "max_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_lastname: max_length must be an integer: %w", err)
	}

	seed, err := findParameter[int](params, "seed")
	if err != nil {
		return nil, fmt.Errorf("neosync_lastname: seed must be an integer: %w", err)
	}

	opts, err := neosynctransformers.NewTransformLastNameOpts(toInt64Ptr(maxLength), preserveLength, toInt64Ptr(seed))
	if err != nil {
		return nil, err
	}

	return &LastNameTransformer{
		transformer: New[string](neosynctransformers.NewTransformLastName(), opts),
	}, nil
}

func (t *LastNameTransformer) CompatibleTypes() []transformers.SupportedDataType {
	return []transformers.SupportedDataType{
		transformers.StringDataType,
	}
}
