// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"fmt"

	neosynctransformers "github.com/nucleuscloud/neosync/worker/pkg/benthos/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

type StringTransformer struct {
	*transformer[string]
}

func NewStringTransformer(params transformers.Parameters) (*StringTransformer, error) {
	preserveLength, err := findParameter[bool](params, "preserve_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_string: preserve_length must be a boolean: %w", err)
	}

	minLength, err := findParameter[int](params, "min_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_string: min_length must be an integer: %w", err)
	}

	maxLength, err := findParameter[int](params, "max_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_string: max_length must be an integer: %w", err)
	}

	seed, err := findParameter[int](params, "seed")
	if err != nil {
		return nil, fmt.Errorf("neosync_string: seed must be an integer: %w", err)
	}

	opts, err := neosynctransformers.NewTransformStringOpts(preserveLength, toInt64Ptr(minLength), toInt64Ptr(maxLength), toInt64Ptr(seed))
	if err != nil {
		return nil, err
	}

	return &StringTransformer{
		transformer: New[string](neosynctransformers.NewTransformString(), opts),
	}, nil
}
