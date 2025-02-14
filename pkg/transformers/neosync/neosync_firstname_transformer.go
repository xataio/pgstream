// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"fmt"

	neosynctransformers "github.com/nucleuscloud/neosync/worker/pkg/benthos/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

type FirstNameTransformer struct {
	*transformer[string]
}

func NewFirstNameTransformer(params transformers.Parameters) (*FirstNameTransformer, error) {
	preserveLength, err := findParameter[bool](params, "preserve_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_firstname: preserve_length must be a boolean: %w", err)
	}

	maxLength, err := findParameter[int](params, "max_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_firstname: max_length must be an integer: %w", err)
	}

	seed, err := findParameter[int](params, "seed")
	if err != nil {
		return nil, fmt.Errorf("neosync_firstname: seed must be an integer: %w", err)
	}

	opts, err := neosynctransformers.NewTransformFirstNameOpts(toInt64Ptr(maxLength), preserveLength, toInt64Ptr(seed))
	if err != nil {
		return nil, err
	}

	return &FirstNameTransformer{
		transformer: New[string](neosynctransformers.NewTransformFirstName(), opts),
	}, nil
}
