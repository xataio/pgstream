// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"fmt"

	neosynctransformers "github.com/nucleuscloud/neosync/worker/pkg/benthos/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

type EmailTransformer struct {
	*transformer[string]
}

func NewEmailTransformer(params transformers.Parameters) (*EmailTransformer, error) {
	preserveLength, err := findParameter[bool](params, "preserve_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_email: preserve_length must be a boolean: %w", err)
	}

	preserveDomain, err := findParameter[bool](params, "preserve_domain")
	if err != nil {
		return nil, fmt.Errorf("neosync_email: preserve_domain must be a boolean: %w", err)
	}

	excludedDomains, err := findParameter[any](params, "excluded_domains")
	if err != nil {
		return nil, fmt.Errorf("neosync_email: excluded_domains must be a list of string: %w", err)
	}

	maxLength, err := findParameter[int](params, "max_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_email: max_length must be an integer: %w", err)
	}

	seed, err := findParameter[int](params, "seed")
	if err != nil {
		return nil, fmt.Errorf("neosync_email: seed must be an integer: %w", err)
	}

	emailType, err := findParameter[string](params, "email_type")
	if err != nil {
		return nil, fmt.Errorf("neosync_email: email_type must be a string: %w", err)
	}

	invalidEmailAction, err := findParameter[string](params, "invalid_email_action")
	if err != nil {
		return nil, fmt.Errorf("neosync_email: invalid_email_action must be a string: %w", err)
	}

	opts, err := neosynctransformers.NewTransformEmailOpts(preserveLength, preserveDomain, excludedDomains, toInt64Ptr(maxLength), toInt64Ptr(seed), emailType, invalidEmailAction)
	if err != nil {
		return nil, err
	}

	return &EmailTransformer{
		transformer: New[string](neosynctransformers.NewTransformEmail(), opts),
	}, nil
}
