// SPDX-License-Identifier: Apache-2.0

package neosync

import (
	"errors"
	"fmt"
	"slices"

	neosynctransformers "github.com/nucleuscloud/neosync/worker/pkg/benthos/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

type EmailTransformer struct {
	*transformer[string]
}

var (
	errInvalidEmailType          = errors.New("email_type must be one of 'uuidv4', 'fullname' or 'any'")
	errInvalidInvalidEmailAction = errors.New("invalid_email_action must be one of 'reject', 'passthrough', 'null' or 'generate'")

	validEmailTypes = []string{
		neosynctransformers.GenerateEmailType_UuidV4.String(),
		neosynctransformers.GenerateEmailType_FullName.String(),
		neosynctransformers.GenerateEmailType_Any.String(),
	}
	validInvalidEmailActions = []string{
		neosynctransformers.InvalidEmailAction_Reject.String(),
		neosynctransformers.InvalidEmailAction_Passthrough.String(),
		neosynctransformers.InvalidEmailAction_Null.String(),
		neosynctransformers.InvalidEmailAction_Generate.String(),
	}
)

func NewEmailTransformer(params transformers.Parameters) (*EmailTransformer, error) {
	preserveLength, err := findParameter[bool](params, "preserve_length")
	if err != nil {
		return nil, fmt.Errorf("neosync_email: preserve_length must be a boolean: %w", err)
	}

	preserveDomain, err := findParameter[bool](params, "preserve_domain")
	if err != nil {
		return nil, fmt.Errorf("neosync_email: preserve_domain must be a boolean: %w", err)
	}

	excludedDomains, err := findParameterArray[string](params, "excluded_domains")
	if err != nil {
		return nil, fmt.Errorf("neosync_email: excluded_domains must be type of []string: %w", err)
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
	if emailType != nil && !slices.Contains(validEmailTypes, *emailType) {
		return nil, errInvalidEmailType
	}

	invalidEmailAction, err := findParameter[string](params, "invalid_email_action")
	if err != nil {
		return nil, fmt.Errorf("neosync_email: invalid_email_action must be a string: %w", err)
	}
	if invalidEmailAction != nil && !slices.Contains(validInvalidEmailActions, *invalidEmailAction) {
		return nil, errInvalidInvalidEmailAction
	}

	opts, err := neosynctransformers.NewTransformEmailOpts(preserveLength, preserveDomain, toAnyPtr(excludedDomains), toInt64Ptr(maxLength), toInt64Ptr(seed), emailType, invalidEmailAction)
	if err != nil {
		return nil, err
	}

	return &EmailTransformer{
		transformer: New[string](neosynctransformers.NewTransformEmail(), opts),
	}, nil
}
