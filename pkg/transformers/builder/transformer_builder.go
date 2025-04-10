// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"fmt"

	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/transformers/greenmask"
	"github.com/xataio/pgstream/pkg/transformers/neosync"
)

// ErrUnknownParameter is returned when unknown parameter is provided
var ErrUnknownParameter = fmt.Errorf("unknown parameter provided to transformer")

func New(cfg *transformers.Config) (transformers.Transformer, error) {
	switch cfg.Name {
	case transformers.GreenmaskString:
		if err := validateParameters(cfg.Parameters, greenmask.StringTransformerParams); err != nil {
			return nil, err
		}
		return greenmask.NewStringTransformer(cfg.Parameters)
	case transformers.GreenmaskFirstName:
		if err := validateParameters(cfg.Parameters, greenmask.FirstNameTransformerParams); err != nil {
			return nil, err
		}
		return greenmask.NewFirstNameTransformer(cfg.Parameters, cfg.DynamicParameters)
	case transformers.GreenmaskInteger:
		if err := validateParameters(cfg.Parameters, greenmask.IntegerTransformerParams); err != nil {
			return nil, err
		}
		return greenmask.NewIntegerTransformer(cfg.Parameters)
	case transformers.GreenmaskFloat:
		if err := validateParameters(cfg.Parameters, greenmask.FloatTransformerParams); err != nil {
			return nil, err
		}
		return greenmask.NewFloatTransformer(cfg.Parameters)
	case transformers.GreenmaskUUID:
		if err := validateParameters(cfg.Parameters, greenmask.UUIDTransformerParams); err != nil {
			return nil, err
		}
		return greenmask.NewUUIDTransformer(cfg.Parameters)
	case transformers.GreenmaskBoolean:
		if err := validateParameters(cfg.Parameters, greenmask.BooleanTransformerParams); err != nil {
			return nil, err
		}
		return greenmask.NewBooleanTransformer(cfg.Parameters)
	case transformers.GreenmaskChoice:
		if err := validateParameters(cfg.Parameters, greenmask.ChoiceTransformerParams); err != nil {
			return nil, err
		}
		return greenmask.NewChoiceTransformer(cfg.Parameters)
	case transformers.GreenmaskUnixTimestamp:
		if err := validateParameters(cfg.Parameters, greenmask.UnixTimestampTransformerParams); err != nil {
			return nil, err
		}
		return greenmask.NewUnixTimestampTransformer(cfg.Parameters)
	case transformers.GreenmaskDate:
		if err := validateParameters(cfg.Parameters, greenmask.DateTransformerParams); err != nil {
			return nil, err
		}
		return greenmask.NewDateTransformer(cfg.Parameters)
	case transformers.GreenmaskUTCTimestamp:
		if err := validateParameters(cfg.Parameters, greenmask.UTCTimestampTransformerParams); err != nil {
			return nil, err
		}
		return greenmask.NewUTCTimestampTransformer(cfg.Parameters)
	case transformers.String:
		if err := validateParameters(cfg.Parameters, transformers.StringTransformerParams); err != nil {
			return nil, err
		}
		return transformers.NewStringTransformer(cfg.Parameters)
	case transformers.NeosyncString:
		if err := validateParameters(cfg.Parameters, neosync.StringTransformerParams); err != nil {
			return nil, err
		}
		return neosync.NewStringTransformer(cfg.Parameters)
	case transformers.NeosyncFirstName:
		if err := validateParameters(cfg.Parameters, neosync.FirstNameTransformerParams); err != nil {
			return nil, err
		}
		return neosync.NewFirstNameTransformer(cfg.Parameters)
	case transformers.NeosyncEmail:
		if err := validateParameters(cfg.Parameters, neosync.EmailTransformerParams); err != nil {
			return nil, err
		}
		return neosync.NewEmailTransformer(cfg.Parameters)
	case transformers.PhoneNumber:
		if err := validateParameters(cfg.Parameters, transformers.PhoneNumberTransformerParams); err != nil {
			return nil, err
		}
		return transformers.NewPhoneNumberTransformer(cfg.Parameters)
	case transformers.Masking:
		if err := validateParameters(cfg.Parameters, transformers.MaskingTransformerParams); err != nil {
			return nil, err
		}
		return transformers.NewMaskingTransformer(cfg.Parameters)
	default:
		return nil, transformers.ErrUnsupportedTransformer
	}
}

// validateParameters checks if all provided parameters are in the expected set
func validateParameters(provided map[string]any, expected []string) error {
	expectedMap := make(map[string]struct{})
	for _, param := range expected {
		expectedMap[param] = struct{}{}
	}

	for key := range provided {
		if _, ok := expectedMap[key]; !ok {
			return fmt.Errorf("%w: unexpected parameter '%s'", ErrUnknownParameter, key)
		}
	}

	return nil
}
