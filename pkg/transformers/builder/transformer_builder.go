// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"strings"

	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/transformers/greenmask"
	"github.com/xataio/pgstream/pkg/transformers/neosync"
)

func New(cfg *transformers.Config) (transformers.Transformer, error) {
	if isGreenmaskTransformer(cfg.Name) {
		var err error
		generatorType, err := greenmask.GetGeneratorType(cfg.Parameters)
		if err != nil {
			return nil, err
		}

		switch cfg.Name {
		case transformers.GreenmaskString:
			return greenmask.NewStringTransformer(generatorType, cfg.Parameters)
		case transformers.GreenmaskFirstName:
			return greenmask.NewFirstNameTransformer(generatorType, cfg.Parameters)
		case transformers.GreenmaskInteger:
			return greenmask.NewIntegerTransformer(generatorType, cfg.Parameters)
		case transformers.GreenmaskFloat:
			return greenmask.NewFloatTransformer(generatorType, cfg.Parameters)
		case transformers.GreenmaskUUID:
			return greenmask.NewUUIDTransformer(generatorType)
		case transformers.GreenmaskBoolean:
			return greenmask.NewBooleanTransformer(generatorType)
		case transformers.GreenmaskChoice:
			return greenmask.NewChoiceTransformer(generatorType, cfg.Parameters)
		case transformers.GreenmaskUnixTimestamp:
			return greenmask.NewUnixTimestampTransformer(generatorType, cfg.Parameters)
		case transformers.GreenmaskDate:
			return greenmask.NewDateTransformer(generatorType, cfg.Parameters)
		case transformers.GreenmaskUTCTimestamp:
			return greenmask.NewUTCTimestampTransformer(generatorType, cfg.Parameters)
		default:
			return nil, transformers.ErrUnsupportedTransformer
		}
	}

	switch cfg.Name {
	case transformers.String:
		return transformers.NewStringTransformer(cfg.Parameters)
	case transformers.NeosyncString:
		return neosync.NewStringTransformer(cfg.Parameters)
	case transformers.NeosyncFirstName:
		return neosync.NewFirstNameTransformer(cfg.Parameters)
	case transformers.NeosyncEmail:
		return neosync.NewEmailTransformer(cfg.Parameters)
	default:
		return nil, transformers.ErrUnsupportedTransformer
	}
}

func isGreenmaskTransformer(name transformers.TransformerType) bool {
	return strings.HasPrefix(string(name), "greenmask_")
}
