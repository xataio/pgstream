// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/transformers/greenmask"
	"github.com/xataio/pgstream/pkg/transformers/neosync"
)

func New(cfg *transformers.Config) (transformers.Transformer, error) {
	switch cfg.Name {
	case transformers.String:
		return transformers.NewStringTransformer(cfg.Generator, cfg.Parameters)
	case transformers.NeosyncString:
		return neosync.NewStringTransformer(cfg.Parameters)
	case transformers.GreenmaskString:
		return greenmask.NewStringTransformer(cfg.Generator, cfg.Parameters)
	case transformers.NeosyncFirstName:
		return neosync.NewFirstNameTransformer(cfg.Parameters)
	case transformers.GreenmaskFirstName:
		return greenmask.NewFirstNameTransformer(cfg.Generator, cfg.Parameters)
	case transformers.GreenmaskInteger:
		return greenmask.NewIntegerTransformer(cfg.Generator, cfg.Parameters)
	case transformers.GreenmaskFloat:
		return greenmask.NewFloatTransformer(cfg.Generator, cfg.Parameters)
	case transformers.GreenmaskUUID:
		return greenmask.NewUUIDTransformer(cfg.Generator)
	case transformers.GreenmaskBoolean:
		return greenmask.NewBooleanTransformer(cfg.Generator)
	default:
		return nil, transformers.ErrUnsupportedTransformer
	}
}
