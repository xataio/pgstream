// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"fmt"

	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/transformers/greenmask"
	"github.com/xataio/pgstream/pkg/transformers/instrumentation"
	"github.com/xataio/pgstream/pkg/transformers/neosync"
)

type TransformerBuilder struct {
	instrumentation *otel.Instrumentation
}

type Option func(b *TransformerBuilder)

func NewTransformerBuilder(opts ...Option) *TransformerBuilder {
	b := &TransformerBuilder{}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func WithInstrumentation(i *otel.Instrumentation) Option {
	return func(b *TransformerBuilder) {
		b.instrumentation = i
	}
}

var TransformersMap = map[transformers.TransformerType]struct {
	Definition *transformers.Definition
	BuildFn    func(cfg *transformers.Config) (transformers.Transformer, error)
}{
	transformers.Masking: {
		Definition: transformers.MaskingTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return transformers.NewMaskingTransformer(cfg.Parameters)
		},
	},
	transformers.Email: {
		Definition: transformers.EmailTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return transformers.NewEmailTransformer(cfg.Parameters)
		},
	},
	transformers.Template: {
		Definition: transformers.TemplateTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return transformers.NewTemplateTransformer(cfg.Parameters)
		},
	},
	transformers.JSON: {
		Definition: transformers.JSONTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return transformers.NewJSONTransformer(cfg.Parameters)
		},
	},
	transformers.Hstore: {
		Definition: transformers.HstoreTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return transformers.NewHstoreTransformer(cfg.Parameters)
		},
	},
	transformers.PhoneNumber: {
		Definition: transformers.PhoneNumberTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return transformers.NewPhoneNumberTransformer(cfg.Parameters, cfg.DynamicParameters)
		},
	},
	transformers.LiteralString: {
		Definition: transformers.LiteralStringTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return transformers.NewLiteralStringTransformer(cfg.Parameters)
		},
	},
	transformers.String: {
		Definition: transformers.StringTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return transformers.NewStringTransformer(cfg.Parameters)
		},
	},
	// Greenmask transformers
	transformers.GreenmaskString: {
		Definition: greenmask.StringTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewStringTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskFirstName: {
		Definition: greenmask.FirstNameTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewFirstNameTransformer(cfg.Parameters, cfg.DynamicParameters)
		},
	},
	transformers.GreenmaskInteger: {
		Definition: greenmask.IntegerTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewIntegerTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskFloat: {
		Definition: greenmask.FloatTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewFloatTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskUUID: {
		Definition: greenmask.UUIDTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewUUIDTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskBoolean: {
		Definition: greenmask.BooleanTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewBooleanTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskChoice: {
		Definition: greenmask.ChoiceTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewChoiceTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskUnixTimestamp: {
		Definition: greenmask.UnixTimestampTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewUnixTimestampTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskDate: {
		Definition: greenmask.DateTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewDateTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskUTCTimestamp: {
		Definition: greenmask.UTCTimestampTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewUTCTimestampTransformer(cfg.Parameters)
		},
	},
	// Neosync transformers
	transformers.NeosyncString: {
		Definition: neosync.StringTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return neosync.NewStringTransformer(cfg.Parameters)
		},
	},
	transformers.NeosyncFirstName: {
		Definition: neosync.FirstNameTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return neosync.NewFirstNameTransformer(cfg.Parameters)
		},
	},
	transformers.NeosyncLastName: {
		Definition: neosync.LastNameTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return neosync.NewLastNameTransformer(cfg.Parameters)
		},
	},
	transformers.NeosyncFullName: {
		Definition: neosync.FullNameTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return neosync.NewFullNameTransformer(cfg.Parameters)
		},
	},
	transformers.NeosyncEmail: {
		Definition: neosync.EmailTransformerDefinition(),
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return neosync.NewEmailTransformer(cfg.Parameters)
		},
	},
}

func (b *TransformerBuilder) New(cfg *transformers.Config) (t transformers.Transformer, err error) {
	defer func() {
		if b.instrumentation != nil {
			t, err = instrumentation.NewTransformer(t, b.instrumentation)
		}
	}()

	transformer, ok := TransformersMap[cfg.Name]
	if !ok {
		return nil, fmt.Errorf("%w: unexpected transformer name '%s'", transformers.ErrUnsupportedTransformer, cfg.Name)
	}

	paramNames := make([]string, len(transformer.Definition.Parameters))
	for i, param := range transformer.Definition.Parameters {
		paramNames[i] = param.Name
	}

	if err := transformers.ValidateParameters(cfg.Parameters, paramNames); err != nil {
		return nil, err
	}
	return transformer.BuildFn(cfg)
}
