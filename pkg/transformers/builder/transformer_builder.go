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

type TransformerDefinition struct {
	SupportedTypes []transformers.SupportedDataType
	Parameters     []transformers.Parameter
	Definition     *transformers.Definition
	BuildFn        func(*transformers.Config) (transformers.Transformer, error)
}

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

var TransformersMap = map[transformers.TransformerType]TransformerDefinition{
	transformers.Masking: {
		SupportedTypes: transformers.MaskingCompatibleTypes,
		Parameters:     transformers.MaskingParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return transformers.NewMaskingTransformer(cfg.Parameters)
		},
	},
	transformers.Template: {
		SupportedTypes: transformers.TemplateCompatibleTypes,
		Parameters:     transformers.TemplateParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return transformers.NewTemplateTransformer(cfg.Parameters)
		},
	},
	transformers.PhoneNumber: {
		SupportedTypes: transformers.PhoneNumberCompatibleTypes,
		Parameters:     transformers.PhoneNumberParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return transformers.NewPhoneNumberTransformer(cfg.Parameters, cfg.DynamicParameters)
		},
	},
	transformers.LiteralString: {
		SupportedTypes: transformers.LiteralStringCompatibleTypes,
		Parameters:     transformers.LiteralStringParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return transformers.NewLiteralStringTransformer(cfg.Parameters)
		},
	},
	transformers.String: {
		SupportedTypes: transformers.StringCompatibleTypes,
		Parameters:     transformers.StringParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return transformers.NewStringTransformer(cfg.Parameters)
		},
	},
	// Greenmask transformers
	transformers.GreenmaskString: {
		SupportedTypes: greenmask.StringCompatibleTypes,
		Parameters:     greenmask.StringParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewStringTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskFirstName: {
		SupportedTypes: greenmask.FirstNameCompatibleTypes,
		Parameters:     greenmask.FirstNameParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewFirstNameTransformer(cfg.Parameters, cfg.DynamicParameters)
		},
	},
	transformers.GreenmaskInteger: {
		SupportedTypes: greenmask.IntegerCompatibleTypes,
		Parameters:     greenmask.IntegerParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewIntegerTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskFloat: {
		SupportedTypes: greenmask.FloatCompatibleTypes,
		Parameters:     greenmask.FloatParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewFloatTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskUUID: {
		SupportedTypes: greenmask.UUIDCompatibleTypes,
		Parameters:     greenmask.UUIDParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewUUIDTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskBoolean: {
		SupportedTypes: greenmask.BooleanCompatibleTypes,
		Parameters:     greenmask.BooleanParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewBooleanTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskChoice: {
		SupportedTypes: greenmask.ChoiceCompatibleTypes,
		Parameters:     greenmask.ChoiceParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewChoiceTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskUnixTimestamp: {
		SupportedTypes: greenmask.UnixTimestampCompatibleTypes,
		Parameters:     greenmask.UnixTimestampParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewUnixTimestampTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskDate: {
		SupportedTypes: greenmask.DateCompatibleTypes,
		Parameters:     greenmask.DateParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewDateTransformer(cfg.Parameters)
		},
	},
	transformers.GreenmaskUTCTimestamp: {
		SupportedTypes: greenmask.UTCTimestampCompatibleTypes,
		Parameters:     greenmask.UTCTimestampParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return greenmask.NewUTCTimestampTransformer(cfg.Parameters)
		},
	},
	// Neosync transformers
	transformers.NeosyncString: {
		SupportedTypes: neosync.StringCompatibleTypes,
		Parameters:     neosync.StringParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return neosync.NewStringTransformer(cfg.Parameters)
		},
	},
	transformers.NeosyncFirstName: {
		SupportedTypes: neosync.FirstNameCompatibleTypes,
		Parameters:     neosync.FirstNameParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return neosync.NewFirstNameTransformer(cfg.Parameters)
		},
	},
	transformers.NeosyncLastName: {
		SupportedTypes: neosync.LastNameCompatibleTypes,
		Parameters:     neosync.LastNameParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return neosync.NewLastNameTransformer(cfg.Parameters)
		},
	},
	transformers.NeosyncFullName: {
		SupportedTypes: neosync.FullNameCompatibleTypes,
		Parameters:     neosync.FullNameParams,
		BuildFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			return neosync.NewFullNameTransformer(cfg.Parameters)
		},
	},
	transformers.NeosyncEmail: {
		SupportedTypes: neosync.EmailCompatibleTypes,
		Parameters:     neosync.EmailParams,
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

	transformerDef, ok := TransformersMap[cfg.Name]
	if !ok {
		return nil, fmt.Errorf("%w: unexpected transformer name '%s'", transformers.ErrUnsupportedTransformer, cfg.Name)
	}

	paramNames := make([]string, len(transformerDef.Parameters))
	for i, param := range transformerDef.Parameters {
		paramNames[i] = param.Name
	}

	if err := transformers.ValidateParameters(cfg.Parameters, paramNames); err != nil {
		return nil, err
	}
	return transformerDef.BuildFn(cfg)
}
