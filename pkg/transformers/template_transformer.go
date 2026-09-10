// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/xataio/pgstream/pkg/transformers/internal/pool"
	"github.com/xataio/pgstream/pkg/transformers/internal/template"
)

type TemplateTransformer struct {
	template *template.Template
	// typeMaps encode the pgx values a snapshot row carries as postgres text.
	// A pgtype.Map memoises encode plans without locking, so each execution
	// takes its own instance from the pool.
	typeMaps *pool.Pool[*pgtype.Map]
}

var (
	errTemplateMustBeProvided = errors.New("template_transformer: template parameter must be provided")
	templateCompatibleTypes   = []SupportedDataType{
		AllDataTypes,
	}
	templateParams = []Parameter{
		{
			Name:          "template",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
	}
)

// templateHstoreOID stands in for the hstore OID, which the extension assigns
// per database. Text encoding only needs the codec, so any unused OID works.
const templateHstoreOID = 1 << 31

func NewTemplateTransformer(params ParameterValues) (*TemplateTransformer, error) {
	templateStr, found, err := FindParameter[string](params, "template")
	if err != nil {
		return nil, fmt.Errorf("template_transformer: template must be a string: %w", err)
	}
	if !found {
		return nil, errTemplateMustBeProvided
	}

	tmpl, err := template.New("", templateStr)
	if err != nil {
		return nil, fmt.Errorf("template_transformer: error parsing template: %w", err)
	}

	typeMaps, err := pool.New(newTemplateTypeMap)
	if err != nil {
		return nil, fmt.Errorf("template_transformer: error creating type map: %w", err)
	}

	return &TemplateTransformer{template: tmpl, typeMaps: typeMaps}, nil
}

func newTemplateTypeMap() (*pgtype.Map, error) {
	m := pgtype.NewMap()
	m.RegisterType(&pgtype.Type{Name: "hstore", OID: templateHstoreOID, Codec: pgtype.HstoreCodec{}})
	return m, nil
}

func (t *TemplateTransformer) Transform(_ context.Context, value Value) (any, error) {
	typeMap, err := t.typeMaps.Acquire()
	if err != nil {
		return nil, fmt.Errorf("template_transformer: error acquiring type map: %w", err)
	}
	defer t.typeMaps.Release(typeMap)

	value.TransformValue = templateRenderable(typeMap, value.TransformValue, value.TransformType)
	for name, dynValue := range value.DynamicValues {
		value.DynamicValues[name] = templateRenderable(typeMap, dynValue, "")
	}

	var buf strings.Builder
	if err := t.template.Execute(&buf, &value); err != nil {
		return nil, fmt.Errorf("template_transformer: error executing template: %w", err)
	}
	return buf.String(), nil
}

// templateRenderable converts the pgx values a snapshot row carries into the
// postgres text the replication path already produces for the same column,
// so a template renders the same value from either source. Text, numbers and
// booleans are returned untouched, as is any value with no known text codec.
//
// The column type is unknown for dynamic values, so those are matched by Go
// type instead. time.Time is left alone there so the sprig date functions can
// still take a dynamic value from a date or timestamp column.
func templateRenderable(typeMap *pgtype.Map, value any, pgType string) any {
	switch value.(type) {
	case nil, string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return value
	}

	var pgTypeInfo *pgtype.Type
	var found bool
	switch v := value.(type) {
	case pgtype.Range[any]:
		return rangeText(typeMap, v, pgType)
	case time.Time:
		if pgType == "" {
			return value
		}
		pgTypeInfo, found = typeMap.TypeForName(pgType)
	case [16]byte:
		// pgx registers no Go type lookup for a bare uuid byte array
		pgTypeInfo, found = typeMap.TypeForName("uuid")
	default:
		if pgType != "" {
			pgTypeInfo, found = typeMap.TypeForName(pgType)
		} else {
			pgTypeInfo, found = typeMap.TypeForValue(value)
		}
	}
	if !found {
		return value
	}

	text, err := typeMap.Encode(pgTypeInfo.OID, pgtype.TextFormatCode, value, nil)
	if err != nil || text == nil {
		return value
	}
	return string(text)
}

// rangeElementTypes maps a postgres range type to the type of its bounds.
var rangeElementTypes = map[string]string{
	"int4range": "int4",
	"int8range": "int8",
	"numrange":  "numeric",
	"daterange": "date",
	"tsrange":   "timestamp",
	"tstzrange": "timestamptz",
}

// rangeText renders a range the way postgres prints it. pgx cannot encode a
// range whose bounds are plain Go values, which is what a snapshot row
// carries, so the bounds are rendered one by one instead.
func rangeText(typeMap *pgtype.Map, r pgtype.Range[any], pgType string) any {
	if !r.Valid {
		return r
	}
	if r.LowerType == pgtype.Empty {
		return "empty"
	}

	elemType := rangeElementTypes[pgType]
	bound := func(v any) string {
		if t, isTime := v.(time.Time); isTime && elemType == "" {
			if text, err := typeMap.Encode(pgtype.TimestamptzOID, pgtype.TextFormatCode, t, nil); err == nil {
				return string(text)
			}
		}
		return fmt.Sprint(templateRenderable(typeMap, v, elemType))
	}

	var b strings.Builder
	if r.LowerType == pgtype.Inclusive {
		b.WriteByte('[')
	} else {
		b.WriteByte('(')
	}
	if r.LowerType != pgtype.Unbounded {
		b.WriteString(bound(r.Lower))
	}
	b.WriteByte(',')
	if r.UpperType != pgtype.Unbounded {
		b.WriteString(bound(r.Upper))
	}
	if r.UpperType == pgtype.Inclusive {
		b.WriteByte(']')
	} else {
		b.WriteByte(')')
	}
	return b.String()
}

func (t *TemplateTransformer) CompatibleTypes() []SupportedDataType {
	return templateCompatibleTypes
}

func (t *TemplateTransformer) Type() TransformerType {
	return Template
}

func (t *TemplateTransformer) IsDynamic() bool {
	return true
}

func (t *TemplateTransformer) Uniqueness() Uniqueness {
	return UniquenessNotGuaranteed
}

func (t *TemplateTransformer) Close() error {
	return nil
}

func TemplateTransformerDefinition() *Definition {
	return &Definition{
		SupportedTypes: templateCompatibleTypes,
		Parameters:     templateParams,
		Uniqueness:     UniquenessNotGuaranteed,
	}
}
