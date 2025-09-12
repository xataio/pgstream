// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"bytes"
	"fmt"
	"text/template"

	"github.com/jackc/pgx/v5/pgtype"
)

type hstoreOperation struct {
	operation     string
	value         *string
	valueTemplate string
	key           string
	errorNotExist bool
	skipNotExist  bool
	tmpl          *template.Template
}

var nullTemplateResult string = "<no value>"

func (o *hstoreOperation) apply(hstore pgtype.Hstore, hstoreVal *hstoreValue, buf *bytes.Buffer) (pgtype.Hstore, error) {
	switch o.operation {
	case hstoreSetOpName:
		if o.tmpl != nil {
			buf.Reset()
			if err := o.tmpl.Execute(buf, hstoreVal); err != nil {
				return nil, fmt.Errorf("error executing template: %w", err)
			}
			newValue := buf.String()
			if newValue == nullTemplateResult {
				hstore[o.key] = nil
			} else {
				hstore[o.key] = &newValue
			}
		} else {
			hstore[o.key] = o.value
		}

	case hstoreDeleteOpName:
		delete(hstore, o.key)

	default:
		return nil, fmt.Errorf("unknown operation %s", o.operation)
	}

	return hstore, nil
}

type hstoreValue struct {
	exists        bool
	value         any
	dynamicValues map[string]any
}

func (hv *hstoreValue) setValue(data pgtype.Hstore, key string) {
	val, found := data[key]
	if !found {
		hv.exists = false
		hv.value = nil
		return
	}
	hv.exists = true

	if val == nil {
		hv.value = nil
	} else {
		hv.value = *val
	}
}

func (hv *hstoreValue) setDynamicValues(values map[string]any) {
	hv.dynamicValues = values
}

func (hv *hstoreValue) GetValue() any {
	if !hv.exists {
		return nil
	}
	return hv.value
}

func (hv *hstoreValue) GetDynamicValue(key string) (any, error) {
	if hv.dynamicValues == nil {
		return nil, errDynamicValuesNil
	}
	dynValue, found := hv.dynamicValues[key]
	if !found {
		return nil, fmt.Errorf("dynamic value '%s' not found", key)
	}
	return dynValue, nil
}
