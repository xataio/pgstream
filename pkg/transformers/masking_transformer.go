// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ggwhite/go-masker"
)

const (
	MPassword   string = "password"
	MName       string = "name"
	MAddress    string = "address"
	MEmail      string = "email"
	MMobile     string = "mobile"
	MTelephone  string = "tel"
	MID         string = "id"
	MCreditCard string = "credit_card"
	MURL        string = "url"
	MDefault    string = "default"
)

var errInvalidMaskingType = errors.New("masking_type must be one of 'password', 'name', 'address', 'email', 'mobile', 'tel', 'id', 'credit_card', 'url' or 'default'")

type maskingFunction func(val string) string

// MaskingTransformer is a transformer that masks sensitive data using the Masker library.
type MaskingTransformer struct {
	maskingFunction maskingFunction
}

// NewMaskingTransformer creates a new MaskingTransformer with the given masking function.
func NewMaskingTransformer(params Parameters) (*MaskingTransformer, error) {
	var mf maskingFunction
	maskType, found, err := FindParameter[string](params, "type")
	if err != nil {
		return nil, fmt.Errorf("masking: type must be a string: %w", err)
	}
	if !found {
		maskType = MDefault
	}
	m := masker.New()
	switch maskType {
	case MPassword:
		mf = m.Password
	case MName:
		mf = m.Name
	case MAddress:
		mf = m.Address
	case MEmail:
		mf = m.Email
	case MMobile:
		mf = m.Mobile
	case MID:
		mf = m.ID
	case MTelephone:
		mf = m.Telephone
	case MCreditCard:
		mf = m.CreditCard
	case MURL:
		mf = m.URL
	case MDefault:
		mf = func(v string) string {
			return strings.Repeat("*", len(v))
		}
	default:
		return nil, errInvalidMaskingType
	}

	return &MaskingTransformer{
		maskingFunction: mf,
	}, nil
}

// Transform applies the masking function to the input value and returns the masked value.
func (t *MaskingTransformer) Transform(value any) (any, error) {
	switch val := value.(type) {
	case string:
		return t.maskingFunction(val), nil
	case []byte:
		return t.maskingFunction(string(val)), nil
	// TODO: support more types, e.g. uuid.UUID
	default:
		return nil, ErrUnsupportedValueType
	}
}
