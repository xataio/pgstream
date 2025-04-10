// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ggwhite/go-masker"
)

const (
	mPassword   string = "password"
	mName       string = "name"
	mAddress    string = "address"
	mEmail      string = "email"
	mMobile     string = "mobile"
	mTelephone  string = "tel"
	mID         string = "id"
	mCreditCard string = "credit_card"
	mURL        string = "url"
	mDefault    string = "default"
)

var errInvalidMaskingType = errors.New("masking_type must be one of 'password', 'name', 'address', 'email', 'mobile', 'tel', 'id', 'credit_card', 'url' or 'default'")

type maskingFunction func(val string) string

// MaskingTransformer is a transformer that masks sensitive data using the Masker library.
type MaskingTransformer struct {
	maskingFunction maskingFunction
}

var MaskingTransformerParams = []string{"type"}

// NewMaskingTransformer creates a new MaskingTransformer with the given masking function.
func NewMaskingTransformer(params Parameters) (*MaskingTransformer, error) {
	var mf maskingFunction
	maskType, found, err := FindParameter[string](params, "type")
	if err != nil {
		return nil, fmt.Errorf("masking: type must be a string: %w", err)
	}
	if !found {
		maskType = mDefault
	}
	m := masker.New()
	switch maskType {
	case mPassword:
		mf = m.Password
	case mName:
		mf = m.Name
	case mAddress:
		mf = m.Address
	case mEmail:
		mf = m.Email
	case mMobile:
		mf = m.Mobile
	case mID:
		mf = m.ID
	case mTelephone:
		mf = m.Telephone
	case mCreditCard:
		mf = m.CreditCard
	case mURL:
		mf = m.URL
	case mDefault:
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
func (t *MaskingTransformer) Transform(value Value) (any, error) {
	switch val := value.TransformValue.(type) {
	case string:
		return t.maskingFunction(val), nil
	case []byte:
		return t.maskingFunction(string(val)), nil
	// TODO: support more types, e.g. uuid.UUID
	default:
		return nil, ErrUnsupportedValueType
	}
}

func (t *MaskingTransformer) CompatibleTypes() []SupportedDataType {
	return []SupportedDataType{
		StringDataType,
		ByteArrayDataType,
	}
}
