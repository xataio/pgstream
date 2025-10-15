// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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
	mCustom     string = "custom"
)

var (
	errInvalidMaskingType = errors.New(
		"type must be one of 'custom', 'password', 'name', 'address', 'email', 'mobile', 'tel', 'id', 'credit_card', 'url' or 'default'",
	)
	errMaskUnmaskCannotBeUsedTogether = errors.New("masking: mask and unmask parameters cannot be used together")
	maskingCompatibleTypes            = []SupportedDataType{
		StringDataType,
		ByteArrayDataType,
	}
	maskingParams = []Parameter{
		{
			Name:          "type",
			SupportedType: "string",
			Default:       "default",
			Dynamic:       false,
			Required:      false,
			Values:        []any{"custom", "password", "name", "address", "email", "mobile", "tel", "id", "credit_card", "url", "default"},
		},
		{
			Name:          "mask_begin",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "mask_end",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "unmask_begin",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
		},
		{
			Name:          "unmask_end",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      false,
		},
	}
)

type maskingFunction func(val string) string

// MaskingTransformer is a transformer that masks sensitive data using the Masker library.
type MaskingTransformer struct {
	maskingFunction maskingFunction
}

// NewMaskingTransformer creates a new MaskingTransformer with the given masking function.
func NewMaskingTransformer(params ParameterValues) (*MaskingTransformer, error) {
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
	case mCustom:
		mf, err = getCustomMaskingFn(params)
		if err != nil {
			return nil, err
		}
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

func (t *MaskingTransformer) PostCreate(_ any) error {
	return nil
}

// Transform applies the masking function to the input value and returns the masked value.
func (t *MaskingTransformer) Transform(_ context.Context, value Value) (any, error) {
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
	return maskingCompatibleTypes
}

func (t *MaskingTransformer) Type() TransformerType {
	return Masking
}

func (t *MaskingTransformer) Close() error {
	return nil
}

func MaskingTransformerDefinition() *Definition {
	return &Definition{
		SupportedTypes: maskingCompatibleTypes,
		Parameters:     maskingParams,
	}
}

func getCustomMaskingFn(params ParameterValues) (maskingFunction, error) {
	maskBegin, maskBeginFound, err := FindParameter[string](params, "mask_begin")
	if err != nil {
		return nil, fmt.Errorf("masking: mask_begin must be a string: %w", err)
	}
	maskEnd, maskEndFound, err := FindParameter[string](params, "mask_end")
	if err != nil {
		return nil, fmt.Errorf("masking: mask_end must be a string: %w", err)
	}
	unmaskBegin, unmaskBeginFound, err := FindParameter[string](params, "unmask_begin")
	if err != nil {
		return nil, fmt.Errorf("masking: unmask_begin must be a string: %w", err)
	}
	unmaskEnd, unmaskEndFound, err := FindParameter[string](params, "unmask_end")
	if err != nil {
		return nil, fmt.Errorf("masking: unmask_end must be a string: %w", err)
	}

	if (unmaskBeginFound || unmaskEndFound) && (maskBeginFound || maskEndFound) {
		return nil, errMaskUnmaskCannotBeUsedTogether
	}

	begin, beginAbs := 0, false
	end, endAbs := 100, false
	mask := true

	if unmaskBeginFound {
		mask = false
		begin, beginAbs, err = getMaskingIndex(unmaskBegin)
		if err != nil {
			return nil, fmt.Errorf("masking: unable to read unmask_begin: %w", err)
		}
	}
	if unmaskEndFound {
		mask = false
		end, endAbs, err = getMaskingIndex(unmaskEnd)
		if err != nil {
			return nil, fmt.Errorf("masking: unable to read unmask_end: %w", err)
		}
	}
	if maskBeginFound {
		begin, beginAbs, err = getMaskingIndex(maskBegin)
		if err != nil {
			return nil, fmt.Errorf("masking: unable to read mask_begin: %w", err)
		}
	}
	if maskEndFound {
		end, endAbs, err = getMaskingIndex(maskEnd)
		if err != nil {
			return nil, fmt.Errorf("masking: unable to read mask_end: %w", err)
		}
	}
	return func(v string) string {
		var beginIndex, endIndex int
		length := len(v)

		if beginAbs {
			beginIndex = getIntegerInRange(begin, 0, length)
		} else {
			beginIndex = length * getIntegerInRange(begin, 0, 100) / 100
		}
		if endAbs {
			endIndex = getIntegerInRange(end, 0, length)
		} else {
			endIndex = length * getIntegerInRange(end, 0, 100) / 100
		}

		if beginIndex > endIndex {
			beginIndex, endIndex = endIndex, beginIndex
		}

		if mask {
			return v[:beginIndex] + strings.Repeat("*", endIndex-beginIndex) + v[endIndex:]
		}
		return strings.Repeat("*", beginIndex) + v[beginIndex:endIndex] + strings.Repeat("*", length-endIndex)
	}, nil
}

func getIntegerInRange(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func getMaskingIndex(s string) (int, bool, error) {
	isAbsolute := true
	if strings.HasSuffix(s, "%") {
		isAbsolute = false
		s = strings.TrimSuffix(s, "%")
	}

	index, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false, err
	}

	return int(index), isAbsolute, nil
}
