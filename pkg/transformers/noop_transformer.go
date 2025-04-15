// SPDX-License-Identifier: Apache-2.0

package transformers

type NoopTransformer struct{}

// NoopTransformer is intended to be used as a placeholder for a transformer that does nothing.
// It is used when a transformer is required for the validation phase for security reasons.
func NewNoopTransformer() (*NoopTransformer, error) {
	return &NoopTransformer{}, nil
}

// Transform method should ideally never be called.
func (nt *NoopTransformer) Transform(value Value) (any, error) {
	return value, nil
}

func (nt *NoopTransformer) CompatibleTypes() []SupportedDataType {
	return []SupportedDataType{AllDataType}
}
