// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/daead/subtle"
)

// EncryptedAESSIVTransformer encrypts values with AES-SIV (RFC 5297), a
// deterministic authenticated encryption scheme: the same input, key and
// associated data always produce the same token, so equality relationships
// between column values are preserved across rows, tables and runs. Unlike
// hashing, the transformation is reversible by holders of the key, and
// tampered or forged tokens fail authenticated decryption. Tokens are
// base64url-encoded (no padding), safe for URLs and file names.
type EncryptedAESSIVTransformer struct {
	siv *subtle.AESSIV
	aad []byte
}

var (
	errEncryptedAESSIVKeyNotFound = errors.New("encrypted_aes_siv_transformer: key_hex parameter not found")
	errEncryptedAESSIVKeyInvalid  = fmt.Errorf("encrypted_aes_siv_transformer: key_hex must be %d hex-encoded bytes", subtle.AESSIVKeySize)

	encryptedAESSIVCompatibleTypes = []SupportedDataType{
		StringDataType,
		ByteArrayDataType,
	}
	encryptedAESSIVParams = []Parameter{
		{
			Name:          "key_hex",
			SupportedType: "string",
			Default:       nil,
			Dynamic:       false,
			Required:      true,
		},
		{
			Name:          "associated_data",
			SupportedType: "string",
			Default:       "",
			Dynamic:       false,
			Required:      false,
		},
	}
)

func NewEncryptedAESSIVTransformer(params ParameterValues) (*EncryptedAESSIVTransformer, error) {
	keyHex, found, err := FindParameter[string](params, "key_hex")
	if err != nil {
		return nil, fmt.Errorf("encrypted_aes_siv_transformer: key_hex must be a string: %w", err)
	}
	if !found {
		return nil, errEncryptedAESSIVKeyNotFound
	}
	key, err := hex.DecodeString(keyHex)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errEncryptedAESSIVKeyInvalid, err)
	}
	if len(key) != subtle.AESSIVKeySize {
		return nil, fmt.Errorf("%w, got %d", errEncryptedAESSIVKeyInvalid, len(key))
	}
	siv, err := subtle.NewAESSIV(key)
	if err != nil {
		return nil, fmt.Errorf("encrypted_aes_siv_transformer: %w", err)
	}

	aad, err := FindParameterWithDefault(params, "associated_data", "")
	if err != nil {
		return nil, fmt.Errorf("encrypted_aes_siv_transformer: associated_data must be a string: %w", err)
	}

	return &EncryptedAESSIVTransformer{
		siv: siv,
		aad: []byte(aad),
	}, nil
}

func (est *EncryptedAESSIVTransformer) Transform(_ context.Context, v Value) (any, error) {
	switch val := v.TransformValue.(type) {
	case string:
		return est.transform(val)
	case []byte:
		return est.transform(string(val))
	default:
		return nil, fmt.Errorf("expected string, got %T: %w", v.TransformValue, ErrUnsupportedValueType)
	}
}

func (est *EncryptedAESSIVTransformer) transform(str string) (string, error) {
	ciphertext, err := est.siv.EncryptDeterministically([]byte(str), est.aad)
	if err != nil {
		return "", fmt.Errorf("encrypted_aes_siv_transformer: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(ciphertext), nil
}

func (est *EncryptedAESSIVTransformer) CompatibleTypes() []SupportedDataType {
	return encryptedAESSIVCompatibleTypes
}

func (est *EncryptedAESSIVTransformer) Type() TransformerType {
	return EncryptedAESSIV
}

func (est *EncryptedAESSIVTransformer) IsDynamic() bool {
	return false
}

func (est *EncryptedAESSIVTransformer) Close() error {
	return nil
}

func EncryptedAESSIVTransformerDefinition() *Definition {
	return &Definition{
		SupportedTypes: encryptedAESSIVCompatibleTypes,
		Parameters:     encryptedAESSIVParams,
	}
}
