// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/tink-crypto/tink-go/v2/daead/subtle"
)

// EncryptedAESSIVTransformer encrypts values with AES-SIV (RFC 5297), a
// deterministic authenticated encryption scheme: the same input, key and
// associated data always produce the same token, so equality relationships
// between column values are preserved across rows, tables and runs. Unlike
// hashing, the transformation is reversible by holders of the key, and
// tampered or forged tokens fail authenticated decryption. Tokens are
// base64url-encoded (no padding), safe for URLs and file names. For bytea
// columns the raw bytes are encrypted regardless of how the value arrives
// (raw during snapshots, hex-text during replication) and the token is
// returned as bytes.
type EncryptedAESSIVTransformer struct {
	siv *subtle.AESSIV
	aad []byte
}

var (
	errEncryptedAESSIVKeyNotFound = errors.New("encrypted_aes_siv_transformer: key_hex parameter not found")
	errEncryptedAESSIVKeyInvalid  = fmt.Errorf("encrypted_aes_siv_transformer: key_hex must be %d hex-encoded bytes", subtle.AESSIVKeySize)
	errEncryptedAESSIVByteaNotHex = errors.New(`encrypted_aes_siv_transformer: bytea value is not in postgres hex format ("\x..."); the source must use bytea_output=hex`)

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
	siv, err := subtle.NewAESSIV(key)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errEncryptedAESSIVKeyInvalid, err)
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

const byteaType = "bytea"

func (est *EncryptedAESSIVTransformer) Transform(_ context.Context, v Value) (any, error) {
	switch val := v.TransformValue.(type) {
	case string:
		// bytea values arrive as raw bytes during snapshots (pgx) but as their
		// hex-text form ("\x...") during replication (wal2json); decode so both
		// paths encrypt the same plaintext and produce the same token
		if v.TransformType == byteaType {
			plaintext, err := decodeByteaHex(val)
			if err != nil {
				return nil, err
			}
			return est.transformToBytes(plaintext)
		}
		return est.transform([]byte(val))
	case []byte:
		return est.transformToBytes(val)
	default:
		return nil, fmt.Errorf("expected string or []byte, got %T: %w", v.TransformValue, ErrUnsupportedValueType)
	}
}

func (est *EncryptedAESSIVTransformer) transform(plaintext []byte) (string, error) {
	ciphertext, err := est.siv.EncryptDeterministically(plaintext, est.aad)
	if err != nil {
		return "", fmt.Errorf("encrypted_aes_siv_transformer: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(ciphertext), nil
}

// transformToBytes returns the token as bytes for bytea columns: pgx can
// binary-encode []byte into bytea (required by COPY during bulk-ingest
// snapshots) but not a plain string.
func (est *EncryptedAESSIVTransformer) transformToBytes(plaintext []byte) ([]byte, error) {
	token, err := est.transform(plaintext)
	if err != nil {
		return nil, err
	}
	return []byte(token), nil
}

func decodeByteaHex(val string) ([]byte, error) {
	hexStr, found := strings.CutPrefix(val, `\x`)
	if !found {
		return nil, errEncryptedAESSIVByteaNotHex
	}
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errEncryptedAESSIVByteaNotHex, err)
	}
	return decoded, nil
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
