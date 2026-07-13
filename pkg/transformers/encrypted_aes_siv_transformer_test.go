// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tink-crypto/tink-go/v2/daead/subtle"
)

// bytes 0x00..0x3f — a fixed, obviously-test 64-byte key
const testEncryptedAESSIVKeyHex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" +
	"202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"

func TestEncryptedAESSIVTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  ParameterValues
		wantErr error
	}{
		{
			name: "ok - valid key",
			params: ParameterValues{
				"key_hex": testEncryptedAESSIVKeyHex,
			},
			wantErr: nil,
		},
		{
			name: "ok - valid key with associated data",
			params: ParameterValues{
				"key_hex":         testEncryptedAESSIVKeyHex,
				"associated_data": "public.orders.file_path",
			},
			wantErr: nil,
		},
		{
			name:    "error - key_hex missing",
			params:  ParameterValues{},
			wantErr: errEncryptedAESSIVKeyNotFound,
		},
		{
			name: "error - key_hex not a string",
			params: ParameterValues{
				"key_hex": 123,
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "error - key_hex not valid hex",
			params: ParameterValues{
				"key_hex": strings.Repeat("zx", subtle.AESSIVKeySize),
			},
			wantErr: errEncryptedAESSIVKeyInvalid,
		},
		{
			name: "error - key_hex wrong length",
			params: ParameterValues{
				"key_hex": strings.Repeat("ab", 32),
			},
			wantErr: errEncryptedAESSIVKeyInvalid,
		},
		{
			name: "error - associated_data not a string",
			params: ParameterValues{
				"key_hex":         testEncryptedAESSIVKeyHex,
				"associated_data": 1,
			},
			wantErr: ErrInvalidParameters,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			est, err := NewEncryptedAESSIVTransformer(tc.params)
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr != nil {
				return
			}
			require.NotNil(t, est)
		})
	}
}

func TestEncryptedAESSIVTransformer_Transform(t *testing.T) {
	t.Parallel()

	newTransformer := func(t *testing.T, params ParameterValues) *EncryptedAESSIVTransformer {
		t.Helper()
		est, err := NewEncryptedAESSIVTransformer(params)
		require.NoError(t, err)
		return est
	}
	transform := func(t *testing.T, est *EncryptedAESSIVTransformer, input any) string {
		t.Helper()
		got, err := est.Transform(context.Background(), Value{TransformValue: input})
		require.NoError(t, err)
		token, ok := got.(string)
		require.True(t, ok, "expected string token, got %T", got)
		return token
	}
	// bytea columns return the token as []byte so pgx can binary-encode it
	transformBytea := func(t *testing.T, est *EncryptedAESSIVTransformer, input any) string {
		t.Helper()
		got, err := est.Transform(context.Background(), Value{TransformValue: input, TransformType: "bytea"})
		require.NoError(t, err)
		token, ok := got.([]byte)
		require.True(t, ok, "expected []byte token, got %T", got)
		return string(token)
	}
	newSIV := func(t *testing.T) *subtle.AESSIV {
		t.Helper()
		key, err := hex.DecodeString(testEncryptedAESSIVKeyHex)
		require.NoError(t, err)
		siv, err := subtle.NewAESSIV(key)
		require.NoError(t, err)
		return siv
	}

	est := newTransformer(t, ParameterValues{"key_hex": testEncryptedAESSIVKeyHex})

	t.Run("ok - golden token", func(t *testing.T) {
		t.Parallel()
		// pins the token format (AES-SIV ciphertext, unpadded base64url) so
		// accidental format changes break loudly: consumers decrypt tokens
		require.Equal(t, "Hc5d96xIxu2ute1RbFuenEftGxw-P__m1Vv_", transform(t, est, "hello world"))
	})

	t.Run("ok - deterministic and []byte parity", func(t *testing.T) {
		t.Parallel()
		fromString := transform(t, est, "orders/1234/scan_8842.stl")
		fromBytes := transformBytea(t, est, []byte("orders/1234/scan_8842.stl"))
		require.Equal(t, fromString, fromBytes)
		require.Equal(t, fromString, transform(t, est, "orders/1234/scan_8842.stl"))
	})

	t.Run("ok - bytea snapshot and replication parity", func(t *testing.T) {
		t.Parallel()
		// snapshots deliver bytea as raw []byte (pgx), replication as the
		// hex-text form (wal2json); both must produce the same token
		raw := []byte{0x00, 0x01, 0xfe, 0xff}
		fromSnapshot := transformBytea(t, est, raw)
		fromReplication := transformBytea(t, est, `\x0001feff`)
		require.Equal(t, fromSnapshot, fromReplication)

		ciphertext, err := base64.RawURLEncoding.DecodeString(fromSnapshot)
		require.NoError(t, err)
		plaintext, err := newSIV(t).DecryptDeterministically(ciphertext, nil)
		require.NoError(t, err)
		require.Equal(t, raw, plaintext)
	})

	t.Run("error - bytea string value not in hex format", func(t *testing.T) {
		t.Parallel()
		_, err := est.Transform(context.Background(), Value{TransformValue: "no hex prefix", TransformType: "bytea"})
		require.ErrorIs(t, err, errEncryptedAESSIVByteaNotHex)
		_, err = est.Transform(context.Background(), Value{TransformValue: `\xnothex`, TransformType: "bytea"})
		require.ErrorIs(t, err, errEncryptedAESSIVByteaNotHex)
	})

	t.Run("ok - round trip", func(t *testing.T) {
		t.Parallel()
		token := transform(t, est, "orders/1234/scan_8842.stl")
		ciphertext, err := base64.RawURLEncoding.DecodeString(token)
		require.NoError(t, err)
		plaintext, err := newSIV(t).DecryptDeterministically(ciphertext, nil)
		require.NoError(t, err)
		require.Equal(t, "orders/1234/scan_8842.stl", string(plaintext))
	})

	t.Run("ok - empty string is encrypted too", func(t *testing.T) {
		t.Parallel()
		token := transform(t, est, "")
		require.NotEmpty(t, token)
		ciphertext, err := base64.RawURLEncoding.DecodeString(token)
		require.NoError(t, err)
		plaintext, err := newSIV(t).DecryptDeterministically(ciphertext, nil)
		require.NoError(t, err)
		require.Empty(t, plaintext)
	})

	t.Run("ok - associated_data binds the token", func(t *testing.T) {
		t.Parallel()
		estAAD := newTransformer(t, ParameterValues{
			"key_hex":         testEncryptedAESSIVKeyHex,
			"associated_data": "public.orders.file_path",
		})
		token := transform(t, estAAD, "hello world")
		require.NotEqual(t, transform(t, est, "hello world"), token)
		// golden token: pinned so the docs example stays accurate
		require.Equal(t, "AsC2hoq-Y9V0iqK6JmNIxq0Fsr3SFPo27QNq", token)

		ciphertext, err := base64.RawURLEncoding.DecodeString(token)
		require.NoError(t, err)
		siv := newSIV(t)
		// decryption without the matching associated data must fail...
		_, err = siv.DecryptDeterministically(ciphertext, nil)
		require.Error(t, err)
		// ...and succeed with it
		plaintext, err := siv.DecryptDeterministically(ciphertext, []byte("public.orders.file_path"))
		require.NoError(t, err)
		require.Equal(t, "hello world", string(plaintext))
	})

	t.Run("error - tampered token fails decryption", func(t *testing.T) {
		t.Parallel()
		token := transform(t, est, "hello world")
		ciphertext, err := base64.RawURLEncoding.DecodeString(token)
		require.NoError(t, err)
		ciphertext[0] ^= 0x01
		_, err = newSIV(t).DecryptDeterministically(ciphertext, nil)
		require.Error(t, err)
	})

	t.Run("error - unsupported type", func(t *testing.T) {
		t.Parallel()
		_, err := est.Transform(context.Background(), Value{TransformValue: 1})
		require.ErrorIs(t, err, ErrUnsupportedValueType)
	})
}
