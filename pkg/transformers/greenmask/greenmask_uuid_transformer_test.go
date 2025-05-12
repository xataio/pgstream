// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func Test_NewUUIDTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  transformers.Parameters
		wantErr error
	}{
		{
			name: "ok - valid random",
			params: transformers.Parameters{
				"generator": random,
			},
			wantErr: nil,
		},
		{
			name: "ok - valid deterministic",
			params: transformers.Parameters{
				"generator": random,
			},
			wantErr: nil,
		},
		{
			name: "error - invalid generator type",
			params: transformers.Parameters{
				"generator": "invalid",
			},
			wantErr: transformers.ErrUnsupportedGenerator,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewUUIDTransformer(tc.params)
			require.ErrorIs(t, err, tc.wantErr)
			if err != nil {
				return
			}
			require.NotNil(t, transformer)
		})
	}
}

func Test_UUIDTransformer_Transform(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   any
		params  transformers.Parameters
		wantErr error
	}{
		{
			name: "ok - string, random",
			params: transformers.Parameters{
				"generator": random,
			},
			input:   "123e4567-e89b-12d3-a456-426655440000",
			wantErr: nil,
		},
		{
			name: "ok - []byte, deterministic",
			params: transformers.Parameters{
				"generator": deterministic,
			},
			input:   []byte("123e4567-e89b-12d3-a456-426655440000"),
			wantErr: nil,
		},
		{
			name: "ok - uuid.UUID, deterministic",
			params: transformers.Parameters{
				"generator": deterministic,
			},
			input:   uuid.MustParse("123e4567-e89b-12d3-a456-426655440000"),
			wantErr: nil,
		},
		{
			name: "ok - can be any string",
			params: transformers.Parameters{
				"generator": deterministic,
			},
			input:   "this is not a uuid",
			wantErr: nil,
		},
		{
			name: "error - invalid input type",
			params: transformers.Parameters{
				"generator": random,
			},
			input:   123,
			wantErr: transformers.ErrUnsupportedValueType,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewUUIDTransformer(tc.params)
			require.NoError(t, err)
			require.NotNil(t, transformer)

			got, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tc.input})
			if !errors.Is(err, tc.wantErr) {
				require.Error(t, err, tc.wantErr.Error())
			}
			if err != nil {
				return
			}
			require.NotNil(t, got)

			// if deterministic, the same input should always produce the same output
			if mustGetGeneratorType(t, tc.params) == deterministic {
				gotAgain, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tc.input})
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}
