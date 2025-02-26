// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func Test_NewUUIDTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		generator transformers.GeneratorType
		wantErr   error
	}{
		{
			name:      "ok - valid random",
			generator: transformers.Random,
			wantErr:   nil,
		},
		{
			name:      "ok - valid deterministic",
			generator: transformers.Deterministic,
			wantErr:   nil,
		},
		{
			name:      "error - invalid generator type",
			generator: "invalid",
			wantErr:   transformers.ErrUnsupportedGenerator,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewUUIDTransformer(tc.generator)
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
		name          string
		generatorType transformers.GeneratorType
		input         any
		wantErr       error
	}{
		{
			name:          "ok - string, random",
			generatorType: transformers.Random,
			input:         "123e4567-e89b-12d3-a456-426655440000",
			wantErr:       nil,
		},
		{
			name:          "ok - []byte, deterministic",
			generatorType: transformers.Deterministic,
			input:         []byte("123e4567-e89b-12d3-a456-426655440000"),
			wantErr:       nil,
		},
		{
			name:          "ok - uuid.UUID, deterministic",
			generatorType: transformers.Deterministic,
			input:         uuid.MustParse("123e4567-e89b-12d3-a456-426655440000"),
			wantErr:       nil,
		},
		{
			name:          "error - invalid input type",
			generatorType: transformers.Random,
			input:         123,
			wantErr:       transformers.ErrUnsupportedValueType,
		},
		{
			name:          "error - cannot parse string",
			generatorType: transformers.Random,
			input:         "123e45671e89b112d31a4561426655440000",
			wantErr:       errors.New("invalid UUID format"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewUUIDTransformer(tc.generatorType)
			require.NoError(t, err)
			require.NotNil(t, transformer)

			got, err := transformer.Transform(tc.input)
			if !errors.Is(err, tc.wantErr) {
				require.Error(t, err, tc.wantErr.Error())
			}
			if err != nil {
				return
			}
			require.NotNil(t, got)

			// if deterministic, the same input should always produce the same output
			if tc.generatorType == transformers.Deterministic {
				gotAgain, err := transformer.Transform(tc.input)
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}
