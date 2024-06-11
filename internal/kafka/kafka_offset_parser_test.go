// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParser_ToString(t *testing.T) {
	t.Parallel()

	o := &Offset{
		Topic:     "test_topic",
		Partition: 0,
		Offset:    1,
	}

	parser := Parser{}
	str := parser.ToString(o)
	require.Equal(t, "test_topic/0/1", str)
}

func TestParser_FromString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		str  string

		wantOffset *Offset
		wantErr    error
	}{
		{
			name: "ok",
			str:  "test_topic/0/1",

			wantOffset: &Offset{
				Topic:     "test_topic",
				Partition: 0,
				Offset:    1,
			},
			wantErr: nil,
		},
		{
			name: "error - invalid format",
			str:  "test_topic01",

			wantOffset: nil,
			wantErr:    ErrInvalidOffsetFormat,
		},
		{
			name: "error - invalid partition",
			str:  "test_topic/zero/1",

			wantOffset: nil,
			wantErr:    ErrInvalidOffsetFormat,
		},
		{
			name: "error - invalid offset",
			str:  "test_topic/0/one",

			wantOffset: nil,
			wantErr:    ErrInvalidOffsetFormat,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parser := NewOffsetParser()
			offset, err := parser.FromString(tc.str)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, offset, tc.wantOffset)
		})
	}
}
