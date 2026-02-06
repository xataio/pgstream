// SPDX-License-Identifier: Apache-2.0

package nats

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParser_ToString(t *testing.T) {
	t.Parallel()

	o := &Offset{
		Stream:    "test_stream",
		Consumer:  "test_consumer",
		StreamSeq: 1,
	}

	parser := Parser{}
	str := parser.ToString(o)
	require.Equal(t, "test_stream/test_consumer/1", str)
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
			str:  "test_stream/test_consumer/1",

			wantOffset: &Offset{
				Stream:    "test_stream",
				Consumer:  "test_consumer",
				StreamSeq: 1,
			},
			wantErr: nil,
		},
		{
			name: "ok - empty consumer",
			str:  "test_stream//0",

			wantOffset: &Offset{
				Stream:    "test_stream",
				Consumer:  "",
				StreamSeq: 0,
			},
			wantErr: nil,
		},
		{
			name: "error - invalid format",
			str:  "test_stream_test_consumer_1",

			wantOffset: nil,
			wantErr:    ErrInvalidOffsetFormat,
		},
		{
			name: "error - invalid stream sequence",
			str:  "test_stream/test_consumer/one",

			wantOffset: nil,
			wantErr:    ErrInvalidOffsetFormat,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parser := NewOffsetParser()
			offset, err := parser.FromString(tc.str)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, offset, tc.wantOffset)
		})
	}
}
