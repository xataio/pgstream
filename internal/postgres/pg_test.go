// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_DefaultReplicationSlotName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantSlotName string
	}{
		{
			name:         "example",
			wantSlotName: "pgstream_example_slot",
		},
		{
			name:         "example.com",
			wantSlotName: "pgstream_example_com_slot",
		},
		{
			name:         "example.test.com",
			wantSlotName: "pgstream_example_test_com_slot",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			slotName := DefaultReplicationSlotName(tc.name)
			require.Equal(t, tc.wantSlotName, slotName)
		})
	}
}
