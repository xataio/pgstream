// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceSnapshotInstanceCheck_Run(t *testing.T) {
	t.Parallel()

	probeErr := errors.New("boom")

	tests := []struct {
		name         string
		probe        func(ctx context.Context, probes int) (int, error)
		wantFindings int
		wantErr      bool
	}{
		{
			name:         "single instance - no finding",
			probe:        func(context.Context, int) (int, error) { return 0, nil },
			wantFindings: 0,
		},
		{
			name:         "load balanced - one finding",
			probe:        func(context.Context, int) (int, error) { return 3, nil },
			wantFindings: 1,
		},
		{
			name:    "probe could not run - check error",
			probe:   func(context.Context, int) (int, error) { return 0, probeErr },
			wantErr: true,
		},
		{
			name:         "detection wins over a probe error",
			probe:        func(context.Context, int) (int, error) { return 2, probeErr },
			wantFindings: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			check := &SourceSnapshotInstanceCheck{Probe: tt.probe, Probes: 8}
			findings, err := check.Run(context.Background())
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, findings, tt.wantFindings)
		})
	}
}
