// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

func TestApplyPostgresBulkBatchDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  batch.Config

		wantMaxBatchSize    int64
		wantMaxBatchBytes   int64
		wantSendConcurrency int
	}{
		{
			name: "all defaults - batch bytes split across default copy workers",
			cfg:  batch.Config{},

			wantMaxBatchSize:    defaultPostgresBulkBatchSize,
			wantMaxBatchBytes:   defaultPostgresBulkBatchBytes / defaultPostgresBulkCopyWorkers,
			wantSendConcurrency: defaultPostgresBulkCopyWorkers,
		},
		{
			name: "single copy worker - full batch bytes",
			cfg:  batch.Config{SendConcurrency: 1},

			wantMaxBatchSize:    defaultPostgresBulkBatchSize,
			wantMaxBatchBytes:   defaultPostgresBulkBatchBytes,
			wantSendConcurrency: 1,
		},
		{
			name: "explicit batch bytes - not split",
			cfg:  batch.Config{MaxBatchBytes: 1024, SendConcurrency: 4},

			wantMaxBatchSize:    defaultPostgresBulkBatchSize,
			wantMaxBatchBytes:   1024,
			wantSendConcurrency: 4,
		},
		{
			name: "explicit copy workers - batch bytes split accordingly",
			cfg:  batch.Config{SendConcurrency: 16},

			wantMaxBatchSize:    defaultPostgresBulkBatchSize,
			wantMaxBatchBytes:   defaultPostgresBulkBatchBytes / 16,
			wantSendConcurrency: 16,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := tc.cfg
			applyPostgresBulkBatchDefaults(&cfg)

			require.Equal(t, tc.wantMaxBatchSize, cfg.MaxBatchSize)
			require.Equal(t, tc.wantMaxBatchBytes, cfg.MaxBatchBytes)
			require.Equal(t, tc.wantSendConcurrency, cfg.SendConcurrency)
			require.Equal(t, defaultPostgresBulkBatchTimeout, cfg.BatchTimeout)
		})
	}
}
