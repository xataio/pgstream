// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseByteSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{name: "empty", input: "", want: 0},
		{name: "whitespace only", input: "   ", want: 0},
		{name: "zero", input: "0", want: 0},
		{name: "plain integer", input: "1572864", want: 1572864},
		{name: "plain integer large", input: "104857600", want: 104857600},
		{name: "MiB suffix", input: "64MiB", want: 67108864},
		{name: "GiB suffix", input: "1GiB", want: 1073741824},
		{name: "fractional MiB", input: "1.5MiB", want: 1572864},
		{name: "MiB with space", input: "64 MiB", want: 67108864},
		{name: "lowercase mb treated as binary", input: "64mb", want: 67108864},
		{name: "surrounding whitespace", input: "  256MiB  ", want: 268435456},
		{name: "invalid - non-numeric", input: "abc", wantErr: true},
		{name: "invalid - bad suffix", input: "6yMiB", wantErr: true},
		{name: "invalid - too long suffix", input: "64MiBB", wantErr: true},
		{name: "invalid - negative", input: "-5", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseByteSize(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestByteSize_UnmarshalText(t *testing.T) {
	t.Parallel()

	var b byteSize
	require.NoError(t, b.UnmarshalText([]byte("64MiB")))
	assert.Equal(t, byteSize(67108864), b)

	require.Error(t, b.UnmarshalText([]byte("6yMiB")))
}

func TestGetByteSize_EnvVar(t *testing.T) {
	t.Setenv("PGSTREAM_POSTGRES_WRITER_BATCH_BYTES", "64MiB")
	got, err := getByteSize("PGSTREAM_POSTGRES_WRITER_BATCH_BYTES")
	require.NoError(t, err)
	assert.Equal(t, int64(67108864), got)

	t.Setenv("PGSTREAM_POSTGRES_WRITER_BATCH_BYTES", "6yMiB")
	_, err = getByteSize("PGSTREAM_POSTGRES_WRITER_BATCH_BYTES")
	require.Error(t, err)
}

func TestParsePostgresProcessorConfig_ByteSizeSuffixes(t *testing.T) {
	t.Setenv("PGSTREAM_POSTGRES_WRITER_TARGET_URL", "postgresql://user:password@localhost:5432/mytargetdatabase")
	t.Setenv("PGSTREAM_POSTGRES_WRITER_BATCH_BYTES", "64MiB")
	t.Setenv("PGSTREAM_POSTGRES_WRITER_MAX_QUEUE_BYTES", "256MiB")

	cfg, err := parsePostgresProcessorConfig()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, int64(67108864), cfg.BatchWriter.BatchConfig.MaxBatchBytes)
	assert.Equal(t, int64(268435456), cfg.BatchWriter.BatchConfig.MaxQueueBytes)

	t.Setenv("PGSTREAM_POSTGRES_WRITER_BATCH_BYTES", "not-a-size")
	_, err = parsePostgresProcessorConfig()
	require.Error(t, err)
}

func TestParseStreamConfig_YAMLByteSizeSuffixes(t *testing.T) {
	yamlCfg := `source:
  postgres:
    url: postgresql://user:password@localhost:5432/mydatabase
    mode: replication
target:
  postgres:
    url: postgresql://user:password@localhost:5432/mytargetdatabase
    batch:
      max_bytes: 64MiB
      max_queue_bytes: 256MiB
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlCfg), 0o600))
	require.NoError(t, LoadFile(path))

	streamConfig, err := ParseStreamConfig()
	require.NoError(t, err)
	require.NotNil(t, streamConfig.Processor.Postgres)
	assert.Equal(t, int64(67108864), streamConfig.Processor.Postgres.BatchWriter.BatchConfig.MaxBatchBytes)
	assert.Equal(t, int64(268435456), streamConfig.Processor.Postgres.BatchWriter.BatchConfig.MaxQueueBytes)
}

func TestParseStreamConfig_YAMLInvalidByteSize(t *testing.T) {
	yamlCfg := `source:
  postgres:
    url: postgresql://user:password@localhost:5432/mydatabase
    mode: replication
target:
  postgres:
    url: postgresql://user:password@localhost:5432/mytargetdatabase
    batch:
      max_bytes: 6yMiB
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlCfg), 0o600))
	require.NoError(t, LoadFile(path))

	_, err := ParseStreamConfig()
	require.Error(t, err)
}
