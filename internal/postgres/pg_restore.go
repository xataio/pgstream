// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"
)

const (
	pgRestoreCmd = "pg_restore"
	psqlCmd      = "psql"
)

type PGRestoreOptions struct {
	// ConnectionString
	ConnectionString string
	// SchemaOnly if true, only schema will be restored (no data)
	SchemaOnly bool
	// Clean all the objects that will be restored
	Clean bool
	// Format (c custom, d directory, t tar, p plain text)
	Format string
	// Options to pass to pg_restore
	Options []string
}

func (opts PGRestoreOptions) toArgs() []string {
	var options []string

	options = append(options, "-d", opts.ConnectionString)

	if opts.SchemaOnly {
		options = append(options, "--schema-only")
	}

	if opts.Clean {
		options = append(options, "--clean")
		options = append(options, "--if-exists")
	}

	options = append(options, opts.Options...)
	return options
}

func (opts PGRestoreOptions) toPSQLArgs() []string {
	return []string{opts.ConnectionString}
}

// Func RunPGRestore runs pg_restore command with the given options and returns
// the result. It the format is plain, it uses `psql` command instead of
// `pg_restore`.
func RunPGRestore(opts PGRestoreOptions, dump []byte) (string, error) {
	var cmd *exec.Cmd
	switch opts.Format {
	case "p":
		cmd = exec.Command(psqlCmd, opts.toPSQLArgs()...) //nolint:gosec
	default:
		cmd = exec.Command(pgRestoreCmd, opts.toArgs()...) //nolint:gosec
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return "", fmt.Errorf("error getting stdin pipe: %w", err)
	}

	go func() {
		defer stdin.Close()
		io.Copy(stdin, bytes.NewReader(dump))
	}()

	// TODO: add streaming support when large data output is required
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("error running pg_restore: %w: (%s)", err, stderr.String())
	}

	return string(out), nil
}
