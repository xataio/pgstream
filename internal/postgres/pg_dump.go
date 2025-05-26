// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
)

var pgDumpCmd = "pg_dump"

type PGDumpOptions struct {
	// ConnectionString
	ConnectionString string
	// Format (c custom, d directory, t tar, p plain text)
	Format string
	// Schemas to export
	Schemas []string
	// Schemas to be excluded from the dump, regex patterns supported
	ExcludeSchemas []string
	// Tables to export
	Tables []string
	// Tables to be excluded from the dump, regex patterns supported
	ExcludeTables []string
	// SchemaOnly if true, only schema will be exported (no data)
	SchemaOnly bool
	// do not dump privileges (grant/revoke)
	NoPrivileges bool
	// Clean all the objects that will be dumped
	Clean bool
	// Options to pass to pg_dump
	Options []string
}

func (opts *PGDumpOptions) ToArgs() []string {
	options := make([]string, 0, len(opts.Schemas)+len(opts.Tables)+len(opts.ExcludeTables)+len(opts.Options))

	options = append(options, opts.ConnectionString)

	if opts.Format != "" {
		options = append(options, fmt.Sprintf(`-F%v`, opts.Format))
	}

	if opts.SchemaOnly {
		options = append(options, "--schema-only")
	}

	if opts.NoPrivileges {
		options = append(options, "--no-privileges")
	}

	for _, schema := range opts.Schemas {
		options = append(options, fmt.Sprintf("--schema=%v", schema))
	}

	for _, schema := range opts.ExcludeSchemas {
		options = append(options, fmt.Sprintf("--exclude-schema=%v", schema))
	}

	for _, table := range opts.Tables {
		options = append(options, fmt.Sprintf("--table=%v", table))
	}

	for _, table := range opts.ExcludeTables {
		options = append(options, fmt.Sprintf("--exclude-table=%v", table))
	}

	if opts.Clean {
		options = append(options, "--clean")
		options = append(options, "--if-exists")
	}

	options = append(options, opts.Options...)
	return options
}

// Func RunPGDump runs pg_dump command with the given options and returns the result.
func RunPGDump(_ context.Context, opts PGDumpOptions) ([]byte, error) {
	cmd := exec.Command(pgDumpCmd, opts.ToArgs()...) //nolint:gosec

	// TODO: add streaming support when it needs to be used for large data dumps
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("error running pg_dump: %w: %s", err, stderr.String())
	}

	return out, nil
}
