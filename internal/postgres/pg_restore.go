// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"
)

var pgRestoreCmd = "pg_restore"

type PGRestoreOptions struct {
	// ConnectionString
	ConnectionString string
	// SchemaOnly if true, only schema will be restored (no data)
	SchemaOnly bool
	// Clean all the objects that will be restored
	Clean bool
	// Options to pass to pg_restore
	Options []string
}

func (opts PGRestoreOptions) ToArgs() []string {
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

// Func RunPGRestore runs pg_restore command with the given options and returns the result.
func RunPGRestore(opts PGRestoreOptions, dump []byte) (string, error) {
	cmd := exec.Command(pgRestoreCmd, opts.ToArgs()...) //nolint:gosec

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
