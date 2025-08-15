// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
)

var pgDumpAllCmd = "pg_dumpall"

type PGDumpAllOptions struct {
	// ConnectionString
	ConnectionString string
	// RolesOnly if true, only roles will be exported (no schema)
	RolesOnly bool
	// Options to pass to pg_dumpall
	Options []string
}

func (opts *PGDumpAllOptions) ToArgs() []string {
	options := []string{"-d", opts.ConnectionString}
	if opts.RolesOnly {
		options = append(options, "--roles-only")
	}

	options = append(options, opts.Options...)
	return options
}

// Func RunPGDumpAll runs pg_dumpall command with the given options and returns the result.
func RunPGDumpAll(_ context.Context, opts PGDumpAllOptions) ([]byte, error) {
	cmd := exec.Command(pgDumpAllCmd, opts.ToArgs()...) //nolint:gosec

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("error running pg_dumpall: %w: %s", err, stderr.String())
	}

	return out, nil
}
