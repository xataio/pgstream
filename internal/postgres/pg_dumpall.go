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
	// Clean if true, the target database will be cleaned before the dump is restored
	Clean bool
	// NoPasswords if true, no role passwords will be included in the dump
	NoPasswords bool
	// Role specifies the role to be used for the dump
	Role string
	// Options to pass to pg_dumpall
	Options []string
}

func (opts *PGDumpAllOptions) ToArgs() []string {
	options := []string{"-d", opts.ConnectionString}
	if opts.RolesOnly {
		options = append(options, "--roles-only")
	}

	if opts.Clean {
		options = append(options, "--clean", "--if-exists")
	}

	if opts.NoPasswords {
		options = append(options, "--no-role-passwords")
	}

	if opts.Role != "" {
		options = append(options, fmt.Sprintf("--role=%v", opts.Role))
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
