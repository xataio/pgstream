// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

const (
	pgRestoreCmd = "pg_restore"
)

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

// Func RunPGRestore runs pg_restore command with the given options and returns
// the result.
func RunPGRestore(opts PGRestoreOptions, dump []byte) (string, error) {
	cmd := exec.Command(pgRestoreCmd, opts.toArgs()...) //nolint:gosec

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return "", fmt.Errorf("error getting stdin pipe: %w", err)
	}

	go func() {
		defer stdin.Close()
		io.Copy(stdin, bytes.NewReader(dump))
	}()

	// TODO: add streaming support when large data output is required
	out, err := cmd.CombinedOutput()
	if err != nil || strings.Contains(string(out), "ERROR") {
		return "", fmt.Errorf("error running pg_restore: %w", parsePgRestoreOutputErrs(out))
	}

	return string(out), nil
}

func parsePgRestoreOutputErrs(out []byte) error {
	errs := &PGRestoreErrors{}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "pg_restore: error:") {
			switch {
			case strings.Contains(line, "already exists"),
				strings.Contains(line, "multiple primary keys for table"):
				errs.addError(&ErrRelationAlreadyExists{Details: line})
			default:
				errs.addError(errors.New(line))
			}
		}
	}
	return errs
}

type PGRestoreErrors struct {
	ignoredErrs  []error
	criticalErrs []error
}

func NewPGRestoreErrors(errs ...error) *PGRestoreErrors {
	pgrestoreErrs := &PGRestoreErrors{}
	for _, err := range errs {
		pgrestoreErrs.addError(err)
	}
	return pgrestoreErrs
}

func (e PGRestoreErrors) Error() string {
	if len(e.criticalErrs) == 0 && len(e.ignoredErrs) == 0 {
		return ""
	}

	return errors.Join(append(e.criticalErrs, e.ignoredErrs...)...).Error()
}

func (e *PGRestoreErrors) HasCriticalErrors() bool {
	return len(e.criticalErrs) > 0
}

func (e *PGRestoreErrors) GetIgnoredErrors() []error {
	return e.ignoredErrs
}

func (e *PGRestoreErrors) addError(err error) {
	if err == nil {
		return
	}

	var errAlreadyExists *ErrRelationAlreadyExists
	switch {
	case errors.As(err, &errAlreadyExists):
		e.ignoredErrs = append(e.ignoredErrs, err)
	default:
		e.criticalErrs = append(e.criticalErrs, err)
	}
}
