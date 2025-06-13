// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

const (
	pgRestoreCmd = "pg_restore"
	psqlCmd      = "psql"
	postgres     = "postgres"
)

type PGRestoreOptions struct {
	// ConnectionString
	ConnectionString string
	// SchemaOnly if true, only schema will be restored (no data)
	SchemaOnly bool
	// Clean all the objects that will be restored
	Clean bool
	// Create target database
	Create bool
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

	if opts.Create {
		options = append(options, "--create")
	}

	options = append(options, opts.Options...)
	return options
}

func (opts PGRestoreOptions) toPSQLArgs() []string {
	return []string{opts.ConnectionString}
}

// Func RunPGRestore runs pg_restore command with the given options and returns
// the result.
func RunPGRestore(ctx context.Context, opts PGRestoreOptions, dump []byte) (string, error) {
	var cmd *exec.Cmd
	// if the database is being created, make sure the connection string
	// does not include it so that pg_restore can create it.
	if opts.Create {
		var err error
		opts.ConnectionString, err = removeDatabaseFromConnectionString(opts.ConnectionString)
		if err != nil {
			return "", err
		}
	}
	switch opts.Format {
	case "c":
		cmd = exec.Command(pgRestoreCmd, opts.toArgs()...) //nolint:gosec
	default:
		cmd = exec.Command(psqlCmd, opts.toPSQLArgs()...) //nolint:gosec
	}

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
		return "", fmt.Errorf("error restoring dump: %w", parsePgRestoreOutputErrs(out))
	}

	return string(out), nil
}

func removeDatabaseFromConnectionString(url string) (string, error) {
	dbName, err := extractDatabase(url)
	if err != nil {
		return "", err
	}
	if dbName == "" || dbName == postgres {
		return url, nil
	}

	return strings.ReplaceAll(url, "/"+dbName, "/"), nil
}

func parsePgRestoreOutputErrs(out []byte) error {
	errs := &PGRestoreErrors{}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "pg_restore: error:") || strings.Contains(line, "ERROR") {
			switch {
			case strings.Contains(line, "already exists"),
				strings.Contains(line, "multiple primary keys for table"):
				errs.addError(&ErrRelationAlreadyExists{Details: line})
			case strings.Contains(line, "cannot drop schema public because other objects depend on it"):
				errs.addError(&ErrConstraintViolation{Details: line})
			default:
				errs.addError(errors.New(line))
			}
		}
		if strings.Contains(line, "psql: error:") {
			errs.addError(errors.New(line))
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
	var errConstraintViolation *ErrConstraintViolation
	switch {
	case errors.As(err, &errAlreadyExists),
		errors.As(err, &errConstraintViolation):
		e.ignoredErrs = append(e.ignoredErrs, err)
	default:
		e.criticalErrs = append(e.criticalErrs, err)
	}
}
