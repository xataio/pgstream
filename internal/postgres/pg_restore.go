// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	neturl "net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	pgRestoreCmd          = "pg_restore"
	psqlCmd               = "psql"
	postgres              = "postgres"
	maxStatementLen       = 500
	maxRestoreOutputBytes = 4096
)

var (
	// passwordLiteral matches the quoted secret in a role statement that carries
	// one: CREATE/ALTER ROLE and CREATE/ALTER USER all spell it `PASSWORD '...'`
	passwordLiteral = regexp.MustCompile(`(?i)PASSWORD\s+'(?:[^']|'')*'`)
	// copyRowContext matches the row payload psql appends when a COPY fails
	copyRowContext = regexp.MustCompile(`(?i)(CONTEXT:\s+COPY\s+[^,]+,\s+line\s+\d+):\s+".*"`)
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
	// SessionSettings are name=value settings passed to PostgreSQL through PGOPTIONS.
	SessionSettings []string
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
	return []string{"--echo-errors", opts.ConnectionString}
}

func (opts PGRestoreOptions) toPGOptions(existing string) string {
	options := make([]string, 0, len(opts.SessionSettings)+1)
	if existing != "" {
		options = append(options, existing)
	}
	for _, setting := range opts.SessionSettings {
		options = append(options, "-c "+setting)
	}
	return strings.Join(options, " ")
}

// Func RunPGRestore runs pg_restore command with the given options and returns
// the result.
func RunPGRestore(ctx context.Context, opts PGRestoreOptions, dump []byte) (string, error) {
	var cmd *exec.Cmd
	// if the database is being created, make sure the connection string
	// does not include it so that pg_restore can create it.
	if opts.Create {
		var err error
		opts.ConnectionString, err = RemoveDatabaseFromConnectionString(opts.ConnectionString)
		if err != nil {
			return "", err
		}
	}
	switch opts.Format {
	case "c":
		cmd = exec.CommandContext(ctx, pgRestoreCmd, opts.toArgs()...) //nolint:gosec
	default:
		cmd = exec.CommandContext(ctx, psqlCmd, opts.toPSQLArgs()...) //nolint:gosec
	}
	if len(opts.SessionSettings) > 0 {
		cmd.Env = append(cmd.Environ(), "PGOPTIONS="+opts.toPGOptions(os.Getenv("PGOPTIONS")))
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
	if restoreErr := buildRestoreError(out, err); restoreErr != nil {
		return "", restoreErr
	}

	return string(out), nil
}

func buildRestoreError(out []byte, execErr error) error {
	if execErr == nil && !strings.Contains(string(out), "ERROR") {
		return nil
	}
	if parseErr := parsePgRestoreOutputErrs(out); parseErr != nil {
		return fmt.Errorf("error restoring dump: %w", parseErr)
	}
	if execErr != nil {
		if tail := tailOutput(out, maxRestoreOutputBytes); tail != "" {
			return fmt.Errorf("error restoring dump: %w: output: %s", execErr, tail)
		}
		return fmt.Errorf("error restoring dump: %w: no output captured", execErr)
	}
	return nil
}

// redactSecrets removes credential material from restore output before it is
// carried in an error.
func redactSecrets(s string) string {
	s = passwordLiteral.ReplaceAllString(s, "PASSWORD '[REDACTED]'")
	return copyRowContext.ReplaceAllString(s, "$1: [REDACTED ROW]")
}

// tailOutput returns the last maxBytes of out, trimmed, prefixed with an
// ellipsis when truncated, and with credential material redacted.
func tailOutput(out []byte, maxBytes int) string {
	trimmed := bytes.TrimSpace(out)
	if len(trimmed) == 0 {
		return ""
	}
	redacted := redactSecrets(string(trimmed))
	if len(redacted) <= maxBytes {
		return redacted
	}
	return "..." + redacted[len(redacted)-maxBytes:]
}

// RemoveDatabaseFromConnectionString returns connString with the database
// removed, so a connection made with it lands on the server's default database
// instead of one that may not exist yet. The postgres maintenance database is
// left in place, since it always exists.
//
// Connection strings come in two shapes and both are handled: a URL, whose
// path is emptied, and a libpq key=value DSN, whose dbname keyword is dropped.
// A DSN value quoted around a space (dbname='my db') is not recognised; libpq
// permits it but nothing in pgstream produces one.
func RemoveDatabaseFromConnectionString(connString string) (string, error) {
	dbName, err := extractDatabase(connString)
	if err != nil {
		return "", err
	}
	if dbName == "" || dbName == postgres {
		return connString, nil
	}

	if strings.HasPrefix(connString, "postgres://") || strings.HasPrefix(connString, "postgresql://") {
		return removeDatabaseFromURL(connString)
	}
	return removeDatabaseFromDSN(connString), nil
}

// removeDatabaseFromURL empties the path of a postgres URL. It parses rather
// than substituting the database name textually, because that name also occurs
// elsewhere in a connection string — most commonly as the user, since
// `postgres://app:pw@host/app` is a routine shape — and a textual replacement
// would strip it from there too.
func removeDatabaseFromURL(rawURL string) (string, error) {
	parsed, err := neturl.Parse(rawURL)
	if err != nil {
		// pgx tolerates unescaped characters in the password that net/url
		// rejects; ParseConfig escapes them the same way before parsing.
		escaped, escapeErr := escapeConnectionURL(rawURL)
		if escapeErr != nil {
			return "", fmt.Errorf("removing database from connection string: %w", escapeErr)
		}
		if parsed, err = neturl.Parse(escaped); err != nil {
			return "", fmt.Errorf("removing database from connection string: %w", err)
		}
	}
	parsed.Path = "/"
	return parsed.String(), nil
}

// removeDatabaseFromDSN drops the dbname keyword from a libpq key=value DSN.
func removeDatabaseFromDSN(dsn string) string {
	fields := strings.Fields(dsn)
	kept := make([]string, 0, len(fields))
	for _, field := range fields {
		if strings.HasPrefix(field, "dbname=") {
			continue
		}
		kept = append(kept, field)
	}
	return strings.Join(kept, " ")
}

func parsePgRestoreOutputErrs(out []byte) error {
	if len(out) == 0 {
		return nil
	}

	errs := &PGRestoreErrors{}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	var currentErr error
	inStatement := false
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case inStatement:
			// continuation of a multi-line statement echo: consume until the
			// terminating semicolon so echoed SQL text (which can contain
			// "ERROR" or other keywords) is never parsed as new records
			inStatement = !endsStatement(line)
		case isStatementLine(line):
			if currentErr != nil {
				if isOwnershipError(currentErr) && isCommentStatement(line) {
					currentErr = &ErrCommentOwnership{Details: currentErr.Error()}
				}
				currentErr = fmt.Errorf("%w: %s", currentErr, truncateStatement(redactSecrets(line)))
			}
			inStatement = !endsStatement(line)
		case isErrorLine(line):
			// Save any pending error before processing new one
			if currentErr != nil {
				errs.addError(currentErr)
			}
			currentErr = parseErrorLine(line)
		case isDetailLine(line):
			// Append details to current error
			if currentErr != nil {
				currentErr = fmt.Errorf("%w: %s", currentErr, line)
			}
		}
	}
	if currentErr != nil {
		errs.addError(currentErr)
	}

	if !errs.HasErrors() {
		return nil
	}

	return errs
}

// isDetailLine checks if a line starts a detail record
func isDetailLine(line string) bool {
	return strings.HasPrefix(strings.TrimSpace(line), "DETAIL:")
}

var statementPrefixes = []string{"STATEMENT:", "Command was:"}

func stripStatementPrefix(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	for _, prefix := range statementPrefixes {
		if strings.HasPrefix(trimmed, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(trimmed, prefix)), true
		}
	}
	return "", false
}

func isStatementLine(line string) bool {
	_, ok := stripStatementPrefix(line)
	return ok
}

func isCommentStatement(line string) bool {
	stmt, ok := stripStatementPrefix(line)
	return ok && strings.HasPrefix(stmt, "COMMENT ON ")
}

func endsStatement(line string) bool {
	return strings.HasSuffix(strings.TrimSpace(line), ";")
}

func isOwnershipError(err error) bool {
	return strings.Contains(err.Error(), "must be owner of")
}

func truncateStatement(line string) string {
	if len(line) <= maxStatementLen {
		return line
	}
	truncated := line[:maxStatementLen]
	// don't leave a partial multibyte rune at the cut point
	for len(truncated) > 0 {
		if r, size := utf8.DecodeLastRuneInString(truncated); r != utf8.RuneError || size > 1 {
			break
		}
		truncated = truncated[:len(truncated)-1]
	}
	return truncated + "..."
}

// isErrorLine checks if a line starts an error record. Anchored on line
// prefixes rather than substring matches so that echoed SQL containing
// keywords like "ERROR" is not mistaken for a new error.
func isErrorLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	switch {
	case strings.HasPrefix(trimmed, "ERROR"),
		strings.HasPrefix(trimmed, "pg_restore: error:"),
		strings.HasPrefix(trimmed, "psql: error:"):
		return true
	default:
		return false
	}
}

// parseErrorLine creates an appropriate error type based on the error content
func parseErrorLine(line string) error {
	switch {
	case strings.Contains(line, "already exists"),
		strings.Contains(line, "already a partition"),
		strings.Contains(line, "multiple primary keys for table"):
		return &ErrRelationAlreadyExists{Details: line}
	case strings.Contains(line, "cannot drop schema public because other objects depend on it"):
		return &ErrConstraintViolation{Details: line}
	case strings.Contains(line, `permission denied to grant privileges as role`):
		return &ErrPermissionDenied{Details: line}
	case strings.Contains(line, "does not exist"):
		return &ErrRelationDoesNotExist{Details: line}
	case isTransientErrorLine(line):
		return &ErrTransientFailure{Details: line}
	default:
		return errors.New(line)
	}
}

// transientErrorFragments are lowercased fragments of the messages postgres and
// its client tools emit for failures that are unrelated to the statement being
// restored: a connection that dropped, a server that is not accepting queries
// yet, or a lock that could not be taken. Rerunning the statement is the only
// way to tell whether the condition has cleared.
//
// Fragments that a permanent failure can also produce are deliberately absent.
// A statement timeout, for instance, reads like a transient cancellation but
// recurs on every attempt when the statement is simply too slow, and
// "out of shared memory" recurs until the server is reconfigured.
var transientErrorFragments = []string{
	"broken pipe",
	"canceling statement due to conflict with recovery",
	"canceling statement due to lock timeout",
	"connection reset by peer",
	"connection timed out",
	"connection to server at",
	"connection to server was lost",
	"could not connect to server",
	"could not obtain lock on",
	"could not receive data from server",
	"could not send data to server",
	"could not serialize access",
	"deadlock detected",
	"eof detected",
	"is not yet accepting connections",
	"no connection to the server",
	"remaining connection slots are reserved",
	"server closed the connection unexpectedly",
	"sorry, too many clients already",
	"ssl connection has been closed unexpectedly",
	"ssl syscall error",
	"terminating connection due to administrator command",
	"terminating connection due to unexpected postmaster exit",
	"the database system is in recovery mode",
	"the database system is shutting down",
	"the database system is starting up",
	"timeout expired",
}

// isTransientErrorLine is checked only after the permanent classifications
// above, so that a connection failure reporting a permanent cause — a missing
// database, say — keeps the classification of that cause.
func isTransientErrorLine(line string) bool {
	lowered := strings.ToLower(line)
	for _, fragment := range transientErrorFragments {
		if strings.Contains(lowered, fragment) {
			return true
		}
	}
	return false
}

type PGRestoreErrors struct {
	ignoredErrs   []error
	criticalErrs  []error
	retryableErrs []error
}

func NewPGRestoreErrors(errs ...error) *PGRestoreErrors {
	pgrestoreErrs := &PGRestoreErrors{}
	for _, err := range errs {
		pgrestoreErrs.addError(err)
	}
	return pgrestoreErrs
}

// MergePGRestoreErrors combines the errors of several restores into a single
// PGRestoreErrors that keeps each error in the bucket it was originally
// classified into, so that HasCriticalErrors and IsRetryable describe the set
// as a whole. It returns nil when none of the restores failed.
//
// Concurrent restores need this: classifying their combined failure by
// whichever error happened to arrive first would make the retry decision
// depend on scheduling, and a merged error built with NewPGRestoreErrors would
// demote every nested restore error to critical, since a PGRestoreErrors
// matches none of the classifications addError checks for.
func MergePGRestoreErrors(errs ...error) error {
	merged := &PGRestoreErrors{}
	for _, err := range errs {
		if err == nil {
			continue
		}
		restoreErrs := &PGRestoreErrors{}
		if errors.As(err, &restoreErrs) {
			merged.ignoredErrs = append(merged.ignoredErrs, restoreErrs.ignoredErrs...)
			merged.criticalErrs = append(merged.criticalErrs, restoreErrs.criticalErrs...)
			merged.retryableErrs = append(merged.retryableErrs, restoreErrs.retryableErrs...)
			continue
		}
		merged.addError(err)
	}

	if !merged.HasErrors() {
		return nil
	}
	return merged
}

func (e PGRestoreErrors) Error() string {
	if !e.HasErrors() {
		return ""
	}
	return errors.Join(e.Unwrap()...).Error()
}

func (e PGRestoreErrors) HasErrors() bool {
	return len(e.criticalErrs) > 0 || len(e.retryableErrs) > 0 || len(e.ignoredErrs) > 0
}

// Unwrap exposes the individual restore errors, so that a caller holding the
// collection can still interrogate it with errors.Is and errors.As rather than
// by matching on the joined message.
func (e PGRestoreErrors) Unwrap() []error {
	all := make([]error, 0, len(e.criticalErrs)+len(e.retryableErrs)+len(e.ignoredErrs))
	all = append(all, e.criticalErrs...)
	all = append(all, e.retryableErrs...)
	all = append(all, e.ignoredErrs...)
	return all
}

// HasCriticalErrors reports whether the restore hit an error that must not be
// swallowed. Transient errors count as critical: a caller that does not retry
// has to surface them rather than treat the restore as complete.
func (e *PGRestoreErrors) HasCriticalErrors() bool {
	return len(e.criticalErrs) > 0 || len(e.retryableErrs) > 0
}

// IsRetryable reports whether every error that failed the restore is transient,
// which is the only case where rerunning the same dump can produce a different
// outcome.
func (e *PGRestoreErrors) IsRetryable() bool {
	return len(e.criticalErrs) == 0 && len(e.retryableErrs) > 0
}

func (e *PGRestoreErrors) GetIgnoredErrors() []error {
	return e.ignoredErrs
}

func (e *PGRestoreErrors) GetRetryableErrors() []error {
	return e.retryableErrs
}

func (e *PGRestoreErrors) addError(err error) {
	if err == nil {
		return
	}

	var errAlreadyExists *ErrRelationAlreadyExists
	var errConstraintViolation *ErrConstraintViolation
	var errPermissionDenied *ErrPermissionDenied
	var errDoesNotExist *ErrRelationDoesNotExist
	var errCommentOwnership *ErrCommentOwnership
	var errTransient *ErrTransientFailure
	switch {
	case errors.As(err, &errAlreadyExists),
		errors.As(err, &errConstraintViolation),
		errors.As(err, &errPermissionDenied),
		errors.As(err, &errDoesNotExist),
		errors.As(err, &errCommentOwnership):
		e.ignoredErrs = append(e.ignoredErrs, err)
	case errors.As(err, &errTransient):
		e.retryableErrs = append(e.retryableErrs, err)
	default:
		e.criticalErrs = append(e.criticalErrs, err)
	}
}
