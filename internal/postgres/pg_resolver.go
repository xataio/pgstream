// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xataio/pgstream/pkg/backoff"
)

// Resolving the host name is the first step of opening a connection, and it
// fails independently of the database: a resolver that drops a UDP packet, or
// one that is briefly unreachable, fails the connection while the database is
// healthy. Connections are not only opened at start up. A pool opens them on
// demand and replaces them as they reach their maximum lifetime, so a resolver
// blip hours into a run aborts work that was progressing, which is what makes a
// long snapshot the usual victim.
//
// The bounds are deliberately short. The system resolver already retries a lost
// packet on its own, so reaching this retry means several seconds were already
// spent, and a connection that cannot be opened must still fail in a time the
// caller recognises as a failure rather than a hang.
const (
	lookupRetryInitialInterval = 250 * time.Millisecond
	lookupRetryMaxInterval     = 2 * time.Second
	lookupRetryMaxElapsedTime  = 30 * time.Second
	lookupRetryMaxRetries      = 3
)

func lookupRetryConfig() *backoff.ExponentialConfig {
	return &backoff.ExponentialConfig{
		InitialInterval: lookupRetryInitialInterval,
		MaxInterval:     lookupRetryMaxInterval,
		MaxElapsedTime:  lookupRetryMaxElapsedTime,
		MaxRetries:      lookupRetryMaxRetries,
	}
}

// configureLookupRetries makes the connection retry a transient host name
// resolution failure instead of failing on it. It wraps whichever resolver the
// configuration carries, so a caller supplied resolver keeps the same
// treatment.
func configureLookupRetries(cfg *pgx.ConnConfig) {
	cfg.LookupFunc = retryTransientLookupFailures(cfg.LookupFunc, lookupRetryConfig())
}

// retryTransientLookupFailures returns lookup wrapped so that a transient
// resolver failure is retried with exponential backoff. A failure the resolver
// will answer the same way on the next attempt, such as a name that does not
// exist, is returned as it is. A nil lookup is returned unchanged, which leaves
// the driver's own resolver in place.
func retryTransientLookupFailures(lookup pgconn.LookupFunc, cfg *backoff.ExponentialConfig) pgconn.LookupFunc {
	if lookup == nil {
		return nil
	}

	return func(ctx context.Context, host string) ([]string, error) {
		var addrs []string
		var lookupErr error
		// the operation only reports the failures worth another attempt, so
		// the backoff stops on success and on a permanent failure alike, and
		// the answer is read from addrs and lookupErr either way. The failure
		// the caller sees is then always the resolver's own, which is what
		// names the host that could not be resolved.
		attempt := func() error {
			addrs, lookupErr = lookup(ctx, host)
			if lookupErr != nil && isTransientLookupError(lookupErr) {
				return lookupErr
			}
			return nil
		}

		// a cancelled context stops the retries, so the connection does not
		// outlive the run that asked for it.
		if err := backoff.NewExponentialBackoff(ctx, cfg).Retry(attempt); err != nil && lookupErr == nil {
			return nil, err
		}
		return addrs, lookupErr
	}
}

// isTransientLookupError reports whether resolving the same host name again
// could give a different answer.
func isTransientLookupError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	dnsErr := &net.DNSError{}
	if errors.As(err, &dnsErr) {
		// a name that does not exist is a configuration error: asking again
		// returns the same answer.
		if dnsErr.IsNotFound {
			return false
		}
		return dnsErr.IsTimeout || dnsErr.IsTemporary
	}

	// a resolver supplied by the caller reports its own error types, so fall
	// back to the only property that generalises.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}

	return false
}
