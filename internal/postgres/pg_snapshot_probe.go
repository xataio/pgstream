// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

var (
	ErrLoadBalancedSource = errors.New("the source appears to be load-balanced across multiple Postgres instances " +
		"(e.g. an Aurora/RDS reader endpoint, or a pooler spanning instances); parallel snapshotting requires every " +
		"connection to reach the same instance — use a single-instance / writer endpoint")

	ErrSnapshotExportFailed = errors.New("could not export a transaction snapshot on the source")

	// snapshotProbeTxOptions mirrors the data snapshot generator's tx options: an
	// imported snapshot requires REPEATABLE READ (or SERIALIZABLE), and the probe
	// only reads.
	snapshotProbeTxOptions = TxOptions{
		IsolationLevel: RepeatableRead,
		AccessMode:     ReadOnly,
	}

	// snapshotIDPattern matches the shape Postgres' pg_export_snapshot() returns
	// Example: "00000027-000019C2-1"
	snapshotIDPattern = regexp.MustCompile(`^[0-9A-Fa-f]+-[0-9A-Fa-f]+-\d+$`)
)

// IsExportedSnapshotMissing reports whether err (from importing an exported
// snapshot via SET TRANSACTION SNAPSHOT) indicates the snapshot is not visible
// on the instance the connection landed on.
func IsExportedSnapshotMissing(err error) bool {
	if err == nil {
		return false
	}
	var relErr *ErrRelationDoesNotExist
	if errors.As(err, &relErr) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "snapshot") && strings.Contains(msg, "does not exist")
}

// ProbeExportedSnapshotVisibility reproduces the parallel data snapshot's core
// requirement: it exports a transaction snapshot on one connection, holds that
// connection open, and tries to import it (SET TRANSACTION SNAPSHOT) on `probes`
// concurrent connections. An exported snapshot is instance-local, so on a
// load-balanced source some probe connections land on a different instance and
// fail with "snapshot does not exist".
//
// The probe is probabilistic: a run that happens to route every connection to
// the exporting instance reports missing == 0. It is weakest for pure DNS
// round-robin endpoints, where a cached resolver answer can pin every dial in
// the burst to one instance.
func ProbeExportedSnapshotVisibility(ctx context.Context, dial func(context.Context) (Querier, error), probes int) (missing int, err error) {
	if probes < 1 {
		return 0, fmt.Errorf("snapshot visibility probe needs at least one probe connection, got %d", probes)
	}
	snapshotID, release, done, err := exportHeldSnapshot(ctx, dial)
	if err != nil {
		return 0, err
	}
	defer func() {
		close(release)
		<-done
	}()
	return probeSnapshotImport(ctx, dial, snapshotID, probes)
}

func exportHeldSnapshot(ctx context.Context, dial func(context.Context) (Querier, error)) (snapshotID string, release, done chan struct{}, err error) {
	conn, err := dial(ctx)
	if err != nil {
		return "", nil, nil, fmt.Errorf("%w: connecting to source: %w", ErrSnapshotExportFailed, err)
	}

	idCh := make(chan string, 1)
	release = make(chan struct{})
	done = make(chan struct{})
	txErr := make(chan error, 1)

	go func() {
		defer close(done)
		defer conn.Close(context.Background())
		txErr <- conn.ExecInTxWithOptions(ctx, func(tx Tx) error {
			var id string
			if err := tx.QueryRow(ctx, []any{&id}, "SELECT pg_export_snapshot()"); err != nil {
				return err
			}
			idCh <- id
			<-release // hold the snapshot alive while the probes import it
			return nil
		}, snapshotProbeTxOptions)
	}()

	select {
	case id := <-idCh:
		if !snapshotIDPattern.MatchString(id) {
			close(release)
			<-done
			return "", nil, nil, fmt.Errorf("%w: unexpected snapshot id %q from source", ErrSnapshotExportFailed, id)
		}
		return id, release, done, nil
	case err := <-txErr:
		close(release)
		<-done
		if err == nil {
			err = errors.New("exporter transaction returned no snapshot id")
		}
		return "", nil, nil, fmt.Errorf("%w: %w", ErrSnapshotExportFailed, err)
	case <-ctx.Done():
		close(release)
		<-done
		return "", nil, nil, fmt.Errorf("%w: %w", ErrSnapshotExportFailed, ctx.Err())
	}
}

func probeSnapshotImport(ctx context.Context, dial func(context.Context) (Querier, error), snapshotID string, probes int) (missing int, firstErr error) {
	type result struct {
		missing bool
		err     error
	}
	results := make([]result, probes)
	var wg sync.WaitGroup
	for i := 0; i < probes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := dial(ctx)
			if err != nil {
				results[i] = result{err: fmt.Errorf("connecting to source: %w", err)}
				return
			}
			defer conn.Close(context.Background())
			err = conn.ExecInTxWithOptions(ctx, func(tx Tx) error {
				// SET TRANSACTION SNAPSHOT must be the first statement in the tx.
				_, execErr := tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotID))
				return execErr
			}, snapshotProbeTxOptions)
			switch {
			case err == nil:
			case IsExportedSnapshotMissing(err):
				results[i] = result{missing: true}
			default:
				results[i] = result{err: err}
			}
		}(i)
	}
	wg.Wait()

	succeeded := false
	for _, r := range results {
		switch {
		case r.missing:
			missing++
		case r.err != nil:
			if firstErr == nil {
				firstErr = r.err
			}
		default:
			succeeded = true
		}
	}
	// If at least one probe imported the snapshot cleanly and none reported it
	// missing, the source is a single instance.
	if missing == 0 && succeeded {
		firstErr = nil
	}
	return missing, firstErr
}
