// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
)

// snapshot_tx.go groups the helpers used to read from a shared transaction
// snapshot. A transaction snapshot is exported once per schema and imported by
// every reader transaction so that all workers observe the same stable view of
// the database, which is what allows the ctid based reader to parallelise the
// work.
// https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION

const exportSnapshotQuery = `SELECT pg_export_snapshot()`

func exportSnapshot(ctx context.Context, tx pglib.Tx) (string, error) {
	var snapshotID string
	if err := tx.QueryRow(ctx, []any{&snapshotID}, exportSnapshotQuery); err != nil {
		return "", fmt.Errorf("exporting snapshot: %w", err)
	}
	return snapshotID, nil
}

func setTransactionSnapshot(ctx context.Context, tx pglib.Tx, snapshotID string) error {
	_, err := tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotID))
	if err != nil {
		return fmt.Errorf("setting transaction snapshot: %w", err)
	}
	return nil
}

// execInSnapshotTx runs fn in a read only repeatable read transaction that
// imports the given transaction snapshot, so it observes the same view of the
// database as the transaction that exported it.
func execInSnapshotTx(ctx context.Context, conn pglib.Querier, snapshotID string, fn func(tx pglib.Tx) error) error {
	return conn.ExecInTxWithOptions(ctx, func(tx pglib.Tx) error {
		if err := setTransactionSnapshot(ctx, tx, snapshotID); err != nil {
			return err
		}

		return fn(tx)
	}, snapshotTxOptions())
}

func snapshotTxOptions() pglib.TxOptions {
	return pglib.TxOptions{
		IsolationLevel: pglib.RepeatableRead,
		AccessMode:     pglib.ReadOnly,
	}
}
