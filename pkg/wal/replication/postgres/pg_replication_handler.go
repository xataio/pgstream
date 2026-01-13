// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal/replication"
)

// Handler handles the postgres replication slot operations
type Handler struct {
	logger    loglib.Logger
	logFields loglib.Fields

	pgReplicationConn        pglib.ReplicationQuerier
	pgReplicationSlotName    string
	pgReplicationConnBuilder func() (pglib.ReplicationQuerier, error)
	pgConnBuilder            func() (pglib.Querier, error)

	excludedTables pglib.SchemaTableMap
	includedTables pglib.SchemaTableMap

	lsnParser replication.LSNParser
}

type Config struct {
	PostgresURL string
	// Name of the replication slot to listen on. If not provided, it defaults
	// to "pgstream_<dbname>_slot".
	ReplicationSlotName string
	// List of qualified tables excluded from the replication for which errors
	// should be ignored.
	ExcludeTables []string
	// List of qualified tables included for replication.
	IncludeTables []string
}

type Option func(h *Handler)

const (
	logLSNPosition = "position"
	logSlotName    = "slot_name"
	logDBName      = "db_name"
	logSystemID    = "system_id"
)

var pluginArguments = []string{
	`"include-timestamp" '1'`,
	`"format-version" '2'`,
	`"write-in-chunks" '1'`,
	`"include-lsn" '1'`,
	`"include-transaction" '0'`,
}

// NewHandler returns a new postgres replication handler for the database on input.
func NewHandler(ctx context.Context, cfg Config, opts ...Option) (*Handler, error) {
	pgReplicationConn, err := pglib.NewReplicationConn(ctx, cfg.PostgresURL)
	if err != nil {
		return nil, err
	}

	sysID, err := pgReplicationConn.IdentifySystem(ctx)
	if err != nil {
		return nil, fmt.Errorf("identifySystem failed: %w", err)
	}

	replicationSlotName := cfg.ReplicationSlotName
	if replicationSlotName == "" {
		replicationSlotName = pglib.DefaultReplicationSlotName(sysID.DBName)
	}

	connBuilder := func() (pglib.Querier, error) {
		return pglib.NewConn(ctx, cfg.PostgresURL)
	}
	replicationConnBuilder := func() (pglib.ReplicationQuerier, error) {
		return pglib.NewReplicationConn(ctx, cfg.PostgresURL)
	}

	h := &Handler{
		logger:                   loglib.NewNoopLogger(),
		pgReplicationConn:        pgReplicationConn,
		pgReplicationSlotName:    replicationSlotName,
		pgConnBuilder:            connBuilder,
		pgReplicationConnBuilder: replicationConnBuilder,
		lsnParser:                &LSNParser{},
		logFields: loglib.Fields{
			logSystemID:    sysID.SystemID,
			logDBName:      sysID.DBName,
			logSlotName:    replicationSlotName,
			logLSNPosition: sysID.XLogPos,
		},
	}

	if len(cfg.IncludeTables) > 0 {
		h.includedTables, err = pglib.NewSchemaTableMap(cfg.IncludeTables)
		if err != nil {
			return nil, err
		}
		// make sure we never ignore the pgstream schema_log table
		if err := h.includedTables.Add(pglib.QuoteQualifiedIdentifier(schemalog.SchemaName, schemalog.TableName)); err != nil {
			return nil, err
		}
	}
	if len(cfg.ExcludeTables) > 0 {
		h.excludedTables, err = pglib.NewSchemaTableMap(cfg.ExcludeTables)
		if err != nil {
			return nil, err
		}
	}

	for _, opt := range opts {
		opt(h)
	}

	// make sure the replication slot we are going to use exists
	if err := h.verifyReplicationSlotExists(ctx); err != nil {
		h.logger.Error(err, "verifying replication slot")
		return nil, err
	}

	return h, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(h *Handler) {
		h.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "postgres_replication_handler",
		})
	}
}

// StartReplication will start the replication process on the configured
// replication slot. It will check for the last synced LSN
// (confirmed_flush_lsn), and if there isn't one, it will start replication from
// the restart_lsn position.
func (h *Handler) StartReplication(ctx context.Context) error {
	conn, err := h.pgConnBuilder()
	if err != nil {
		return fmt.Errorf("creating pg connection: %w", err)
	}
	defer conn.Close(ctx)

	startPos, err := h.getLastSyncedLSN(ctx, conn)
	if err != nil {
		return fmt.Errorf("read last position: %w", err)
	}

	h.logger.Trace("read last LSN position", h.logFields, loglib.Fields{
		logLSNPosition: h.lsnParser.ToString(startPos),
	})

	if startPos == 0 {
		startPos, err = h.getRestartLSN(ctx, conn, h.pgReplicationSlotName)
		if err != nil {
			return fmt.Errorf("get restart LSN: %w", err)
		}
	}

	return h.StartReplicationFromLSN(ctx, startPos)
}

// StartReplicationFromLSN will start the replication process on the configured
// replication slot from the LSN on input.
func (h *Handler) StartReplicationFromLSN(ctx context.Context, lsn replication.LSN) error {
	h.logFields[logLSNPosition] = h.lsnParser.ToString(lsn)
	h.logger.Trace("set start LSN", h.logFields)

	err := h.pgReplicationConn.StartReplication(
		ctx, pglib.ReplicationConfig{
			SlotName:        h.pgReplicationSlotName,
			StartPos:        uint64(lsn),
			PluginArguments: pluginArguments,
		})
	if err != nil {
		return fmt.Errorf("startReplication: %w", err)
	}

	h.logger.Info("logical replication started", h.logFields)

	return h.SyncLSN(ctx, lsn)
}

// ReceiveMessage will listen for messages from the WAL. It returns an error if
// an unexpected message is received.
func (h *Handler) ReceiveMessage(ctx context.Context) (*replication.Message, error) {
	pgMsg, err := h.pgReplicationConn.ReceiveMessage(ctx)
	if err != nil {
		switch {
		case h.isExcludedTableError(err):
			// ignore errors for excluded tables
			return nil, nil
		default:
			return nil, h.mapPostgresError(err)
		}
	}

	return &replication.Message{
		LSN:            replication.LSN(pgMsg.LSN),
		Data:           pgMsg.WALData,
		ServerTime:     pgMsg.ServerTime,
		ReplyRequested: pgMsg.ReplyRequested,
	}, nil
}

// SyncLSN notifies Postgres how far we have processed in the WAL.
func (h *Handler) SyncLSN(ctx context.Context, lsn replication.LSN) error {
	err := h.pgReplicationConn.SendStandbyStatusUpdate(ctx, uint64(lsn))
	if err != nil {
		return fmt.Errorf("syncLSN: send status update: %w", err)
	}
	h.logger.Trace("stored new LSN position", loglib.Fields{
		logLSNPosition: h.lsnParser.ToString(lsn),
	})
	return nil
}

// GetReplicationLag will return the consumer current replication lag. This
// value is different from the postgres replication lag, which takes into
// consideration all consumers and ongoing transactions.
func (h *Handler) GetReplicationLag(ctx context.Context) (int64, error) {
	conn, err := h.pgConnBuilder()
	if err != nil {
		return -1, err
	}
	defer conn.Close(ctx)

	var lag int64
	lagQuery := `SELECT (pg_current_wal_lsn() - confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name=$1`
	if err := conn.QueryRow(ctx, []any{&lag}, lagQuery, h.pgReplicationSlotName); err != nil {
		return -1, err
	}

	return lag, nil
}

func (h *Handler) GetCurrentLSN(ctx context.Context) (replication.LSN, error) {
	conn, err := h.pgConnBuilder()
	if err != nil {
		return 0, fmt.Errorf("creating pg connection: %w", err)
	}
	defer conn.Close(ctx)

	var currentLSN string
	err = conn.QueryRow(ctx, []any{&currentLSN}, `SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name=$1`, h.pgReplicationSlotName)
	if err != nil {
		return 0, err
	}
	return h.lsnParser.FromString(currentLSN)
}

func (h *Handler) GetReplicationSlotName() string {
	return h.pgReplicationSlotName
}

func (h *Handler) ResetConnection(ctx context.Context) error {
	conn, err := h.pgReplicationConnBuilder()
	if err != nil {
		return err
	}
	if h.pgReplicationConn != nil {
		h.pgReplicationConn.Close(ctx)
	}
	h.pgReplicationConn = conn

	return h.StartReplication(ctx)
}

// GetLSNParser returns a postgres implementation of the LSN parser.
func (h *Handler) GetLSNParser() replication.LSNParser {
	return h.lsnParser
}

// Close closes the database connections.
func (h *Handler) Close() error {
	return h.pgReplicationConn.Close(context.Background())
}

// getRestartLSN returns the absolute earliest possible LSN we can support. If
// the consumer's LSN is earlier than this, we cannot (easily) catch the
// consumer back up.
func (h *Handler) getRestartLSN(ctx context.Context, conn pglib.Querier, slotName string) (replication.LSN, error) {
	var restartLSN string
	err := conn.QueryRow(
		ctx,
		[]any{&restartLSN},
		`select restart_lsn from pg_replication_slots where slot_name=$1`,
		slotName,
	)
	if err != nil {
		// TODO: improve error message in case the slot doesn't exist
		return 0, err
	}
	return h.lsnParser.FromString(restartLSN)
}

// getLastSyncedLSN gets the `confirmed_flush_lsn` from PG. This is the last LSN
// that the consumer confirmed it had completed.
func (h *Handler) getLastSyncedLSN(ctx context.Context, conn pglib.Querier) (replication.LSN, error) {
	var confirmedFlushLSN string
	err := conn.QueryRow(ctx, []any{&confirmedFlushLSN}, `select confirmed_flush_lsn from pg_replication_slots where slot_name=$1`, h.pgReplicationSlotName)
	if err != nil {
		return 0, err
	}

	return h.lsnParser.FromString(confirmedFlushLSN)
}

func (h *Handler) verifyReplicationSlotExists(ctx context.Context) error {
	slotExists := false
	conn, err := h.pgConnBuilder()
	if err != nil {
		return fmt.Errorf("creating pg connection: %w", err)
	}
	defer conn.Close(context.Background())

	err = conn.QueryRow(ctx, []any{&slotExists}, `SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)`, h.pgReplicationSlotName)
	if err != nil {
		return fmt.Errorf("retrieving replication slot: %w", err)
	}
	if !slotExists {
		return fmt.Errorf("replication slot %q does not exist", h.pgReplicationSlotName)
	}
	return nil
}

func (h *Handler) mapPostgresError(err error) error {
	// log and ignore warnings
	replErr := &pglib.Error{}
	if errors.As(err, &replErr) && replErr.Severity == "WARNING" {
		h.logger.Warn(err, "receiving message")
		return nil
	}

	h.logger.Error(err, "receiving message")

	if errors.Is(err, pglib.ErrConnTimeout) {
		return replication.ErrConnTimeout
	}

	return err
}

// Format for no tuple error:
// no tuple identifier for DELETE in table \"public\".\"test3\"
// no tuple identifier for UPDATE in table \"public\".\"test3\"
var noTupleRegex = regexp.MustCompile(`no tuple identifier for (DELETE|UPDATE) in table "(.*)"."(.*)"`)

func (h *Handler) isExcludedTableError(err error) bool {
	replErr := &pglib.Error{}
	if errors.As(err, &replErr) && replErr.Severity == "WARNING" {
		matches := noTupleRegex.FindStringSubmatch(replErr.Msg)
		if len(matches) == 4 {
			schema := matches[2]
			table := matches[3]
			if len(h.excludedTables) > 0 {
				if h.excludedTables.ContainsSchemaTable(schema, table) {
					return true
				}
			}
			if len(h.includedTables) > 0 {
				if !h.includedTables.ContainsSchemaTable(schema, table) {
					return true
				}
			}
		}
	}
	return false
}
