// SPDX-License-Identifier: Apache-2.0

package transaction

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
)

const (
	acknowledgementTokenDelimiter = "--"
	acknowledgementTokenPrefix    = "__pgstream_tx_ack__:"
)

var (
	errDuplicateTransaction = errors.New("transaction is already being tracked")
	errUnknownTransaction   = errors.New("transaction is not being tracked")
	errCommittedTransaction = errors.New("transaction is already committed")
	errUnknownAckToken      = errors.New("unknown transaction acknowledgement token")
)

// Tracker delays source checkpointing until every target-bound entry in a
// transaction has been acknowledged. Completed transactions are released only
// from the head of the queue, so an acknowledged later transaction can never
// move the source beyond an incomplete earlier transaction.
type Tracker struct {
	mu sync.Mutex

	generation uint64
	checkpoint checkpointer.Checkpoint
	queue      []*checkpointState
	byTxID     map[uint64]*checkpointState
	byToken    map[wal.CommitPosition]*checkpointState
	nextToken  uint64

	lastCheckpointed wal.CommitPosition
}

type checkpointState struct {
	transactionID   uint64
	position        wal.CommitPosition
	committed       bool
	forceCheckpoint bool
	pending         map[wal.CommitPosition]struct{}
}

func NewTracker(checkpoint checkpointer.Checkpoint) *Tracker {
	return &Tracker{
		checkpoint: checkpoint,
		byTxID:     map[uint64]*checkpointState{},
		byToken:    map[wal.CommitPosition]*checkpointState{},
	}
}

func (t *Tracker) Begin(transactionID, generation uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.startGeneration(generation)

	if _, exists := t.byTxID[transactionID]; exists {
		return fmt.Errorf("%w: %d", errDuplicateTransaction, transactionID)
	}

	state := &checkpointState{
		transactionID: transactionID,
		pending:       map[wal.CommitPosition]struct{}{},
	}
	t.byTxID[transactionID] = state
	t.queue = append(t.queue, state)

	return nil
}

// TrackEntry registers one target-bound transaction entry and returns the
// opaque token that the target must return through Checkpoint after durable
// delivery.
func (t *Tracker) TrackEntry(transactionID, generation uint64) (wal.CommitPosition, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.startGeneration(generation)

	state, exists := t.byTxID[transactionID]
	if !exists {
		return "", fmt.Errorf("%w: %d", errUnknownTransaction, transactionID)
	}
	if state.committed {
		return "", fmt.Errorf("%w: %d", errCommittedTransaction, transactionID)
	}

	return t.addPendingToken(state), nil
}

// Commit seals a transaction with the exact LSN from its wal2json C record.
// If all of its entries were acknowledged before C arrived, this can advance
// the source checkpoint immediately.
func (t *Tracker) Commit(ctx context.Context, transactionID, generation uint64, position wal.CommitPosition) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.startGeneration(generation)

	state, exists := t.byTxID[transactionID]
	if !exists {
		return fmt.Errorf("%w: %d", errUnknownTransaction, transactionID)
	}
	if state.committed {
		return fmt.Errorf("%w: %d", errCommittedTransaction, transactionID)
	}

	state.position = position
	state.committed = true

	return t.advance(ctx)
}

// TrackStandalone registers a non-transactional event as a one-entry
// checkpoint state. It participates in the same ordered queue, so it cannot
// jump over an incomplete transaction.
func (t *Tracker) TrackStandalone(generation uint64, position wal.CommitPosition) wal.CommitPosition {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.startGeneration(generation)

	state := &checkpointState{
		position:  position,
		committed: true,
		pending:   map[wal.CommitPosition]struct{}{},
	}
	t.queue = append(t.queue, state)

	return t.addPendingToken(state)
}

// TrackKeepalive registers a flush barrier without treating PostgreSQL's
// ServerWALEnd as processed. ServerWALEnd can be ahead of logical decoding, so
// reporting it could skip transactions that have not been emitted yet. Once
// the barrier reaches the head, the tracker instead re-reports its last safe
// transaction position (or 0/0 before the first one).
func (t *Tracker) TrackKeepalive(generation uint64) wal.CommitPosition {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.startGeneration(generation)

	state := &checkpointState{
		committed:       true,
		forceCheckpoint: true,
		pending:         map[wal.CommitPosition]struct{}{},
	}
	t.queue = append(t.queue, state)

	return t.addPendingToken(state)
}

// Checkpoint consumes target acknowledgements. It intentionally accepts the
// existing Checkpoint function signature so all target writers can use it
// without learning about PostgreSQL transactions.
func (t *Tracker) Checkpoint(ctx context.Context, positions []wal.CommitPosition) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, position := range positions {
		token, valid := parseAcknowledgementToken(position)
		if !valid {
			return fmt.Errorf("%w: %q", errUnknownAckToken, position)
		}
		if token.generation != t.generation {
			// The target finished work queued by an older connection. PostgreSQL
			// will replay it from the last confirmed LSN, so this acknowledgement
			// must neither fail nor advance the current generation.
			continue
		}

		state, exists := t.byToken[position]
		if !exists {
			if token.id > 0 && token.id <= t.nextToken {
				continue
			}
			return fmt.Errorf("%w: %q", errUnknownAckToken, position)
		}
		delete(state.pending, position)
		delete(t.byToken, position)
	}

	return t.advance(ctx)
}

func (t *Tracker) addPendingToken(state *checkpointState) wal.CommitPosition {
	t.nextToken++
	token := wal.CommitPosition(fmt.Sprintf("%s%d%s%d", acknowledgementTokenPrefix, t.nextToken, acknowledgementTokenDelimiter, t.generation))
	state.pending[token] = struct{}{}
	t.byToken[token] = state
	return token
}

// advance must be called with t.mu held. States are removed only after the
// underlying source checkpointer succeeds; on failure they remain ready so a
// subsequent acknowledgement can retry the same safe prefix.
func (t *Tracker) advance(ctx context.Context) error {
	releasable := 0
	var commitPos wal.CommitPosition
	forceCheckpoint := false
	for _, state := range t.queue {
		if !state.committed || len(state.pending) != 0 {
			break
		}

		releasable++
		forceCheckpoint = forceCheckpoint || state.forceCheckpoint
		if state.position != "" && state.position != wal.ZeroLSN {
			commitPos = state.position
		}
	}

	if releasable == 0 {
		return nil
	}

	if commitPos == "" && !forceCheckpoint {
		return nil
	}
	if commitPos == "" && t.lastCheckpointed != "" {
		commitPos = t.lastCheckpointed
	}

	if commitPos == "" {
		commitPos = wal.ZeroLSN
	}

	if t.checkpoint != nil {
		if err := t.checkpoint(ctx, []wal.CommitPosition{commitPos}); err != nil {
			return err
		}
	}

	if commitPos != wal.ZeroLSN {
		t.lastCheckpointed = commitPos
	}

	for _, state := range t.queue[:releasable] {
		if state.transactionID != 0 {
			delete(t.byTxID, state.transactionID)
		}
	}
	clear(t.queue[:releasable])
	t.queue = t.queue[releasable:]

	return nil
}

type acknowledgementToken struct {
	id         uint64
	generation uint64
}

func parseAcknowledgementToken(position wal.CommitPosition) (acknowledgementToken, bool) {
	value := string(position)
	payload, found := strings.CutPrefix(value, acknowledgementTokenPrefix)
	if !found {
		return acknowledgementToken{}, false
	}

	idValue, generationValue, found := strings.Cut(payload, acknowledgementTokenDelimiter)
	if !found {
		return acknowledgementToken{}, false
	}

	id, err := strconv.ParseUint(idValue, 10, 64)
	if err != nil || id == 0 {
		return acknowledgementToken{}, false
	}
	generation, err := strconv.ParseUint(generationValue, 10, 64)
	if err != nil {
		return acknowledgementToken{}, false
	}

	return acknowledgementToken{id: id, generation: generation}, true
}

// startGeneration must be called with t.mu held. A new replication
// connection replays from PostgreSQL's confirmed position, so unresolved work
// from the previous connection is discarded and its later acknowledgements are
// ignored through the generation encoded in their tokens.
func (t *Tracker) startGeneration(generation uint64) {
	if generation == t.generation {
		return
	}

	t.clearState()
	t.generation = generation
}

func (t *Tracker) clearState() {
	t.nextToken = 0
	clear(t.queue)
	t.queue = nil
	t.byTxID = make(map[uint64]*checkpointState)
	t.byToken = make(map[wal.CommitPosition]*checkpointState)
	t.lastCheckpointed = ""
}
