// SPDX-License-Identifier: Apache-2.0

package transaction

import (
	"context"
	"fmt"

	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// Processor consumes transaction control events and assigns acknowledgement
// tokens to target-bound data. It must sit directly above the target and below
// filters, so intentionally filtered entries are not counted as pending.
type Processor struct {
	processor processor.Processor
	tracker   *Tracker
}

func NewProcessor(p processor.Processor, tracker *Tracker) *Processor {
	return &Processor{processor: p, tracker: tracker}
}

func (p *Processor) ProcessWALEvent(ctx context.Context, event *wal.Event) error {
	if event == nil {
		return p.processor.ProcessWALEvent(ctx, event)
	}

	if event.Transaction != nil {
		switch event.Transaction.Boundary {
		case wal.TransactionBegin:
			return p.tracker.Begin(event.Transaction.ID, event.Generation)
		case wal.TransactionCommit:
			return p.tracker.Commit(ctx, event.Transaction.ID, event.Generation, event.CommitPosition)
		case wal.TransactionEntry:
			// An earlier pipeline stage can deliberately remove the row data.
			// For example, the injector ignores rows without a usable identity,
			// and the listener ignores pgstream's own internal table changes.
			// There is nothing to write to the target in that case, so the entry
			// is already complete. Do not create a pending token or forward an
			// empty event just to make the batch writer acknowledge it. The C
			// record will still checkpoint the transaction after all real target
			// writes have been acknowledged.
			if event.Data == nil {
				return nil
			}
			token, err := p.tracker.TrackEntry(event.Transaction.ID, event.Generation)
			if err != nil {
				return err
			}
			trackedEvent := *event
			trackedEvent.CheckpointPosition = token
			return p.processor.ProcessWALEvent(ctx, &trackedEvent)
		default:
			return fmt.Errorf("unknown transaction boundary: %d", event.Transaction.Boundary)
		}
	}

	if event.CommitPosition == "" {
		return p.processor.ProcessWALEvent(ctx, event)
	}

	trackedEvent := *event
	if event.Data == nil {
		trackedEvent.CheckpointPosition = p.tracker.TrackKeepalive(event.Generation)
	} else {
		trackedEvent.CheckpointPosition = p.tracker.TrackStandalone(event.Generation, event.CommitPosition)
	}
	return p.processor.ProcessWALEvent(ctx, &trackedEvent)
}

func (p *Processor) Name() string {
	return p.processor.Name()
}

func (p *Processor) Close() error {
	return p.processor.Close()
}
