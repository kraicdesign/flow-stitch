package application

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/indexname"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

// Finalize projects and enqueues a flow before deleting its open state (ADR-0003, section 3).
func Finalize(ctx context.Context, tx Tx, f *flow.Flow, configured rule.Rule, reason flow.Reason, now time.Time, recorder Recorder) (projection.OutputID, error) {
	started := time.Now()
	defer func() { recorder.FinalizeLatency(time.Since(started)) }()
	snapshot := f.Finalize(reason, now)
	document, err := projection.Project(snapshot, configured)
	if err != nil {
		return "", fmt.Errorf("finalize: project: %w", err)
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return "", fmt.Errorf("finalize: marshal: %w", err)
	}
	outputID := projection.NewOutputID(configured.ID, f.Key().CorrelationKey, f.FirstObservedAt())
	// Resolve once and persist the target. Retrying in a later UTC day must not
	// create the same deterministic ID in a second index (ADR-0008, section 3).
	index := indexname.Resolve(configured.Output.Index, document.Timestamp, now)
	if err := tx.EnqueueOutbox(ctx, outbox.Record{OutputID: outputID, Index: index, Document: raw, CreatedAt: now}); err != nil {
		return "", fmt.Errorf("finalize: enqueue outbox: %w", err)
	}
	if err := tx.DeleteFlow(ctx, f.Key()); err != nil {
		return "", fmt.Errorf("finalize: delete flow: %w", err)
	}
	recorder.FlowFinalized(configured.ID, reason, now.Sub(f.FirstObservedAt()), f.IncompleteInvocations())
	return outputID, nil
}
