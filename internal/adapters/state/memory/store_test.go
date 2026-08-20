package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/adapters/state/memory"
	"github.com/kraicdesign/flow-stitch/internal/adapters/state/statetest"
	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

func TestConformance(t *testing.T) {
	statetest.Run(t, func(*testing.T) application.StateStore { return memory.New() })
}

func testRule() rule.Rule {
	return rule.Rule{
		ID:        "http-flow",
		Version:   "1",
		Enabled:   true,
		Lifecycle: rule.Lifecycle{Timeout: 10 * time.Second},
	}
}

func TestFlowRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	r := testRule()
	now := time.Now()
	key := flow.Key{RuleID: r.ID, CorrelationKey: "abc"}

	f := flow.Open(key, r, event.Event{Doc: map[string]any{"event": "http.request"}, ObservedAt: now})
	f.Apply(event.Event{Doc: map[string]any{"event": "http.request"}, ObservedAt: now}, r, now)

	err := store.WithTx(ctx, func(tx application.Tx) error {
		return tx.SaveFlow(ctx, f)
	})
	if err != nil {
		t.Fatalf("SaveFlow: %v", err)
	}

	err = store.WithTx(ctx, func(tx application.Tx) error {
		loaded, ok, err := tx.LoadFlow(ctx, key)
		if err != nil {
			return err
		}
		if !ok {
			t.Fatal("LoadFlow: flow not found after save")
		}
		if got := len(loaded.Events()); got != 1 {
			t.Fatalf("len(Events()) = %d, want 1", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("LoadFlow: %v", err)
	}
}

// The timeout manager finds work by asking the store which persisted
// deadlines have passed.
func TestDueFlowsRespectsDeadline(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	r := testRule()
	now := time.Now()
	key := flow.Key{RuleID: r.ID, CorrelationKey: "abc"}

	f := flow.Open(key, r, event.Event{Doc: map[string]any{"event": "http.request"}, ObservedAt: now})
	if err := store.WithTx(ctx, func(tx application.Tx) error { return tx.SaveFlow(ctx, f) }); err != nil {
		t.Fatalf("SaveFlow: %v", err)
	}

	err := store.WithTx(ctx, func(tx application.Tx) error {
		early, err := tx.DueFlows(ctx, now.Add(time.Second), 10)
		if err != nil {
			return err
		}
		if len(early) != 0 {
			t.Fatalf("DueFlows before deadline = %d flows, want 0", len(early))
		}

		late, err := tx.DueFlows(ctx, now.Add(time.Minute), 10)
		if err != nil {
			return err
		}
		if len(late) != 1 {
			t.Fatalf("DueFlows after deadline = %d flows, want 1", len(late))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("DueFlows: %v", err)
	}
}

// ADR-0003: the outbox is keyed by deterministic output ID, so re-finalizing the
// same flow cannot enqueue a second copy of the same document.
func TestEnqueueOutboxIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	record := outbox.Record{OutputID: "deadbeef", Index: "application-flows", CreatedAt: time.Now()}

	err := store.WithTx(ctx, func(tx application.Tx) error {
		if err := tx.EnqueueOutbox(ctx, record); err != nil {
			return err
		}
		return tx.EnqueueOutbox(ctx, record)
	})
	if err != nil {
		t.Fatalf("EnqueueOutbox: %v", err)
	}

	if got := store.PendingRecords(); got != 1 {
		t.Fatalf("PendingRecords() = %d, want 1", got)
	}
}

func TestResolveOutboxRemovesDeliveredRecords(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	record := outbox.Record{OutputID: "deadbeef", Index: "application-flows", CreatedAt: time.Now()}

	err := store.WithTx(ctx, func(tx application.Tx) error {
		if err := tx.EnqueueOutbox(ctx, record); err != nil {
			return err
		}
		_, err := tx.ResolveOutbox(ctx, []outbox.Result{{
			OutputID:    record.OutputID,
			Disposition: outbox.Delivered,
		}})
		return err
	})
	if err != nil {
		t.Fatalf("ResolveOutbox: %v", err)
	}

	if got := store.PendingRecords(); got != 0 {
		t.Fatalf("PendingRecords() = %d, want 0", got)
	}
}

func TestDeadLetterSummaryTracksReasonsIndicesAndDrops(t *testing.T) {
	store := memory.New(1)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := store.WithTx(ctx, func(tx application.Tx) error {
		for _, id := range []projection.OutputID{"one", "two"} {
			if err := tx.EnqueueOutbox(ctx, outbox.Record{OutputID: id, Index: "flows", CreatedAt: now}); err != nil {
				return err
			}
		}
		_, err := tx.ResolveOutbox(ctx, []outbox.Result{
			{OutputID: "one", Disposition: outbox.Permanent, RejectionType: "mapper_parsing_exception"},
			{OutputID: "two", Disposition: outbox.Permanent, RejectionType: "mapper_parsing_exception"},
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	summary, err := store.DeadLetters(ctx)
	if err != nil || summary.Records != 1 || summary.Dropped != 1 || summary.Reasons["mapper_parsing_exception"] != 1 || summary.Indices["flows"] != 1 {
		t.Fatalf("DeadLetters() = %+v, %v", summary, err)
	}
}
