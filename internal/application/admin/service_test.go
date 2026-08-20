package admin_test

import (
	"context"
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/adapters/state/memory"
	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/application/admin"
	"github.com/kraicdesign/flow-stitch/internal/application/deliver"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
)

type clock struct{ now time.Time }

func (c clock) Now() time.Time { return c.now }

type sink struct{ records []outbox.Record }

func (s *sink) Deliver(_ context.Context, records []outbox.Record) ([]outbox.Result, error) {
	s.records = append(s.records, records...)
	results := make([]outbox.Result, len(records))
	for i, record := range records {
		results[i] = outbox.Result{OutputID: record.OutputID, Disposition: outbox.Delivered}
	}
	return results, nil
}
func (*sink) Name() string { return "stub" }

type quarantine struct{}

func (quarantine) CaptureEvent(context.Context, event.Event, string) error    { return nil }
func (quarantine) CaptureRecord(context.Context, outbox.Record, string) error { return nil }

func TestReplayedRecordReachesSinkWithOriginalIdentity(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	store := memory.New()
	record := outbox.Record{OutputID: "stable-id", Index: "flows-2026.08.21", Document: []byte(`{"ok":true}`), CreatedAt: now}
	if err := store.WithTx(ctx, func(tx application.Tx) error {
		if err := tx.EnqueueOutbox(ctx, record); err != nil {
			return err
		}
		_, err := tx.ResolveOutbox(ctx, []outbox.Result{{OutputID: record.OutputID, Disposition: outbox.Permanent, RejectionType: "mapping", DeadLetteredAt: now}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	service := admin.New(store, clock{now}, application.NoopRecorder{}, nil)
	if records, err := service.Replay(ctx, outbox.DeadLetterFilter{OutputID: record.OutputID, Limit: 1}, false); err != nil || len(records) != 1 {
		t.Fatalf("Replay() = (%+v, %v), want one", records, err)
	}
	target := &sink{}
	delivery := deliver.New(store, target, quarantine{}, clock{now}, 10, 3, application.NoopRecorder{})
	if _, err := delivery.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if len(target.records) != 1 || target.records[0].OutputID != record.OutputID || target.records[0].Index != record.Index {
		t.Fatalf("sink records = %+v, want original id and index", target.records)
	}
}
