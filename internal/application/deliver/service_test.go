package deliver_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/adapters/passthrough"
	"github.com/kraicdesign/flow-stitch/internal/adapters/state/memory"
	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/application/alerts"
	"github.com/kraicdesign/flow-stitch/internal/application/deliver"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
)

type deliveryClock struct{ now time.Time }

func (c *deliveryClock) Now() time.Time { return c.now }

type deliveryRecorder struct {
	application.NoopRecorder
	pending  int
	oldest   time.Duration
	attempts []string
	dlq      int
	dropped  int
}

func (r *deliveryRecorder) OutboxDepth(pending int, oldest time.Duration) {
	r.pending, r.oldest = pending, oldest
}
func (r *deliveryRecorder) SinkAttempt(_ string, result string) {
	r.attempts = append(r.attempts, result)
}
func (r *deliveryRecorder) DeadLetterDepth(records, dropped int) {
	r.dlq, r.dropped = records, dropped
}

type deliverySink struct {
	results []outbox.Result
	err     error
}

func (s deliverySink) Deliver(_ context.Context, records []outbox.Record) ([]outbox.Result, error) {
	if s.results != nil {
		return s.results, s.err
	}
	results := make([]outbox.Result, len(records))
	for i, record := range records {
		results[i] = outbox.Result{OutputID: record.OutputID, Disposition: outbox.Retryable, Err: errors.New("503")}
	}
	return results, s.err
}
func (deliverySink) Name() string { return "test" }

type captureSink struct{ seen []outbox.Record }

func (s *captureSink) Deliver(_ context.Context, records []outbox.Record) ([]outbox.Result, error) {
	s.seen = append(s.seen, records...)
	results := make([]outbox.Result, len(records))
	for i, record := range records {
		results[i] = outbox.Result{OutputID: record.OutputID, Disposition: outbox.Retryable, Err: errors.New("outage")}
	}
	return results, nil
}
func (*captureSink) Name() string { return "test" }

type orderedSink struct{ calls []string }

func (s *orderedSink) Deliver(_ context.Context, records []outbox.Record) ([]outbox.Result, error) {
	s.calls = append(s.calls, "outbox")
	results := make([]outbox.Result, len(records))
	for i, record := range records {
		results[i] = outbox.Result{OutputID: record.OutputID, Disposition: outbox.Delivered}
	}
	return results, nil
}
func (s *orderedSink) DeliverPassthrough(_ context.Context, records []application.PassthroughRecord) ([]application.PassthroughResult, error) {
	s.calls = append(s.calls, "passthrough")
	results := make([]application.PassthroughResult, len(records))
	for i, record := range records {
		results[i] = application.PassthroughResult{Sequence: record.Sequence, Disposition: outbox.Delivered}
	}
	return results, nil
}
func (*orderedSink) Name() string { return "test" }

type alertingSink struct{ alerts []application.AlertRecord }

func (s *alertingSink) Deliver(_ context.Context, records []outbox.Record) ([]outbox.Result, error) {
	results := make([]outbox.Result, len(records))
	for i, record := range records {
		results[i] = outbox.Result{OutputID: record.OutputID, Disposition: outbox.Permanent, Err: errors.New("safe mapping failure"), RejectionType: "mapper_parsing_exception"}
	}
	return results, nil
}
func (s *alertingSink) DeliverAlert(_ context.Context, record application.AlertRecord) error {
	s.alerts = append(s.alerts, record)
	return nil
}
func (*alertingSink) Name() string { return "test" }

type deliveryQuarantine struct {
	records []outbox.Record
	reasons []string
}

func (*deliveryQuarantine) CaptureEvent(context.Context, event.Event, string) error { return nil }
func (q *deliveryQuarantine) CaptureRecord(_ context.Context, record outbox.Record, reason string) error {
	q.records = append(q.records, record)
	q.reasons = append(q.reasons, reason)
	return nil
}

func TestDrainBacksOffRetryableRecordsWithGrowingJitteredDeadlinesAndCap(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	clock := &deliveryClock{now: now}
	store := memory.New()
	enqueue(t, store,
		outbox.Record{OutputID: "one", CreatedAt: now.Add(-time.Minute)},
		outbox.Record{OutputID: "two", CreatedAt: now.Add(-time.Second)},
	)
	recorder := &deliveryRecorder{}
	service := deliver.New(store, deliverySink{}, &deliveryQuarantine{}, clock, 10, 50, recorder)

	var previous time.Duration
	for attempt := 1; attempt <= 12; attempt++ {
		if _, err := service.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		records := allRecords(t, store)
		if len(records) != 2 || records[0].Attempts != attempt || records[1].Attempts != attempt {
			t.Fatalf("attempt %d records = %+v", attempt, records)
		}
		firstDelay := records[0].NextAttemptAt.Sub(clock.now)
		secondDelay := records[1].NextAttemptAt.Sub(clock.now)
		if firstDelay == secondDelay {
			t.Fatalf("attempt %d deadlines are identical: %v", attempt, firstDelay)
		}
		if firstDelay > 5*time.Minute || secondDelay > 5*time.Minute {
			t.Fatalf("attempt %d exceeds cap: %v, %v", attempt, firstDelay, secondDelay)
		}
		if attempt > 1 && firstDelay < previous {
			t.Fatalf("attempt %d delay = %v, want >= %v", attempt, firstDelay, previous)
		}
		previous = firstDelay
		clock.now = later(records[0].NextAttemptAt, records[1].NextAttemptAt)
	}
	if recorder.pending != 2 || recorder.oldest <= 0 {
		t.Fatalf("OutboxDepth = (%d, %v), want two pending", recorder.pending, recorder.oldest)
	}
}

func TestDrainMovesPermanentFailureToQuarantineAndDoesNotBlockFollowingRecord(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	store := memory.New()
	enqueue(t, store,
		outbox.Record{OutputID: "bad", Index: "flows", CreatedAt: now},
		outbox.Record{OutputID: "good", Index: "flows", CreatedAt: now},
	)
	sink := deliverySink{results: []outbox.Result{
		{OutputID: "bad", Disposition: outbox.Permanent, Err: errors.New("mapping conflict")},
		{OutputID: "good", Disposition: outbox.Delivered},
	}}
	dlq := &deliveryQuarantine{}
	recorder := &deliveryRecorder{}
	service := deliver.New(store, sink, dlq, &deliveryClock{now: now}, 10, 3, recorder)
	delivered, err := service.Drain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if delivered != 1 || len(allRecords(t, store)) != 0 {
		t.Fatalf("Drain = %d delivered, %d pending; want 1, 0", delivered, len(allRecords(t, store)))
	}
	if len(dlq.records) != 1 || dlq.records[0].OutputID != "bad" || dlq.reasons[0] != "mapping conflict" {
		t.Fatalf("quarantine = %+v, reasons %v", dlq.records, dlq.reasons)
	}
	if len(recorder.attempts) != 2 {
		t.Fatalf("SinkAttempt calls = %v", recorder.attempts)
	}
}

func TestPermanentRejectionPersistsTypeAndEmitsOneDiagnostic(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	store := memory.New()
	enqueue(t, store, outbox.Record{OutputID: "bad", Index: "flows", CreatedAt: now})
	sink := &alertingSink{}
	dlq := &deliveryQuarantine{}
	recorder := &deliveryRecorder{}
	service := deliver.New(store, sink, dlq, &deliveryClock{now: now}, 10, 3, recorder)
	reporter := alerts.New(alerts.Options{Enabled: true, Index: "flowstitch-alerts", MinInterval: 5 * time.Minute, OutboxAgeThreshold: 5 * time.Minute}, outbox.DeadLetterSummary{}, sink, recorder, nil)
	service.SetStuckReporter(reporter)
	if _, err := service.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(dlq.records) != 1 || dlq.records[0].RejectionType != "mapper_parsing_exception" {
		t.Fatalf("dead-letter record = %+v", dlq.records)
	}
	summary, err := store.DeadLetters(context.Background())
	if err != nil || summary.Reasons["mapper_parsing_exception"] != 1 || summary.Indices["flows"] != 1 {
		t.Fatalf("dead-letter summary = %+v, %v", summary, err)
	}
	if len(sink.alerts) != 1 {
		t.Fatalf("alerts = %d, want 1", len(sink.alerts))
	}
	var document alerts.Document
	if err := json.Unmarshal(sink.alerts[0].Document, &document); err != nil {
		t.Fatal(err)
	}
	if document.Kind != alerts.KindDLQ || len(document.Reasons) != 1 || document.Reasons[0].Type != "mapper_parsing_exception" {
		t.Fatalf("alert document = %+v", document)
	}
}

func TestDrainTurnsFailurePermanentPastRetryLimit(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	store := memory.New()
	enqueue(t, store, outbox.Record{OutputID: "exhausted", CreatedAt: now, Attempts: 2})
	dlq := &deliveryQuarantine{}
	service := deliver.New(store, deliverySink{}, dlq, &deliveryClock{now: now}, 10, 2, &deliveryRecorder{})
	if _, err := service.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(dlq.records) != 1 || dlq.records[0].Attempts != 3 || len(allRecords(t, store)) != 0 {
		t.Fatalf("retry exhaustion: dlq=%+v pending=%+v", dlq.records, allRecords(t, store))
	}
}

func TestDrainReportsDeadLetterCapDrops(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	store := memory.New(2)
	records := []outbox.Record{
		{OutputID: "oldest", CreatedAt: now},
		{OutputID: "middle", CreatedAt: now.Add(time.Second)},
		{OutputID: "newest", CreatedAt: now.Add(2 * time.Second)},
	}
	enqueue(t, store, records...)
	results := make([]outbox.Result, len(records))
	for i := range records {
		results[i] = outbox.Result{OutputID: records[i].OutputID, Disposition: outbox.Permanent, Err: errors.New("bad document")}
	}
	recorder := &deliveryRecorder{}
	service := deliver.New(store, deliverySink{results: results}, &deliveryQuarantine{}, &deliveryClock{now: now.Add(time.Minute)}, 10, 3, recorder)
	if _, err := service.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if recorder.dlq != 2 || recorder.dropped != 1 {
		t.Fatalf("DeadLetterDepth = (%d, %d), want (2, 1)", recorder.dlq, recorder.dropped)
	}
	if count, err := store.DeadLetterRecords(context.Background()); err != nil || count != 2 {
		t.Fatalf("DeadLetterRecords = (%d, %v), want (2, nil)", count, err)
	}
}

func TestDrainTreatsSinkErrorAndMissingResultsAsRetryable(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	store := memory.New()
	enqueue(t, store, outbox.Record{OutputID: "one", CreatedAt: now})
	service := deliver.New(store, deliverySink{err: errors.New("outage")}, &deliveryQuarantine{}, &deliveryClock{now: now}, 10, 3, &deliveryRecorder{})
	if _, err := service.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	records := allRecords(t, store)
	if len(records) != 1 || records[0].Attempts != 1 || records[0].NextAttemptAt.IsZero() {
		t.Fatalf("retry state = %+v", records)
	}
}

func TestDrainRetryReusesIndexStoredAtFinalization(t *testing.T) {
	now := time.Date(2026, 8, 22, 1, 0, 0, 0, time.UTC)
	store := memory.New()
	enqueue(t, store, outbox.Record{OutputID: "one", Index: "application-flows-2026.08.21", CreatedAt: now.Add(-time.Hour)})
	sink := &captureSink{}
	service := deliver.New(store, sink, &deliveryQuarantine{}, &deliveryClock{now: now}, 10, 3, &deliveryRecorder{})
	if _, err := service.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.seen) != 1 || sink.seen[0].Index != "application-flows-2026.08.21" {
		t.Fatalf("delivered indices = %+v", sink.seen)
	}
	records := allRecords(t, store)
	if len(records) != 1 || records[0].Index != "application-flows-2026.08.21" {
		t.Fatalf("stored retry = %+v", records)
	}
}

func TestDrainPrioritizesOutboxBeforePassthrough(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	store := memory.New()
	enqueue(t, store, outbox.Record{OutputID: "flow", Index: "flows", Document: []byte(`{}`), CreatedAt: now})
	buffer := passthrough.New(passthrough.Options{Index: "logs", BufferSize: 2, BatchSize: 2, FlushInterval: time.Second, Clock: &deliveryClock{now}, Recorder: application.NoopRecorder{}})
	if err := buffer.Accept(event.Event{Doc: map[string]any{"message": "ordinary"}}); err != nil {
		t.Fatal(err)
	}
	sink := &orderedSink{}
	service := deliver.New(store, sink, &deliveryQuarantine{}, &deliveryClock{now}, 10, 3, &deliveryRecorder{}, buffer)
	if _, err := service.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.calls) != 2 || sink.calls[0] != "outbox" || sink.calls[1] != "passthrough" || buffer.Depth() != 0 {
		t.Fatalf("drain calls=%v depth=%d, want outbox then pass-through", sink.calls, buffer.Depth())
	}
}

func enqueue(t *testing.T, store *memory.Store, records ...outbox.Record) {
	t.Helper()
	if err := store.WithTx(context.Background(), func(tx application.Tx) error {
		for _, record := range records {
			if err := tx.EnqueueOutbox(context.Background(), record); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func allRecords(t *testing.T, store *memory.Store) []outbox.Record {
	t.Helper()
	var records []outbox.Record
	if err := store.WithTx(context.Background(), func(tx application.Tx) error {
		var err error
		records, err = tx.PendingOutbox(context.Background(), time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC), 0)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return records
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
