package ingest_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/adapters/passthrough"
	adapterrules "github.com/kraicdesign/flow-stitch/internal/adapters/rules"
	"github.com/kraicdesign/flow-stitch/internal/adapters/state/memory"
	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/application/deliver"
	"github.com/kraicdesign/flow-stitch/internal/application/expire"
	"github.com/kraicdesign/flow-stitch/internal/application/ingest"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/path"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

var baseTime = time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

type accepting struct{}

func (accepting) AcceptingEvents(context.Context) (bool, string) { return true, "" }

type finalizedCall struct {
	ruleID     rule.ID
	reason     flow.Reason
	age        time.Duration
	incomplete int
}

type recording struct {
	application.NoopRecorder
	received   []rule.ID
	rejected   []string
	duplicates []rule.ID
	opened     []rule.ID
	finalized  []finalizedCall
}

func (r *recording) EventReceived(id rule.ID)    { r.received = append(r.received, id) }
func (r *recording) EventRejected(reason string) { r.rejected = append(r.rejected, reason) }
func (r *recording) EventDuplicate(id rule.ID)   { r.duplicates = append(r.duplicates, id) }
func (r *recording) FlowOpened(id rule.ID)       { r.opened = append(r.opened, id) }
func (r *recording) FlowFinalized(id rule.ID, reason flow.Reason, age time.Duration, incomplete int) {
	r.finalized = append(r.finalized, finalizedCall{id, reason, age, incomplete})
}
func (*recording) OutboxDepth(int, time.Duration) {}
func (*recording) DeadLetterDepth(int, int)       {}
func (*recording) SinkAttempt(string, string)     {}
func (*recording) IngestLatency(time.Duration)    {}
func (*recording) FinalizeLatency(time.Duration)  {}

type quarantine struct {
	events  []event.Event
	reasons []string
}

func (q *quarantine) CaptureEvent(_ context.Context, e event.Event, reason string) error {
	q.events = append(q.events, e)
	q.reasons = append(q.reasons, reason)
	return nil
}
func (*quarantine) CaptureRecord(context.Context, outbox.Record, string) error { return nil }

type unavailableSink struct{}

func (unavailableSink) Deliver(_ context.Context, records []outbox.Record) ([]outbox.Result, error) {
	results := make([]outbox.Result, len(records))
	for i, record := range records {
		results[i] = outbox.Result{OutputID: record.OutputID, Disposition: outbox.Retryable, Err: errors.New("503")}
	}
	return results, nil
}
func (unavailableSink) Name() string { return "opensearch" }

type fixture struct {
	service    *ingest.Service
	store      *memory.Store
	registry   *adapterrules.Registry
	clock      *fakeClock
	quarantine *quarantine
	recorder   *recording
	rule       rule.Rule
}

func newFixture(t *testing.T, mutate func(*rule.Rule)) fixture {
	t.Helper()
	configured := rule.Rule{
		ID: "application-flow", Version: "1", Enabled: true,
		Extract: rule.Extract{EventType: mustPath(t, "$.event"), Timestamp: mustPath(t, "$.datetime")},
		Key:     mustPath(t, "$.flow_id"),
		Stitch: []rule.Stitch{{ID: "api-invocation", GroupBy: []path.Path{mustPath(t, "$.service"), mustPath(t, "$.context.invocation_id")},
			Roles: []rule.Role{{Name: "request", Types: []string{"http.request"}}, {Name: "response", Types: []string{"http.response", "http.error"}}}, Requires: []string{"request", "response"}}},
		Lifecycle: rule.Lifecycle{Timeout: 10 * time.Second, CloseWhen: rule.CloseAllInvocationsComplete},
		Limits:    rule.Limits{MaxEvents: 100}, Output: rule.Output{Index: "application-flows", TimestampSource: rule.TimestampFirstEvent},
	}
	if mutate != nil {
		mutate(&configured)
	}
	store := memory.New()
	registry := adapterrules.NewRegistry([]rule.Rule{configured})
	clock := &fakeClock{now: baseTime.Add(time.Second)}
	q := &quarantine{}
	recorder := &recording{}
	return fixture{service: ingest.New(store, registry, q, accepting{}, clock, recorder), store: store, registry: registry, clock: clock, quarantine: q, recorder: recorder, rule: configured}
}

func openFlowTotal(t *testing.T, store application.StateStore) int {
	t.Helper()
	counts, err := store.OpenFlows(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}

func mustPath(t *testing.T, expression string) path.Path {
	t.Helper()
	compiled, err := path.Compile(expression)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func producerEvent(kind, service, invocation, stamp string, observed time.Time) event.Event {
	doc := map[string]any{"flow_id": "flow-1", "event": kind, "datetime": stamp}
	if service != "" {
		doc["service"] = service
	}
	if invocation != "" {
		doc["context"] = map[string]any{"invocation_id": invocation}
	}
	return event.Event{Doc: doc, ObservedAt: observed}
}

func acceptEvent(t *testing.T, svc *ingest.Service, e event.Event) ingest.Result {
	t.Helper()
	result, err := svc.Accept(context.Background(), e)
	if err != nil {
		t.Fatalf("Accept() = %v", err)
	}
	return result
}

func pending(t *testing.T, store *memory.Store) []outbox.Record {
	t.Helper()
	var records []outbox.Record
	if err := store.WithTx(context.Background(), func(tx application.Tx) error {
		var err error
		records, err = tx.PendingOutbox(context.Background(), baseTime.Add(time.Hour), 100)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return records
}

func decodeDocument(t *testing.T, record outbox.Record) projection.Document {
	t.Helper()
	var document projection.Document
	if err := json.Unmarshal(record.Document, &document); err != nil {
		t.Fatalf("Unmarshal(document) = %v", err)
	}
	return document
}

func TestRequestAndResponseProduceOneMergedDocument(t *testing.T) {
	f := newFixture(t, nil)
	request := producerEvent("http.request", "web", "inv-1", "2026-08-20T12:00:00Z", baseTime)
	response := producerEvent("http.response", "web", "inv-1", "2026-08-20T12:00:00.041Z", baseTime.Add(time.Millisecond))
	if got := acceptEvent(t, f.service, request).Disposition; got != ingest.Correlated {
		t.Fatalf("request disposition = %q, want correlated", got)
	}
	if got := acceptEvent(t, f.service, response); got.Disposition != ingest.Finalized || got.Reason != string(flow.ReasonInvocationsComplete) {
		t.Fatalf("response result = %+v", got)
	}
	if got := f.recorder.finalized; len(got) != 1 || got[0].reason != flow.ReasonInvocationsComplete || got[0].ruleID != f.rule.ID || got[0].incomplete != 0 {
		t.Fatalf("finalized calls = %+v, want invocations_complete", got)
	}
	records := pending(t, f.store)
	if len(records) != 1 {
		t.Fatalf("PendingRecords = %d, want 1", len(records))
	}
	document := decodeDocument(t, records[0])
	if document.Flow.Reason != flow.ReasonInvocationsComplete || document.Flow.EntryCount != 1 || document.Flow.EventCount != 2 {
		t.Fatalf("flow meta = %+v", document.Flow)
	}
	entry := document.Events[0]
	if complete, _ := entry["complete"].(bool); !complete {
		t.Fatalf("entry complete = %v, want true", entry["complete"])
	}
	if got := int64(entry["duration_ms"].(float64)); got != 41 {
		t.Fatalf("duration_ms = %d, want 41", got)
	}
	if _, ok := entry["request"]; !ok {
		t.Fatal("merged entry has no request role")
	}
	if _, ok := entry["response"]; !ok {
		t.Fatal("merged entry has no response role")
	}
}

func TestFlowOpenAcrossReloadFinalizesUnderOriginalRule(t *testing.T) {
	f := newFixture(t, nil)
	acceptEvent(t, f.service, producerEvent("http.request", "web", "inv-1", "2026-08-20T12:00:00Z", baseTime))
	oldVersion := f.rule.Version
	reloaded := f.rule
	reloaded.Version = "reloaded"
	reloaded.Limits.MaxEvents = 1
	f.registry.Publish([]rule.Rule{reloaded})

	result := acceptEvent(t, f.service, producerEvent("http.response", "web", "inv-1", "2026-08-20T12:00:00.041Z", baseTime.Add(time.Millisecond)))
	if result.Reason != string(flow.ReasonInvocationsComplete) {
		t.Fatalf("result reason = %q, want original rule to finalize invocations_complete", result.Reason)
	}
	document := decodeDocument(t, pending(t, f.store)[0])
	if document.Flow.RuleVersion != oldVersion {
		t.Fatalf("rule version = %q, want %q", document.Flow.RuleVersion, oldVersion)
	}
	if _, ok := f.registry.Get(f.rule.ID, oldVersion); ok {
		t.Fatal("original rule version retained after its final flow closed")
	}
	third := reloaded
	third.Version = "third"
	f.registry.Publish([]rule.Rule{third})
	if _, ok := f.registry.Get(reloaded.ID, reloaded.Version); ok {
		t.Fatal("reload candidate reference leaked while appending to an older flow")
	}
}

func TestFinalizationResolvesIndexFromDocumentTimestampAndFallsBackToFinalization(t *testing.T) {
	for _, test := range []struct {
		name      string
		timestamp string
		want      string
	}{
		{"producer timestamp in UTC", "2026-08-21T00:30:00+02:00", "application-flows-2026.08.20"},
		{"missing timestamp", "", "application-flows-2026.08.20"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, func(configured *rule.Rule) {
				configured.Output.Index = "application-flows-{yyyy}.{MM}.{dd}"
			})
			acceptEvent(t, f.service, producerEvent("http.request", "web", "inv-1", test.timestamp, baseTime))
			acceptEvent(t, f.service, producerEvent("http.response", "web", "inv-1", test.timestamp, baseTime.Add(time.Millisecond)))
			records := pending(t, f.store)
			if len(records) != 1 || records[0].Index != test.want {
				t.Fatalf("outbox index = %v, want %q", records, test.want)
			}
		})
	}
}

func TestSinkOutageDoesNotBlockCorrelationAndOutboxGrows(t *testing.T) {
	f := newFixture(t, nil)
	finalizeFlow := func(flowID, invocation string) {
		request := producerEvent("http.request", "web", invocation, "2026-08-20T12:00:00Z", baseTime)
		request.Doc["flow_id"] = flowID
		response := producerEvent("http.response", "web", invocation, "2026-08-20T12:00:00.010Z", baseTime.Add(time.Millisecond))
		response.Doc["flow_id"] = flowID
		acceptEvent(t, f.service, request)
		acceptEvent(t, f.service, response)
	}

	finalizeFlow("flow-1", "inv-1")
	delivery := deliver.New(f.store, unavailableSink{}, f.quarantine, f.clock, 10, 8, f.recorder)
	if _, err := delivery.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	finalizeFlow("flow-2", "inv-2")
	if got := len(pending(t, f.store)); got != 2 {
		t.Fatalf("pending after outage = %d, want 2", got)
	}
	if got := openFlowTotal(t, f.store); got != 0 {
		t.Fatalf("open flows = %d, want 0", got)
	}
}

// ADR-0005: arrival order must not change the result.
func TestResponseThenRequestProducesIdenticalDocument(t *testing.T) {
	request := producerEvent("http.request", "web", "inv-1", "2026-08-20T12:00:00Z", baseTime)
	response := producerEvent("http.response", "web", "inv-1", "2026-08-20T12:00:00.041Z", baseTime)
	forward := newFixture(t, nil)
	acceptEvent(t, forward.service, request)
	acceptEvent(t, forward.service, response)
	reverse := newFixture(t, nil)
	acceptEvent(t, reverse.service, response)
	acceptEvent(t, reverse.service, request)
	if got, want := pending(t, reverse.store)[0].Document, pending(t, forward.store)[0].Document; !reflect.DeepEqual(got, want) {
		t.Fatalf("reverse document differs\ngot  %s\nwant %s", got, want)
	}
}

func TestFanOutWaitsForEveryInvocation(t *testing.T) {
	f := newFixture(t, nil)
	events := []event.Event{
		producerEvent("http.request", "web", "inv-1", "2026-08-20T12:00:00Z", baseTime),
		producerEvent("http.request", "apm", "inv-2", "2026-08-20T12:00:00.010Z", baseTime.Add(time.Millisecond)),
		producerEvent("http.response", "web", "inv-1", "2026-08-20T12:00:00.020Z", baseTime.Add(2*time.Millisecond)),
		producerEvent("http.response", "apm", "inv-2", "2026-08-20T12:00:00.030Z", baseTime.Add(3*time.Millisecond)),
	}
	for i, item := range events {
		result := acceptEvent(t, f.service, item)
		if i < 3 && result.Disposition == ingest.Finalized {
			t.Fatalf("event %d finalized before every invocation completed", i)
		}
	}
	if got := openFlowTotal(t, f.store); got != 0 {
		t.Fatalf("OpenFlows = %d, want 0", got)
	}
	if got := len(pending(t, f.store)); got != 1 {
		t.Fatalf("PendingRecords = %d, want 1", got)
	}
}

// ADR-0005, section 3: no invocations must not be treated as all invocations complete.
func TestPlainFlowDoesNotCloseVacuouslyAndTimesOut(t *testing.T) {
	f := newFixture(t, nil)
	plain := producerEvent("log.message", "", "", "2026-08-20T12:00:00Z", baseTime)
	if got := acceptEvent(t, f.service, plain).Disposition; got != ingest.Correlated {
		t.Fatalf("Disposition = %q, want correlated", got)
	}
	if got := openFlowTotal(t, f.store); got != 1 {
		t.Fatalf("OpenFlows = %d, want 1", got)
	}
	sweeper := expire.New(f.store, f.registry, f.clock, 10, f.recorder)
	count, err := sweeper.Sweep(context.Background(), baseTime.Add(11*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("Sweep() = %d, want 1", count)
	}
	if got := decodeDocument(t, pending(t, f.store)[0]).Flow.Reason; got != flow.ReasonTimeout {
		t.Fatalf("Reason = %q, want timeout", got)
	}
}

func TestIncompleteInvocationIsProjectedOnTimeout(t *testing.T) {
	f := newFixture(t, nil)
	acceptEvent(t, f.service, producerEvent("http.request", "web", "inv-1", "2026-08-20T12:00:00Z", baseTime))
	sweeper := expire.New(f.store, f.registry, f.clock, 10, f.recorder)
	if _, err := sweeper.Sweep(context.Background(), baseTime.Add(11*time.Second)); err != nil {
		t.Fatal(err)
	}
	document := decodeDocument(t, pending(t, f.store)[0])
	if document.Flow.IncompleteInvocations != 1 {
		t.Fatalf("IncompleteInvocations = %d, want 1", document.Flow.IncompleteInvocations)
	}
	if complete := document.Events[0]["complete"].(bool); complete {
		t.Fatal("incomplete entry projected complete")
	}
}

func TestSettleWindowFinalizesWithCloseConditionReason(t *testing.T) {
	f := newFixture(t, func(r *rule.Rule) { r.Lifecycle.Settle = time.Second })
	acceptEvent(t, f.service, producerEvent("http.request", "web", "inv-1", "2026-08-20T12:00:00Z", baseTime))
	result := acceptEvent(t, f.service, producerEvent("http.response", "web", "inv-1", "2026-08-20T12:00:00.041Z", baseTime.Add(time.Millisecond)))
	if result.Disposition != ingest.Correlated {
		t.Fatalf("response disposition = %q, want settle wait", result.Disposition)
	}
	sweeper := expire.New(f.store, f.registry, f.clock, 10, f.recorder)
	count, err := sweeper.Sweep(context.Background(), f.clock.now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("Sweep() = %d, want 1", count)
	}
	if got := decodeDocument(t, pending(t, f.store)[0]).Flow.Reason; got != flow.ReasonInvocationsComplete {
		t.Fatalf("Reason = %q, want invocations_complete", got)
	}
}

func TestIdenticalReplayIncrementsDuplicateCountWithoutAppending(t *testing.T) {
	f := newFixture(t, nil)
	request := producerEvent("http.request", "web", "inv-1", "2026-08-20T12:00:00Z", baseTime)
	acceptEvent(t, f.service, request)
	if got := acceptEvent(t, f.service, event.Event{Doc: request.Doc, ObservedAt: baseTime.Add(time.Millisecond)}).Disposition; got != ingest.Duplicate {
		t.Fatalf("replay disposition = %q, want duplicate", got)
	}
	if got := f.recorder.duplicates; !reflect.DeepEqual(got, []rule.ID{f.rule.ID}) {
		t.Fatalf("duplicate calls = %v, want [%s]", got, f.rule.ID)
	}
	acceptEvent(t, f.service, producerEvent("http.response", "web", "inv-1", "2026-08-20T12:00:00.041Z", baseTime.Add(2*time.Millisecond)))
	document := decodeDocument(t, pending(t, f.store)[0])
	if document.Flow.DuplicateCount != 1 || document.Flow.EventCount != 2 {
		t.Fatalf("flow meta = %+v", document.Flow)
	}
	if got := int(document.Events[0]["duplicate_count"].(float64)); got != 1 {
		t.Fatalf("entry duplicate_count = %d, want 1", got)
	}
}

func TestDifferentEventInOccupiedSlotIsPreservedAsCardinalityAnomaly(t *testing.T) {
	f := newFixture(t, nil)
	first := producerEvent("http.request", "web", "inv-1", "2026-08-20T12:00:00Z", baseTime)
	second := producerEvent("http.request", "web", "inv-1", "2026-08-20T12:00:00.001Z", baseTime.Add(time.Millisecond))
	second.Doc["attempt"] = 2
	acceptEvent(t, f.service, first)
	acceptEvent(t, f.service, second)
	acceptEvent(t, f.service, producerEvent("http.response", "web", "inv-1", "2026-08-20T12:00:00.041Z", baseTime.Add(2*time.Millisecond)))
	document := decodeDocument(t, pending(t, f.store)[0])
	if document.Flow.EventCount != 3 || document.Flow.EntryCount != 2 || document.Flow.DuplicateCount != 0 {
		t.Fatalf("flow meta = %+v", document.Flow)
	}
	if len(document.Anomalies) != 1 || document.Anomalies[0].Kind != flow.AnomalyCardinality {
		t.Fatalf("anomalies = %+v", document.Anomalies)
	}
}

func TestPlainEntriesAreNotDeduplicated(t *testing.T) {
	f := newFixture(t, nil)
	plain := producerEvent("log.message", "", "", "2026-08-20T12:00:00Z", baseTime)
	acceptEvent(t, f.service, plain)
	acceptEvent(t, f.service, event.Event{Doc: plain.Doc, ObservedAt: baseTime.Add(time.Millisecond)})
	sweeper := expire.New(f.store, f.registry, f.clock, 10, f.recorder)
	if _, err := sweeper.Sweep(context.Background(), baseTime.Add(11*time.Second)); err != nil {
		t.Fatal(err)
	}
	document := decodeDocument(t, pending(t, f.store)[0])
	if document.Flow.EventCount != 2 || document.Flow.EntryCount != 2 || document.Flow.DuplicateCount != 0 {
		t.Fatalf("flow meta = %+v", document.Flow)
	}
}

func TestTerminalEventClosesWithIncompleteInvocation(t *testing.T) {
	f := newFixture(t, func(r *rule.Rule) { r.Lifecycle.TerminalEvents = []string{"flow.completed"} })
	acceptEvent(t, f.service, producerEvent("http.request", "web", "inv-1", "2026-08-20T12:00:00Z", baseTime))
	result := acceptEvent(t, f.service, producerEvent("flow.completed", "", "", "2026-08-20T12:00:01Z", baseTime.Add(time.Second)))
	if result.Disposition != ingest.Finalized || result.Reason != string(flow.ReasonTerminalEvent) {
		t.Fatalf("result = %+v", result)
	}
	document := decodeDocument(t, pending(t, f.store)[0])
	if document.Flow.IncompleteInvocations != 1 {
		t.Fatalf("IncompleteInvocations = %d, want 1", document.Flow.IncompleteInvocations)
	}
}

func TestMaxEventsShardsOneKeyIntoDistinctDocuments(t *testing.T) {
	f := newFixture(t, func(r *rule.Rule) { r.Limits.MaxEvents = 1 })
	first := producerEvent("log.message", "", "", "2026-08-20T12:00:00Z", baseTime)
	second := producerEvent("log.message", "", "", "2026-08-20T12:00:01Z", baseTime.Add(time.Second))
	for _, item := range []event.Event{first, second} {
		result := acceptEvent(t, f.service, item)
		if result.Reason != string(flow.ReasonLimitExceeded) {
			t.Fatalf("result = %+v", result)
		}
	}
	records := pending(t, f.store)
	if len(records) != 2 {
		t.Fatalf("PendingRecords = %d, want 2", len(records))
	}
	if records[0].OutputID == records[1].OutputID {
		t.Fatalf("successive batches share output ID %q", records[0].OutputID)
	}
}

func TestRefinalizingUsesSameOutputID(t *testing.T) {
	f := newFixture(t, nil)
	e := producerEvent("log.message", "", "", "2026-08-20T12:00:00Z", baseTime)
	key := flow.Key{RuleID: f.rule.ID, CorrelationKey: "flow-1"}
	current := flow.Open(key, f.rule, e)
	current.Apply(e, f.rule, baseTime)
	err := f.store.WithTx(context.Background(), func(tx application.Tx) error {
		if _, err := application.Finalize(context.Background(), tx, current, f.rule, flow.ReasonTimeout, baseTime.Add(11*time.Second), f.recorder); err != nil {
			return err
		}
		_, err := application.Finalize(context.Background(), tx, current, f.rule, flow.ReasonTimeout, baseTime.Add(11*time.Second), f.recorder)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := f.store.PendingRecords(); got != 1 {
		t.Fatalf("PendingRecords = %d, want 1", got)
	}
}

func TestNoMatchingRuleIsCountedAndDroppedWithoutPassThrough(t *testing.T) {
	f := newFixture(t, nil)
	result, err := f.service.Accept(context.Background(), event.Event{Doc: map[string]any{"event": "log.message"}, ObservedAt: baseTime})
	if err != nil {
		t.Fatalf("Accept() = %v, want nil", err)
	}
	if result.Disposition != ingest.Quarantined || result.Reason != "no matching rule" {
		t.Fatalf("result = %+v", result)
	}
	if len(f.quarantine.events) != 0 {
		t.Fatalf("quarantine = %+v, want no unmatched event", f.quarantine.reasons)
	}
	if !reflect.DeepEqual(f.recorder.rejected, []string{"no matching rule"}) {
		t.Fatalf("rejected calls = %v, want [no matching rule]", f.recorder.rejected)
	}
}

func TestPassthroughBackpressureDoesNotAffectCorrelatedEventsOrCreateOutbox(t *testing.T) {
	f := newFixture(t, nil)
	buffer := passthrough.New(passthrough.Options{Index: "logs", BufferSize: 1, BatchSize: 1, FlushInterval: time.Second, Clock: f.clock, Recorder: f.recorder})
	service := ingest.New(f.store, f.registry, f.quarantine, accepting{}, f.clock, f.recorder, buffer)
	unmatched := event.Event{Doc: map[string]any{"message": "ordinary"}, Raw: []byte(`{"message":"ordinary"}`), ObservedAt: baseTime}
	result, err := service.Accept(context.Background(), unmatched)
	if err != nil || result.Disposition != ingest.PassedThrough {
		t.Fatalf("first unmatched = %+v, %v", result, err)
	}
	if got := f.store.PendingRecords(); got != 0 {
		t.Fatalf("outbox after pass-through = %d, want 0", got)
	}
	if _, err := service.Accept(context.Background(), unmatched); !errors.Is(err, ingest.ErrUnavailable) {
		t.Fatalf("full Accept() = %v, want ErrUnavailable", err)
	}
	correlated := producerEvent("http.request", "web", "inv-1", "2026-08-20T12:00:00Z", baseTime)
	if result, err := service.Accept(context.Background(), correlated); err != nil || result.Disposition != ingest.Correlated {
		t.Fatalf("correlated with full buffer = %+v, %v", result, err)
	}
	if got := buffer.Depth(); got != 1 {
		t.Fatalf("buffer depth = %d, want 1", got)
	}
	secondCorrelated := producerEvent("http.request", "web", "inv-2", "2026-08-20T12:00:01Z", baseTime.Add(time.Second))
	secondCorrelated.Doc["flow_id"] = "flow-2"
	results, err := service.AcceptBatch(context.Background(), []event.Event{unmatched, secondCorrelated})
	if !errors.Is(err, ingest.ErrUnavailable) || len(results) != 1 || results[0].Disposition != ingest.Correlated {
		t.Fatalf("AcceptBatch() = %+v, %v; want correlated event accepted with pass-through backpressure", results, err)
	}
}
