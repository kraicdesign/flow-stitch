// Package application holds the use cases and the driven ports they depend on.
//
// This is the hexagon boundary. Everything above it (the domain) is pure;
// everything below it (adapters: HTTP, state stores, OpenSearch, config) is
// replaceable. The correlation policy must never learn about HTTP, OpenSearch
// or the chosen database (ADR-0002).
package application

import (
	"context"
	"errors"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

// Clock supplies the observed-time reference. It is a port so timeout and
// recovery behaviour can be tested deterministically.
type Clock interface {
	Now() time.Time
}

// Logger records operational failures without coupling use cases to a logging adapter.
type Logger interface {
	ErrorContext(ctx context.Context, message string, args ...any)
}

// Recorder receives instrumentation events. It takes domain values, never
// label maps: a map[string]string parameter is how producer-derived labels
// find their way back in (ADR-0007, section 4).
type Recorder interface {
	EventReceived(ruleID rule.ID)
	EventRejected(reason string)
	EventDuplicate(ruleID rule.ID)

	FlowOpened(ruleID rule.ID)
	FlowFinalized(ruleID rule.ID, reason flow.Reason, age time.Duration, incompleteInvocations int)

	OutboxDepth(pending int, oldest time.Duration)
	DeadLetterDepth(records, dropped int)
	SinkAttempt(sink string, result string)
	PassthroughEvent()
	PassthroughBuffer(depth int)
	PassthroughDropped()
	AlertEmitted(kind string)
	DeadLetterReplayed(records int)

	IngestLatency(d time.Duration)
	FinalizeLatency(d time.Duration)
}

// NoopRecorder discards instrumentation. Tests and compositions that do not
// expose metrics use it so application code never needs nil checks.
type NoopRecorder struct{}

// EventReceived implements Recorder without recording a metric.
func (NoopRecorder) EventReceived(rule.ID) {}

// EventRejected implements Recorder without recording a metric.
func (NoopRecorder) EventRejected(string) {}

// EventDuplicate implements Recorder without recording a metric.
func (NoopRecorder) EventDuplicate(rule.ID) {}

// FlowOpened implements Recorder without recording a metric.
func (NoopRecorder) FlowOpened(rule.ID) {}

// FlowFinalized implements Recorder without recording a metric.
func (NoopRecorder) FlowFinalized(rule.ID, flow.Reason, time.Duration, int) {}

// OutboxDepth implements Recorder without recording a metric.
func (NoopRecorder) OutboxDepth(int, time.Duration) {}

// DeadLetterDepth implements Recorder without recording a metric.
func (NoopRecorder) DeadLetterDepth(int, int) {}

// SinkAttempt implements Recorder without recording a metric.
func (NoopRecorder) SinkAttempt(string, string) {}

// PassthroughEvent implements Recorder without recording a metric.
func (NoopRecorder) PassthroughEvent() {}

// PassthroughBuffer implements Recorder without recording a metric.
func (NoopRecorder) PassthroughBuffer(int) {}

// PassthroughDropped implements Recorder without recording a metric.
func (NoopRecorder) PassthroughDropped() {}

// AlertEmitted implements Recorder without recording a metric.
func (NoopRecorder) AlertEmitted(string) {}

// DeadLetterReplayed implements Recorder without recording a metric.
func (NoopRecorder) DeadLetterReplayed(int) {}

// IngestLatency implements Recorder without recording a metric.
func (NoopRecorder) IngestLatency(time.Duration) {}

// FinalizeLatency implements Recorder without recording a metric.
func (NoopRecorder) FinalizeLatency(time.Duration) {}

// RuleRegistry selects and versions correlation rules.
//
// A reload publishes a new snapshot; flows already open keep resolving their
// pinned version through Get until they finalize.
type RuleRegistry interface {
	// Match returns the single rule that owns this event. Ambiguous matches
	// are rejected at config validation time, so Match never has to choose.
	MatchAndRetain(e event.Event) (rule.Rule, bool)

	// Get resolves the exact version an open flow was created under.
	Get(id rule.ID, version rule.Version) (rule.Rule, bool)

	// Release removes one open-flow reference after durable finalization.
	Release(reference rule.Reference)
}

// StateStore is the durable source of truth. Memory is only ever a cache or an
// acceleration structure (ADR-0002).
//
// Every mutation runs inside WithTx so recovery observes either an open flow
// or a finalized outbox record, never neither and never both (ADR-0003).
type StateStore interface {
	// WithTx runs fn inside one atomic unit of work. If fn returns an error,
	// nothing it did is durable.
	WithTx(ctx context.Context, fn func(Tx) error) error

	// OpenFlows returns the number of flows currently held, per rule. It is
	// called once at startup to seed the process-maintained gauge (ADR-0007).
	OpenFlows(ctx context.Context) (map[rule.Reference]int, error)

	// OutboxRecords returns durable backlog depth for readiness high-water checks.
	OutboxRecords(ctx context.Context) (int, error)

	// DeadLetterRecords returns the retained permanent-rejection count.
	DeadLetterRecords(ctx context.Context) (int, error)

	// DeadLetters scans the bounded retained set once at startup to seed
	// process-maintained alert aggregation (ADR-0013).
	DeadLetters(ctx context.Context) (outbox.DeadLetterSummary, error)

	// ListDeadLetters returns payload-free records ordered by output ID. Cursor
	// is exclusive and supports stable paging over the bounded DLQ (ADR-0015).
	ListDeadLetters(ctx context.Context, filter outbox.DeadLetterFilter, cursor projection.OutputID) (outbox.DeadLetterPage, error)

	// DeadLetter fetches one explicitly named record, including its body.
	DeadLetter(ctx context.Context, id projection.OutputID) (outbox.Record, bool, error)

	// Close releases the store. A store that cannot be opened cleanly must
	// fail closed rather than start empty.
	Close() error
}

// Tx is the transactional view of open flows, the expiry index, and the outbox.
type Tx interface {
	// LoadFlow returns the open flow for a key, if one exists.
	LoadFlow(ctx context.Context, key flow.Key) (*flow.Flow, bool, error)

	// SaveFlow persists a still-open flow and its expiry entry.
	SaveFlow(ctx context.Context, f *flow.Flow) error

	// DeleteFlow removes an open flow and its expiry entry.
	DeleteFlow(ctx context.Context, key flow.Key) error

	// DueFlows returns flows whose persisted deadline has passed. The timeout
	// manager reads this on a schedule and again on startup, which is what
	// makes timeouts survive a crash.
	DueFlows(ctx context.Context, before time.Time, limit int) ([]flow.Key, error)

	// EnqueueOutbox stores a finalized document before the open flow is deleted
	// (ADR-0003, section 3).
	EnqueueOutbox(ctx context.Context, r outbox.Record) error

	// PendingOutbox returns records ready for a delivery attempt.
	PendingOutbox(ctx context.Context, now time.Time, limit int) ([]outbox.Record, error)

	// ResolveOutbox applies delivery verdicts: delivered records are removed,
	// retryable ones get a new backoff deadline, permanent ones go to the DLQ.
	ResolveOutbox(ctx context.Context, results []outbox.Result) (DeadLetterChange, error)

	// ReplayDeadLetters atomically moves a bounded selection back to the
	// outbox, ready immediately, while preserving replay history (ADR-0015).
	ReplayDeadLetters(ctx context.Context, filter outbox.DeadLetterFilter, now time.Time) ([]outbox.DeadLetterMetadata, DeadLetterChange, error)
}

// DeadLetterChange reports the retained depth and evictions produced by one
// outbox resolution transaction.
type DeadLetterChange struct {
	Records int
	Dropped int
	Changed bool
	Added   []outbox.DeadLetterRef
	Removed []outbox.DeadLetterRef
}

// Sink delivers finalized documents downstream. It is bulk-shaped because
// one document per request would waste the OpenSearch bulk API.
type Sink interface {
	Deliver(ctx context.Context, records []outbox.Record) ([]outbox.Result, error)

	// Name identifies the sink in metrics and logs.
	Name() string
}

// AlertRecord is an engine-owned diagnostic document. It contains no producer data.
type AlertRecord struct {
	Index    string
	Document []byte
}

// AlertSink delivers one best-effort diagnostic document. Failure is logged
// and discarded; alerts never enter the outbox or dead-letter store (ADR-0013).
type AlertSink interface {
	DeliverAlert(context.Context, AlertRecord) error
}

// StuckReporter maintains alert state from delivery-loop observations.
type StuckReporter interface {
	ApplyDeadLetterChange(DeadLetterChange)
	Observe(context.Context, []outbox.Record, time.Time)
}

// PassthroughRecord is an unmatched producer document waiting in memory for
// delivery. Sequence is internal queue identity and is never sent as an
// OpenSearch document ID (ADR-0010, section 4).
type PassthroughRecord struct {
	Sequence uint64
	Index    string
	Document []byte
}

// PassthroughResult is the sink verdict for one in-memory record.
type PassthroughResult struct {
	Sequence    uint64
	Disposition outbox.Disposition
}

// ErrPassthroughFull asks ingress to apply retryable backpressure.
var (
	ErrPassthroughFull     = errors.New("passthrough buffer full")
	ErrPassthroughDisabled = errors.New("passthrough disabled")
)

// PassthroughBuffer is the consumer-owned port for the bounded, non-durable
// unmatched-event queue (ADR-0010, sections 2 and 3).
type PassthroughBuffer interface {
	Accept(event.Event) error
	Pending() []PassthroughRecord
	Resolve([]PassthroughResult)
	Depth() int
	Ready() <-chan struct{}
	FlushInterval() time.Duration
}

// PassthroughSink delivers documents without assigning an ID.
type PassthroughSink interface {
	DeliverPassthrough(context.Context, []PassthroughRecord) ([]PassthroughResult, error)
}

// Quarantine durably captures records that must not be retried forever.
//
// Capture must be durable before the ingress reports the event as quarantined;
// otherwise a permanent rejection could silently lose the record.
type Quarantine interface {
	CaptureEvent(ctx context.Context, e event.Event, reason string) error
	CaptureRecord(ctx context.Context, r outbox.Record, reason string) error
}

// Capacity reports whether it is still safe to accept new events.
//
// When durability can no longer be guaranteed, ingestion must return a
// retryable failure rather than quietly accept data it may lose.
type Capacity interface {
	// AcceptingEvents reports whether ingestion is within its high-water
	// marks. The reason is surfaced on /health/ready and in the retryable
	// response returned to the forwarder.
	AcceptingEvents(ctx context.Context) (ok bool, reason string)
}
