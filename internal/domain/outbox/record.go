// Package outbox models the durable hand-off between correlation and delivery.
//
// The outbox exists because OpenSearch cannot join the state store's
// transaction. Finalization writes the record before deleting the open flow;
// replay between those ordered writes is safe because the output ID is
// deterministic (ADR-0003). Delivery happens later and may be retried. A
// sink outage therefore grows disk-backed backlog instead of destroying
// correlation state.
package outbox

import (
	"time"

	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
)

// Record is one finalized document awaiting delivery.
type Record struct {
	// OutputID is the deterministic sink document ID (ADR-0003). It is also the
	// outbox record's primary key, so re-finalizing the same flow cannot
	// enqueue two copies.
	OutputID projection.OutputID

	// Index is the resolved target index for this document.
	Index string

	// Document is the serialized payload. It is stored serialized so that a
	// delivery retry after a restart cannot re-project a different shape from
	// a changed rule version.
	Document []byte

	// CreatedAt drives the outbox-age metric that alerts on backlog.
	CreatedAt time.Time

	// DeadLetteredAt orders operator-visible rejections. ReplayCount survives
	// moves between the outbox and DLQ so repeated rejection remains visible
	// (ADR-0015).
	DeadLetteredAt time.Time
	ReplayCount    int

	// Attempts and LastError support retry classification and the DLQ
	// decision for permanent errors.
	Attempts  int
	LastError string

	// RejectionType is the OpenSearch error type only. The accompanying reason
	// is never retained because it may quote producer data (ADR-0013).
	RejectionType string

	// NextAttemptAt implements exponential backoff with jitter.
	NextAttemptAt time.Time
}

// Age reports how long the record has been waiting. Oldest-pending age is the
// signal that distinguishes "sink is slow" from "sink is broken".
func (r Record) Age(now time.Time) time.Duration {
	return now.Sub(r.CreatedAt)
}

// Ready reports whether the record may be attempted now.
func (r Record) Ready(now time.Time) bool {
	return r.NextAttemptAt.IsZero() || !now.Before(r.NextAttemptAt)
}

// Disposition is the delivery verdict for one record.
type Disposition string

// Delivery dispositions. The distinction between retryable and permanent is
// what keeps an unmappable document from blocking the queue forever.
const (
	// Delivered means the sink acknowledged the document.
	Delivered Disposition = "delivered"

	// Retryable means a transient failure; the record stays in the outbox.
	Retryable Disposition = "retryable"

	// Permanent means the document can never succeed as written (mapping
	// conflict, validation rejection) and belongs in the DLQ.
	Permanent Disposition = "permanent"
)

// Result reports the outcome of one delivery attempt.
type Result struct {
	OutputID       projection.OutputID
	Disposition    Disposition
	Err            error
	RejectionType  string
	DeadLetteredAt time.Time

	// Attempts and NextAttemptAt are assigned by the delivery use case for a
	// retry. Keeping policy out of the store makes both adapters behave alike.
	Attempts      int
	NextAttemptAt time.Time
}

// DeadLetterRef contains only the engine-owned dimensions used to summarize
// retained rejections. It never contains the rejected document.
type DeadLetterRef struct {
	Type  string
	Index string
}

// DeadLetterSummary is seeded once from durable state, then maintained from
// transactional changes rather than rescanned on every delivery drain.
type DeadLetterSummary struct {
	Records int            `json:"records"`
	Dropped int            `json:"dropped"`
	Reasons map[string]int `json:"reasons"`
	Indices map[string]int `json:"indices"`
}

// DeadLetterMetadata is the payload-free administrative view of a rejected
// document. Document bodies are deliberately fetched through a separate,
// explicitly ID-addressed operation (ADR-0015).
type DeadLetterMetadata struct {
	OutputID       projection.OutputID `json:"output_id"`
	Index          string              `json:"index"`
	RejectionType  string              `json:"rejection_type"`
	ByteSize       int                 `json:"byte_size"`
	CreatedAt      time.Time           `json:"created_at"`
	DeadLetteredAt time.Time           `json:"dead_lettered_at"`
	Attempts       int                 `json:"attempts"`
	ReplayCount    int                 `json:"replay_count"`
}

// Metadata returns the payload-free view of a record.
func (r Record) Metadata() DeadLetterMetadata {
	return DeadLetterMetadata{OutputID: r.OutputID, Index: r.Index, RejectionType: r.RejectionType,
		ByteSize: len(r.Document), CreatedAt: r.CreatedAt, DeadLetteredAt: r.DeadLetteredAt,
		Attempts: r.Attempts, ReplayCount: r.ReplayCount}
}

// DeadLetterFilter selects records for administrative listing and replay.
// Zero values do not constrain the selection.
type DeadLetterFilter struct {
	OutputID   projection.OutputID
	ReasonType string
	Index      string
	OlderThan  time.Time
	Limit      int
}

// Matches reports whether a retained record satisfies every configured field.
func (f DeadLetterFilter) Matches(r Record) bool {
	return (f.OutputID == "" || r.OutputID == f.OutputID) &&
		(f.ReasonType == "" || r.RejectionType == f.ReasonType) &&
		(f.Index == "" || r.Index == f.Index) &&
		(f.OlderThan.IsZero() || r.DeadLetteredAt.Before(f.OlderThan))
}

// DeadLetterPage is one stable, output-ID-ordered metadata page.
type DeadLetterPage struct {
	Records    []DeadLetterMetadata
	NextCursor projection.OutputID
}
