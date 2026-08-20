// Package quarantine captures records that must not be retried forever.
package quarantine

import (
	"context"
	"log/slog"

	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
)

// LogQuarantine records captures to the internal log.
//
// It deliberately logs only engine metadata and never the producer document,
// because producer data may contain secrets or personal data.
//
// This is not a durable input quarantine. Rejected events need a state-store or
// dedicated-DLQ implementation before quarantine can survive a restart.
type LogQuarantine struct {
	logger *slog.Logger
}

// NewLog builds a logging quarantine.
func NewLog(logger *slog.Logger) *LogQuarantine {
	return &LogQuarantine{logger: logger}
}

// CaptureEvent records a rejected input event.
func (q *LogQuarantine) CaptureEvent(_ context.Context, e event.Event, reason string) error {
	q.logger.Warn("event quarantined", slog.String("event", e.String()), slog.String("reason", reason))
	return nil
}

// CaptureRecord records an undeliverable finalized document.
func (q *LogQuarantine) CaptureRecord(_ context.Context, r outbox.Record, _ string) error {
	q.logger.Error("outbox record quarantined",
		slog.String("output_id", string(r.OutputID)),
		slog.String("index", r.Index),
		slog.Int("attempts", r.Attempts),
		slog.String("rejection_type", r.RejectionType),
	)
	return nil
}

var _ application.Quarantine = (*LogQuarantine)(nil)
