// Package admin implements authenticated-surface use cases independently of
// their HTTP representation.
package admin

import (
	"context"

	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
)

// Service inspects and replays retained permanent rejections.
type Service struct {
	state    application.StateStore
	clock    application.Clock
	recorder application.Recorder
	alerts   application.StuckReporter
}

// New builds the administrative use case over the running service's state store.
func New(state application.StateStore, clock application.Clock, recorder application.Recorder, alerts application.StuckReporter) *Service {
	return &Service{state: state, clock: clock, recorder: recorder, alerts: alerts}
}

// List returns dead-letter summary and payload-free page data.
func (s *Service) List(ctx context.Context, filter outbox.DeadLetterFilter, cursor projection.OutputID) (outbox.DeadLetterSummary, outbox.DeadLetterPage, error) {
	summary, err := s.state.DeadLetters(ctx)
	if err != nil {
		return outbox.DeadLetterSummary{}, outbox.DeadLetterPage{}, err
	}
	page, err := s.state.ListDeadLetters(ctx, filter, cursor)
	return summary, page, err
}

// Get returns one explicitly selected dead-letter record including its payload.
func (s *Service) Get(ctx context.Context, id projection.OutputID) (outbox.Record, bool, error) {
	return s.state.DeadLetter(ctx, id)
}

// Replay previews or atomically returns selected dead letters to the outbox.
func (s *Service) Replay(ctx context.Context, filter outbox.DeadLetterFilter, dryRun bool) ([]outbox.DeadLetterMetadata, error) {
	if dryRun {
		page, err := s.state.ListDeadLetters(ctx, filter, "")
		return page.Records, err
	}
	var records []outbox.DeadLetterMetadata
	var change application.DeadLetterChange
	err := s.state.WithTx(ctx, func(tx application.Tx) error {
		var err error
		records, change, err = tx.ReplayDeadLetters(ctx, filter, s.clock.Now())
		return err
	})
	if err != nil {
		return nil, err
	}
	if change.Changed {
		s.recorder.DeadLetterDepth(change.Records, 0)
		s.recorder.DeadLetterReplayed(len(records))
		if s.alerts != nil {
			s.alerts.ApplyDeadLetterChange(change)
		}
	}
	return records, nil
}
