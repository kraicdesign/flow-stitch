// Package ingest implements event acceptance. It writes flow state before it
// returns, but never waits for the flow to finish or for OpenSearch delivery
// before allowing the ingress adapter to respond (ADR-0003, section 4).
package ingest

import (
	"context"
	"errors"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

// Disposition is what happened to an accepted event. It is reported back to
// the forwarder and counted in metrics.
type Disposition string

// Event dispositions.
const (
	// Correlated means the event joined or opened a flow that is still open.
	Correlated Disposition = "correlated"

	// Finalized means the event completed a flow, which is now in the outbox.
	Finalized Disposition = "finalized"

	// Duplicate means the event was already known and was not stored twice.
	Duplicate Disposition = "duplicate"

	// PassedThrough means the unmatched event entered the bounded in-memory
	// delivery buffer (ADR-0010).
	PassedThrough Disposition = "passed_through"

	// Quarantined means the event was durably captured after a permanent rejection.
	Quarantined Disposition = "quarantined"
)

// Result is the outcome of accepting one event.
type Result struct {
	Disposition Disposition
	Reason      string
}

// Errors returned by the ingest service. Only these two shapes matter to the
// transport: rejected (do not retry) and unavailable (retry later).
var (
	// ErrRejected means the event is permanently unacceptable. The forwarder
	// must not retry it; it has been quarantined instead.
	ErrRejected = errors.New("ingest: event rejected")

	// ErrUnavailable means FlowStitch cannot durably accept right now, e.g.
	// past a capacity high-water mark. The forwarder should retry.
	ErrUnavailable = errors.New("ingest: not accepting events")
)

// Service accepts events and drives the correlation aggregate.
type Service struct {
	state       application.StateStore
	rules       application.RuleRegistry
	quarantine  application.Quarantine
	capacity    application.Capacity
	clock       application.Clock
	recorder    application.Recorder
	passthrough application.PassthroughBuffer
}

// New builds the ingest service from its ports.
func New(
	state application.StateStore,
	rules application.RuleRegistry,
	quarantine application.Quarantine,
	capacity application.Capacity,
	clock application.Clock,
	recorder application.Recorder,
	passthrough ...application.PassthroughBuffer,
) *Service {
	service := &Service{
		state:      state,
		rules:      rules,
		quarantine: quarantine,
		capacity:   capacity,
		clock:      clock,
		recorder:   recorder,
	}
	if len(passthrough) > 0 {
		service.passthrough = passthrough[0]
	}
	return service
}

// Accept processes one event.
//
// Correlated events follow this sequence:
//
//  1. preserve the raw producer document and stamp observed time
//  2. select the first rule whose correlation key resolves
//  3. load or create the durable flow state
//  4. apply stitching, deduplication, limits and closing
//  5. persist an open flow, or enqueue output then delete it (ADR-0003, section 3)
//  6. let sink workers deliver independently
//
// Flow mutation and finalization must happen inside a single [application.Tx], and the
// caller must not report success until WithTx has returned nil.
func (s *Service) Accept(ctx context.Context, e event.Event) (Result, error) {
	started := time.Now()
	defer func() { s.recorder.IngestLatency(time.Since(started)) }()
	configured, matched := s.rules.MatchAndRetain(e)
	if !matched {
		const reason = "no matching rule"
		if s.passthrough != nil {
			if err := s.passthrough.Accept(e); err != nil {
				if errors.Is(err, application.ErrPassthroughDisabled) {
					s.recorder.EventRejected(reason)
					s.recorder.PassthroughDropped()
					return Result{Disposition: Quarantined, Reason: reason}, nil
				}
				if errors.Is(err, application.ErrPassthroughFull) {
					return Result{}, errors.Join(ErrUnavailable, err)
				}
				return Result{}, errors.Join(ErrUnavailable, err)
			}
			return Result{Disposition: PassedThrough}, nil
		}
		s.recorder.EventRejected(reason)
		s.recorder.PassthroughDropped()
		return Result{Disposition: Quarantined, Reason: reason}, nil
	}
	candidateReference := rule.Reference{ID: configured.ID, Version: configured.Version}
	retainedCandidate := true
	defer func() {
		if retainedCandidate {
			s.rules.Release(candidateReference)
		}
	}()
	if ok, reason := s.capacity.AcceptingEvents(ctx); !ok {
		s.recorder.EventRejected("capacity")
		return Result{}, wrapReason(ErrUnavailable, reason)
	}
	s.recorder.EventReceived(configured.ID)
	correlationKey, ok := configured.Key.Resolve(e.Doc)
	if !ok || correlationKey == "" {
		// Registry.Match uses the same compiled path, so this can only happen if
		// the document was mutated concurrently by its caller.
		return Result{}, errors.Join(ErrRejected, errors.New("correlation key disappeared after rule selection"))
	}

	now := s.clock.Now()
	result := Result{}
	opened := false
	var finalizedReference *rule.Reference
	err := s.state.WithTx(ctx, func(tx application.Tx) error {
		key := flow.Key{RuleID: configured.ID, CorrelationKey: correlationKey}
		current, exists, err := tx.LoadFlow(ctx, key)
		if err != nil {
			return err
		}
		if !exists {
			current = flow.Open(key, configured, e)
			s.recorder.FlowOpened(configured.ID)
			opened = true
		} else {
			pinned, available := s.rules.Get(key.RuleID, current.RuleVersion())
			if !available {
				pinned = current.PinnedRule()
			}
			configured = pinned
		}
		outcome := current.Apply(e, configured, now)
		if outcome.Duplicate {
			s.recorder.EventDuplicate(configured.ID)
		}
		if reason, close := current.ShouldClose(configured, now); close {
			if _, err := application.Finalize(ctx, tx, current, configured, reason, now, s.recorder); err != nil {
				return err
			}
			result = Result{Disposition: Finalized, Reason: string(reason)}
			finalizedReference = &rule.Reference{ID: configured.ID, Version: configured.Version}
			return nil
		}
		if err := tx.SaveFlow(ctx, current); err != nil {
			return err
		}
		result = Result{Disposition: Correlated}
		if outcome.Duplicate {
			result.Disposition = Duplicate
		}
		return nil
	})
	if err != nil {
		return Result{}, errors.Join(ErrUnavailable, err)
	}
	if opened && finalizedReference == nil {
		retainedCandidate = false
	}
	if finalizedReference != nil && !opened {
		s.rules.Release(*finalizedReference)
	}
	return result, nil
}

// AcceptBatch processes a batch, reporting one result per event.
//
// TODO(contracts): the exact acknowledgement contract with Fluent Bit remains
// undecided. Partial batch success needs a decided answer: per-event
// results, all-or-nothing, or accept-and-quarantine-the-bad-ones. The response
// shape of POST /v1/events/batch depends on it.
func (s *Service) AcceptBatch(ctx context.Context, events []event.Event) ([]Result, error) {
	results := make([]Result, 0, len(events))
	var passthroughBackpressure error
	for _, e := range events {
		res, err := s.Accept(ctx, e)
		if err != nil {
			if errors.Is(err, application.ErrPassthroughFull) {
				passthroughBackpressure = err
				continue
			}
			return results, err
		}
		results = append(results, res)
	}
	return results, passthroughBackpressure
}

func wrapReason(err error, reason string) error {
	if reason == "" {
		return err
	}
	return errors.Join(err, errors.New(reason))
}
