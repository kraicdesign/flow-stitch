// Package expire implements the timeout manager.
//
// It owns no state of its own. It finds flows whose persisted deadline has
// passed and asks for them to be finalized; the durable `expires_at` on each
// flow is the truth, and any in-memory heap or timing wheel is only an
// optimization on top of it.
package expire

import (
	"context"
	"log/slog"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

// Service finalizes flows that have run out of time.
type Service struct {
	state    application.StateStore
	rules    application.RuleRegistry
	clock    application.Clock
	logger   application.Logger
	recorder application.Recorder

	// batch bounds how many flows one sweep may finalize, so a large backlog
	// after a restart cannot monopolise the process.
	batch int
}

// New builds the timeout service.
func New(state application.StateStore, rules application.RuleRegistry, clock application.Clock, batch int, recorder application.Recorder, loggers ...application.Logger) *Service {
	if batch <= 0 {
		batch = 128
	}
	service := &Service{state: state, rules: rules, clock: clock, batch: batch, recorder: recorder}
	if len(loggers) > 0 {
		service.logger = loggers[0]
	}
	return service
}

// Sweep finalizes every flow already past its deadline, up to the batch bound.
// It returns how many flows were finalized.
//
// Recovery uses the same code path: on startup, flows already past
// `expires_at` finalize before normal ingestion resumes.
func (s *Service) Sweep(ctx context.Context, now time.Time) (int, error) {
	finalized := 0
	var released []rule.Reference
	err := s.state.WithTx(ctx, func(tx application.Tx) error {
		visited := make(map[flow.Key]struct{})
		queryLimit := s.batch
		for finalized < s.batch {
			keys, err := tx.DueFlows(ctx, now, queryLimit)
			if err != nil {
				return err
			}
			for _, key := range keys {
				if _, seen := visited[key]; seen {
					continue
				}
				visited[key] = struct{}{}
				current, exists, err := tx.LoadFlow(ctx, key)
				if err != nil {
					return err
				}
				if !exists {
					continue
				}
				configured, exists := s.rules.Get(key.RuleID, current.RuleVersion())
				if !exists {
					configured = current.PinnedRule()
					if s.logger != nil {
						s.logger.ErrorContext(ctx, "finalizing with durable rule snapshot: pinned rule version is unavailable",
							slog.String("rule_id", string(key.RuleID)), slog.String("rule_version", string(current.RuleVersion())))
					}
				}
				reason := flow.ReasonRuleUnavailable
				if exists {
					reason = flow.ReasonTimeout
					if closeReason, close := current.ShouldClose(configured, now); close {
						reason = closeReason
					}
				}
				if _, err := application.Finalize(ctx, tx, current, configured, reason, now, s.recorder); err != nil {
					return err
				}
				finalized++
				released = append(released, rule.Reference{ID: key.RuleID, Version: current.RuleVersion()})
				if finalized == s.batch {
					break
				}
			}
			if len(keys) < queryLimit {
				break
			}
			queryLimit += s.batch
		}
		return nil
	})
	if err == nil {
		for _, reference := range released {
			s.rules.Release(reference)
		}
	}
	return finalized, err
}

// Run sweeps on an interval until the context is cancelled.
//
// The interval is an acceleration detail: a longer interval delays emission of
// timed-out flows but can never lose them, because the deadline is persisted.
func (s *Service) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := s.Sweep(ctx, s.clock.Now()); err != nil {
				return err
			}
		}
	}
}
