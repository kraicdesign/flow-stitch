// Package deliver drains the durable outbox into a sink.
package deliver

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
)

const (
	defaultBatch       = 64
	defaultMaxRetries  = 8
	baseBackoff        = time.Second
	maximumBackoff     = 5 * time.Minute
	allPendingDeadline = 253402300799 // 9999-12-31T23:59:59Z
)

// Service drains durable and pass-through backlogs into the configured sink.
type Service struct {
	state       application.StateStore
	sink        application.Sink
	dlq         application.Quarantine
	clock       application.Clock
	recorder    application.Recorder
	batch       int
	maxRetries  int
	passthrough application.PassthroughBuffer
	alerts      application.StuckReporter
}

// SetStuckReporter attaches best-effort stuck-document alerting.
func (s *Service) SetStuckReporter(reporter application.StuckReporter) { s.alerts = reporter }

// New builds the delivery use case and optionally attaches pass-through buffering.
func New(state application.StateStore, sink application.Sink, dlq application.Quarantine, clock application.Clock, batch, maxRetries int, recorder application.Recorder, passthrough ...application.PassthroughBuffer) *Service {
	if batch <= 0 {
		batch = defaultBatch
	}
	if maxRetries <= 0 {
		maxRetries = defaultMaxRetries
	}
	service := &Service{state: state, sink: sink, dlq: dlq, clock: clock, batch: batch, maxRetries: maxRetries, recorder: recorder}
	if len(passthrough) > 0 {
		service.passthrough = passthrough[0]
	}
	return service
}

// Drain attempts one ready batch. Sink failures become retry state and do not
// make the process unready while the outbox still has capacity (ADR-0008).
func (s *Service) Drain(ctx context.Context) (int, error) {
	now := s.clock.Now()
	var records []outbox.Record
	if err := s.state.WithTx(ctx, func(tx application.Tx) error {
		var err error
		records, err = tx.PendingOutbox(ctx, now, s.batch)
		return err
	}); err != nil {
		return 0, fmt.Errorf("deliver: load pending: %w", err)
	}
	if len(records) == 0 {
		if err := s.refreshDepth(ctx, now); err != nil {
			return 0, err
		}
		return s.drainPassthrough(ctx)
	}

	results, sinkErr := s.sink.Deliver(ctx, records)
	results = completeResults(records, results, sinkErr)
	recordByID := make(map[projection.OutputID]outbox.Record, len(records))
	for _, record := range records {
		recordByID[record.OutputID] = record
	}

	delivered := 0
	for i := range results {
		result := &results[i]
		record := recordByID[result.OutputID]
		attempt := record.Attempts + 1
		result.Attempts = attempt
		switch result.Disposition {
		case outbox.Delivered:
			delivered++
		case outbox.Retryable:
			if attempt > s.maxRetries {
				result.Disposition = outbox.Permanent
				if result.RejectionType == "" {
					result.RejectionType = "retry_limit_exceeded"
				}
				result.Err = fmt.Errorf("retry limit exceeded after %d attempts: %w", attempt, safeError(result.Err))
			} else {
				result.NextAttemptAt = now.Add(backoff(result.OutputID, attempt))
			}
		}
		if result.Disposition == outbox.Permanent && result.RejectionType == "" {
			result.RejectionType = "unknown"
		}
		if result.Disposition == outbox.Permanent {
			result.DeadLetteredAt = now
		}
	}

	// Capture first. If the DLQ is unavailable, leaving every record in the
	// outbox is safer than removing an undeliverable document.
	for _, result := range results {
		if result.Disposition != outbox.Permanent {
			continue
		}
		record := recordByID[result.OutputID]
		record.Attempts = result.Attempts
		record.LastError = safeError(result.Err).Error()
		record.RejectionType = result.RejectionType
		if err := s.dlq.CaptureRecord(ctx, record, record.LastError); err != nil {
			return 0, fmt.Errorf("deliver: capture permanent record %q: %w", result.OutputID, err)
		}
	}

	var deadLetters application.DeadLetterChange
	if err := s.state.WithTx(ctx, func(tx application.Tx) error {
		var err error
		deadLetters, err = tx.ResolveOutbox(ctx, results)
		return err
	}); err != nil {
		return 0, fmt.Errorf("deliver: resolve outbox: %w", err)
	}
	if deadLetters.Changed {
		s.recorder.DeadLetterDepth(deadLetters.Records, deadLetters.Dropped)
		if s.alerts != nil {
			s.alerts.ApplyDeadLetterChange(deadLetters)
		}
	}
	for _, result := range results {
		s.recorder.SinkAttempt(s.sink.Name(), string(result.Disposition))
	}
	if err := s.refreshDepth(ctx, now); err != nil {
		return delivered, err
	}
	passedThrough, err := s.drainPassthrough(ctx)
	return delivered + passedThrough, err
}

func (s *Service) drainPassthrough(ctx context.Context) (int, error) {
	if s.passthrough == nil {
		return 0, nil
	}
	records := s.passthrough.Pending()
	if len(records) == 0 {
		return 0, nil
	}
	sink, ok := s.sink.(application.PassthroughSink)
	if !ok {
		return 0, errors.New("deliver: sink does not support pass-through")
	}
	results, sinkErr := sink.DeliverPassthrough(ctx, records)
	bySequence := make(map[uint64]application.PassthroughResult, len(results))
	for _, result := range results {
		bySequence[result.Sequence] = result
	}
	completed := make([]application.PassthroughResult, 0, len(records))
	delivered := 0
	for _, record := range records {
		result, exists := bySequence[record.Sequence]
		if !exists {
			result = application.PassthroughResult{Sequence: record.Sequence, Disposition: outbox.Retryable}
		}
		if result.Disposition == outbox.Delivered {
			delivered++
		}
		completed = append(completed, result)
		s.recorder.SinkAttempt(s.sink.Name(), string(result.Disposition))
	}
	s.passthrough.Resolve(completed)
	return delivered, sinkErr
}

func (s *Service) refreshDepth(ctx context.Context, now time.Time) error {
	var records []outbox.Record
	if err := s.state.WithTx(ctx, func(tx application.Tx) error {
		var err error
		records, err = tx.PendingOutbox(ctx, time.Unix(allPendingDeadline, 0).UTC(), 0)
		return err
	}); err != nil {
		return fmt.Errorf("deliver: refresh outbox depth: %w", err)
	}
	oldest := time.Duration(0)
	if len(records) > 0 {
		oldest = records[0].Age(now)
	}
	s.recorder.OutboxDepth(len(records), oldest)
	if s.alerts != nil {
		s.alerts.Observe(ctx, records, now)
	}
	return nil
}

func completeResults(records []outbox.Record, results []outbox.Result, sinkErr error) []outbox.Result {
	byID := make(map[projection.OutputID]outbox.Result, len(results))
	for _, result := range results {
		byID[result.OutputID] = result
	}
	completed := make([]outbox.Result, 0, len(records))
	for _, record := range records {
		result, ok := byID[record.OutputID]
		if !ok {
			err := errors.New("sink omitted delivery result")
			if sinkErr != nil {
				err = fmt.Errorf("sink delivery failed: %w", sinkErr)
			}
			result = outbox.Result{OutputID: record.OutputID, Disposition: outbox.Retryable, Err: err}
		}
		completed = append(completed, result)
	}
	return completed
}

func backoff(id projection.OutputID, attempt int) time.Duration {
	shift := attempt - 1
	if shift > 30 {
		shift = 30
	}
	raw := baseBackoff * time.Duration(1<<shift)
	if raw >= maximumBackoff {
		floor := baseBackoff
		for floor*2 < maximumBackoff {
			floor *= 2
		}
		sum := sha256.Sum256([]byte(string(id) + ":cap"))
		fraction := float64(uint16(sum[0])<<8|uint16(sum[1])) / 65535
		return floor + time.Duration(float64(maximumBackoff-floor)*fraction)
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", id, attempt)))
	// Deterministic 75-100% jitter prevents lockstep while keeping each
	// successive pre-cap deadline larger than the previous one.
	fraction := 0.75 + float64(uint16(sum[0])<<8|uint16(sum[1]))/65535*0.25
	return time.Duration(float64(raw) * fraction)
}

func safeError(err error) error {
	if err == nil {
		return errors.New("sink permanently rejected record")
	}
	return err
}

// Run drains on the sink interval or an early pass-through readiness signal.
func (s *Service) Run(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(s.nextInterval(interval))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if _, err := s.Drain(ctx); err != nil {
				return err
			}
			timer.Reset(s.nextInterval(interval))
		case <-s.ready():
			if _, err := s.Drain(ctx); err != nil {
				return err
			}
		}
	}
}

func (s *Service) nextInterval(sinkInterval time.Duration) time.Duration {
	if s.passthrough != nil {
		passthroughInterval := s.passthrough.FlushInterval()
		if sinkInterval <= 0 || passthroughInterval < sinkInterval {
			return passthroughInterval
		}
	}
	return sinkInterval
}

func (s *Service) ready() <-chan struct{} {
	if s.passthrough == nil {
		return nil
	}
	return s.passthrough.Ready()
}
