// Package alerts reports engine-owned diagnostics for documents stuck before OpenSearch.
package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/indexname"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
)

const (
	// KindDLQ identifies diagnostics about retained permanent rejections.
	KindDLQ = "dlq"
	// KindOutboxBacklog identifies diagnostics about overdue delivery records.
	KindOutboxBacklog = "outbox_backlog"
)

// Options configures diagnostic index selection and emission thresholds.
type Options struct {
	Enabled            bool
	Index              string
	MinInterval        time.Duration
	OutboxAgeThreshold time.Duration
}

type condition struct {
	active      bool
	lastEmitted time.Time
}

// Service aggregates stuck-document state and emits rate-limited diagnostics.
type Service struct {
	mu       sync.Mutex
	opts     Options
	sink     application.AlertSink
	recorder application.Recorder
	logger   application.Logger
	dlq      outbox.DeadLetterSummary
	states   map[string]condition
}

// New builds a reporter seeded from the durable dead-letter summary.
func New(opts Options, seed outbox.DeadLetterSummary, sink application.AlertSink, recorder application.Recorder, logger application.Logger) *Service {
	normalizedReasons := make(map[string]int)
	for reason, count := range seed.Reasons {
		normalizedReasons[reasonType(reason)] += count
	}
	seed.Reasons = normalizedReasons
	if seed.Indices == nil {
		seed.Indices = make(map[string]int)
	}
	return &Service{opts: opts, sink: sink, recorder: recorder, logger: logger, dlq: seed, states: make(map[string]condition)}
}

// Reconfigure applies alert settings from a validated configuration reload.
func (s *Service) Reconfigure(opts Options) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.opts.Enabled != opts.Enabled || s.opts.Index != opts.Index {
		s.states = make(map[string]condition)
	}
	s.opts = opts
}

// ApplyDeadLetterChange updates the process-local aggregate after a state transaction.
func (s *Service) ApplyDeadLetterChange(change application.DeadLetterChange) {
	if !change.Changed {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, removed := range change.Removed {
		decrement(s.dlq.Reasons, reasonType(removed.Type))
		decrement(s.dlq.Indices, removed.Index)
	}
	for _, added := range change.Added {
		s.dlq.Reasons[reasonType(added.Type)]++
		s.dlq.Indices[added.Index]++
	}
	s.dlq.Records = change.Records
	s.dlq.Dropped += change.Dropped
}

// Observe emits due dead-letter or outbox-backlog diagnostics.
func (s *Service) Observe(ctx context.Context, records []outbox.Record, now time.Time) {
	s.mu.Lock()
	if !s.opts.Enabled {
		s.mu.Unlock()
		return
	}
	alertIndex := s.opts.Index
	oldest := time.Duration(0)
	if len(records) > 0 {
		oldest = records[0].Age(now)
		if oldest < 0 {
			oldest = 0
		}
	}
	counts := alertCounts{DeadLetterRecords: s.dlq.Records, DeadLetterDropped: s.dlq.Dropped, OutboxRecords: len(records)}
	reasons := sortedReasons(s.dlq.Reasons)
	dlqIndices := sortedPositiveKeys(s.dlq.Indices)
	outboxIndices := recordIndices(records)
	var documents []Document
	if document, emit := s.transition(KindDLQ, s.dlq.Records > 0, now, counts, reasons, dlqIndices, oldest); emit {
		documents = append(documents, document)
	}
	if document, emit := s.transition(KindOutboxBacklog, oldest > s.opts.OutboxAgeThreshold, now, counts, reasons, outboxIndices, oldest); emit {
		documents = append(documents, document)
	}
	s.mu.Unlock()

	for _, document := range documents {
		raw, err := json.Marshal(document)
		if err == nil {
			index := indexname.Resolve(alertIndex, now, now)
			err = s.sink.DeliverAlert(ctx, application.AlertRecord{Index: index, Document: raw})
		}
		s.recorder.AlertEmitted(document.Kind)
		if err != nil && s.logger != nil {
			s.logger.ErrorContext(ctx, "alert document rejected and discarded",
				slog.String("kind", document.Kind), slog.String("error", err.Error()))
		}
	}
}

// ReasonCount is one stable rejection-type count in a diagnostic document.
type ReasonCount struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

type alertCounts struct {
	DeadLetterRecords int `json:"dead_letter_records"`
	DeadLetterDropped int `json:"dead_letter_dropped"`
	OutboxRecords     int `json:"outbox_records"`
}

// Document is the engine-owned, producer-data-free diagnostic shape.
type Document struct {
	Timestamp              time.Time     `json:"@timestamp"`
	Kind                   string        `json:"kind"`
	State                  string        `json:"state"`
	Counts                 alertCounts   `json:"counts"`
	Reasons                []ReasonCount `json:"reasons"`
	Indices                []string      `json:"indices"`
	OldestOutboxAgeSeconds float64       `json:"oldest_outbox_age_seconds"`
	Summary                string        `json:"summary"`
}

func (s *Service) transition(kind string, active bool, now time.Time, counts alertCounts, reasons []ReasonCount, indices []string, oldest time.Duration) (Document, bool) {
	state := s.states[kind]
	phase := ""
	switch {
	case active && !state.active:
		phase = "starting"
	case active && now.Sub(state.lastEmitted) >= s.opts.MinInterval:
		phase = "ongoing"
	case !active && state.active:
		phase = "clearing"
	default:
		return Document{}, false
	}
	state.active = active
	state.lastEmitted = now
	s.states[kind] = state
	return Document{
		Timestamp: now, Kind: kind, State: phase, Counts: counts,
		Reasons: reasons, Indices: indices, OldestOutboxAgeSeconds: oldest.Seconds(),
		Summary: summary(kind, phase, counts, oldest),
	}, true
}

func summary(kind, phase string, counts alertCounts, oldest time.Duration) string {
	if kind == KindDLQ {
		return fmt.Sprintf("dead-letter condition %s: %d retained, %d dropped", phase, counts.DeadLetterRecords, counts.DeadLetterDropped)
	}
	return fmt.Sprintf("outbox backlog condition %s: %d records, oldest %.0fs", phase, counts.OutboxRecords, oldest.Seconds())
}

func sortedReasons(counts map[string]int) []ReasonCount {
	keys := sortedPositiveKeys(counts)
	out := make([]ReasonCount, 0, len(keys))
	for _, key := range keys {
		out = append(out, ReasonCount{Type: key, Count: counts[key]})
	}
	return out
}

func sortedPositiveKeys(counts map[string]int) []string {
	keys := make([]string, 0, len(counts))
	for key, count := range counts {
		if count > 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func recordIndices(records []outbox.Record) []string {
	counts := make(map[string]int)
	for _, record := range records {
		counts[record.Index]++
	}
	return sortedPositiveKeys(counts)
}

func decrement(counts map[string]int, key string) {
	if counts[key] <= 1 {
		delete(counts, key)
		return
	}
	counts[key]--
}

func reasonType(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}

var _ application.StuckReporter = (*Service)(nil)
