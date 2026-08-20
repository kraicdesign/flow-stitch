// Package memory is a non-durable [application.StateStore] used for correlation
// tests and local experimentation.
//
// It exists to keep the port honest — an interface with no implementation
// compile-checks nothing — and to give the correlation engine a fast backend
// for unit tests.
//
// It is NOT a durable deployment option. Everything it holds is lost on restart.
package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

// Store keeps all state in maps behind a single mutex.
//
// The coarse lock is deliberate: it makes the transactional semantics obvious
// at test scale. Production concurrency belongs in a durable store adapter.
type Store struct {
	mu      sync.Mutex
	flows   map[flow.Key]*flow.Flow
	records map[projection.OutputID]outbox.Record
	dead    map[projection.OutputID]outbox.Record
	dropped int
	maxDLQ  int
}

// New builds an empty in-memory store.
func New(maxDLQRecords ...int) *Store {
	maxDLQ := 10000
	if len(maxDLQRecords) > 0 {
		maxDLQ = maxDLQRecords[0]
	}
	return &Store{
		flows:   make(map[flow.Key]*flow.Flow),
		records: make(map[projection.OutputID]outbox.Record),
		dead:    make(map[projection.OutputID]outbox.Record),
		maxDLQ:  maxDLQ,
	}
}

// WithTx runs fn against an isolated snapshot and publishes it only on success.
func (s *Store) WithTx(ctx context.Context, fn func(application.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	working, err := s.clone()
	if err != nil {
		return err
	}
	if err := fn(&tx{store: working, ctx: ctx}); err != nil {
		return err
	}
	s.flows = working.flows
	s.records = working.records
	s.dead = working.dead
	s.dropped = working.dropped
	return nil
}

// Close releases the store. Nothing to release.
func (s *Store) Close() error { return nil }

type tx struct {
	store *Store
	ctx   context.Context
}

func (t *tx) LoadFlow(_ context.Context, key flow.Key) (*flow.Flow, bool, error) {
	f, ok := t.store.flows[key]
	if !ok {
		return nil, false, nil
	}
	cloned, err := cloneFlow(f)
	return cloned, err == nil, err
}

func (t *tx) SaveFlow(_ context.Context, f *flow.Flow) error {
	cloned, err := cloneFlow(f)
	if err != nil {
		return err
	}
	t.store.flows[f.Key()] = cloned
	return nil
}

func (t *tx) DeleteFlow(_ context.Context, key flow.Key) error {
	delete(t.store.flows, key)
	return nil
}

// DueFlows scans every open flow. A durable store must use the ordered expiry
// index instead; a full scan is only tolerable at test scale.
func (t *tx) DueFlows(_ context.Context, before time.Time, limit int) ([]flow.Key, error) {
	due := make([]flow.Key, 0, limit)
	for key, f := range t.store.flows {
		if f.ExpiresAt().After(before) {
			continue
		}
		due = append(due, key)
	}

	sort.Slice(due, func(i, j int) bool {
		return t.store.flows[due[i]].ExpiresAt().Before(t.store.flows[due[j]].ExpiresAt())
	})
	if limit > 0 && len(due) > limit {
		due = due[:limit]
	}
	return due, nil
}

func (t *tx) EnqueueOutbox(_ context.Context, r outbox.Record) error {
	// Keyed by output ID, so re-finalizing the same flow overwrites rather
	// than duplicating (ADR-0003).
	t.store.records[r.OutputID] = r
	return nil
}

func (t *tx) PendingOutbox(_ context.Context, now time.Time, limit int) ([]outbox.Record, error) {
	pending := make([]outbox.Record, 0, limit)
	for _, r := range t.store.records {
		if !r.Ready(now) {
			continue
		}
		pending = append(pending, r)
	}

	sort.Slice(pending, func(i, j int) bool {
		if pending[i].CreatedAt.Equal(pending[j].CreatedAt) {
			return pending[i].OutputID < pending[j].OutputID
		}
		return pending[i].CreatedAt.Before(pending[j].CreatedAt)
	})
	if limit > 0 && len(pending) > limit {
		pending = pending[:limit]
	}
	return pending, nil
}

func (t *tx) ResolveOutbox(_ context.Context, results []outbox.Result) (application.DeadLetterChange, error) {
	deadChanged := false
	change := application.DeadLetterChange{}
	for _, res := range results {
		record, ok := t.store.records[res.OutputID]
		if !ok {
			continue
		}
		switch res.Disposition {
		case outbox.Delivered, outbox.Permanent:
			delete(t.store.records, res.OutputID)
			if res.Disposition == outbox.Permanent {
				deadChanged = true
				if res.Attempts > 0 {
					record.Attempts = res.Attempts
				}
				if res.Err != nil {
					record.LastError = res.Err.Error()
				}
				record.RejectionType = res.RejectionType
				record.DeadLetteredAt = res.DeadLetteredAt
				if previous, exists := t.store.dead[res.OutputID]; exists {
					change.Removed = append(change.Removed, deadLetterRef(previous))
				}
				t.store.dead[res.OutputID] = record
				change.Added = append(change.Added, deadLetterRef(record))
			}
		case outbox.Retryable:
			if res.Attempts > 0 {
				record.Attempts = res.Attempts
			} else {
				record.Attempts++
			}
			record.NextAttemptAt = res.NextAttemptAt
			if res.Err != nil {
				record.LastError = res.Err.Error()
			}
			t.store.records[res.OutputID] = record
		default:
			return application.DeadLetterChange{}, fmt.Errorf("memory state: unknown outbox disposition %q", res.Disposition)
		}
	}
	if !deadChanged {
		return application.DeadLetterChange{}, nil
	}
	dropped := t.store.trimDeadLetters()
	for _, record := range dropped {
		change.Removed = append(change.Removed, deadLetterRef(record))
	}
	change.Records = len(t.store.dead)
	change.Dropped = len(dropped)
	change.Changed = true
	return change, nil
}

func (t *tx) ReplayDeadLetters(_ context.Context, filter outbox.DeadLetterFilter, now time.Time) ([]outbox.DeadLetterMetadata, application.DeadLetterChange, error) {
	ids := make([]projection.OutputID, 0, len(t.store.dead))
	for id, record := range t.store.dead {
		if filter.Matches(record) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if filter.Limit > 0 && len(ids) > filter.Limit {
		ids = ids[:filter.Limit]
	}
	metadata := make([]outbox.DeadLetterMetadata, 0, len(ids))
	change := application.DeadLetterChange{}
	for _, id := range ids {
		record := t.store.dead[id]
		metadata = append(metadata, record.Metadata())
		change.Removed = append(change.Removed, deadLetterRef(record))
		record.Attempts = 0
		record.LastError = ""
		record.RejectionType = ""
		record.DeadLetteredAt = time.Time{}
		record.NextAttemptAt = now
		record.ReplayCount++
		t.store.records[id] = record
		delete(t.store.dead, id)
	}
	if len(metadata) > 0 {
		change.Changed = true
		change.Records = len(t.store.dead)
	}
	return metadata, change, nil
}

func (s *Store) trimDeadLetters() []outbox.Record {
	var dropped []outbox.Record
	for s.maxDLQ >= 0 && len(s.dead) > s.maxDLQ {
		var oldest projection.OutputID
		for id, record := range s.dead {
			candidate, found := s.dead[oldest]
			if !found || record.CreatedAt.Before(candidate.CreatedAt) || (record.CreatedAt.Equal(candidate.CreatedAt) && id < oldest) {
				oldest = id
			}
		}
		dropped = append(dropped, s.dead[oldest])
		delete(s.dead, oldest)
		s.dropped++
	}
	return dropped
}

func deadLetterRef(record outbox.Record) outbox.DeadLetterRef {
	return outbox.DeadLetterRef{Type: record.RejectionType, Index: record.Index}
}

// OpenFlows counts open flows per configured rule for startup gauge seeding.
func (s *Store) OpenFlows(ctx context.Context) (map[rule.Reference]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	counts := make(map[rule.Reference]int)
	for key, current := range s.flows {
		counts[rule.Reference{ID: key.RuleID, Version: current.RuleVersion()}]++
	}
	return counts, nil
}

// OutboxRecords returns the number of documents awaiting delivery.
func (s *Store) OutboxRecords(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return len(s.records), nil
}

// DeadLetterRecords returns the number of retained permanent rejections.
func (s *Store) DeadLetterRecords(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return len(s.dead), nil
}

// DeadLetters summarizes the retained dead-letter set without exposing payloads.
func (s *Store) DeadLetters(ctx context.Context) (outbox.DeadLetterSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return outbox.DeadLetterSummary{}, err
	}
	summary := outbox.DeadLetterSummary{Records: len(s.dead), Dropped: s.dropped, Reasons: make(map[string]int), Indices: make(map[string]int)}
	for _, record := range s.dead {
		summary.Reasons[record.RejectionType]++
		summary.Indices[record.Index]++
	}
	return summary, nil
}

// ListDeadLetters returns one payload-free page after the exclusive cursor.
func (s *Store) ListDeadLetters(ctx context.Context, filter outbox.DeadLetterFilter, cursor projection.OutputID) (outbox.DeadLetterPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return outbox.DeadLetterPage{}, err
	}
	ids := make([]projection.OutputID, 0, len(s.dead))
	for id, record := range s.dead {
		if id > cursor && filter.Matches(record) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	hasMore := filter.Limit > 0 && len(ids) > filter.Limit
	if hasMore {
		ids = ids[:filter.Limit]
	}
	page := outbox.DeadLetterPage{Records: make([]outbox.DeadLetterMetadata, 0, len(ids))}
	for _, id := range ids {
		page.Records = append(page.Records, s.dead[id].Metadata())
	}
	if hasMore {
		page.NextCursor = ids[len(ids)-1]
	}
	return page, nil
}

// DeadLetter returns one explicitly requested record including its document body.
func (s *Store) DeadLetter(ctx context.Context, id projection.OutputID) (outbox.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return outbox.Record{}, false, err
	}
	record, found := s.dead[id]
	record.Document = append([]byte(nil), record.Document...)
	return record, found, nil
}

// PendingRecords reports the outbox depth.
func (s *Store) PendingRecords() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

func (s *Store) clone() (*Store, error) {
	cloned := New(s.maxDLQ)
	cloned.dropped = s.dropped
	for key, current := range s.flows {
		copy, err := cloneFlow(current)
		if err != nil {
			return nil, err
		}
		cloned.flows[key] = copy
	}
	for id, record := range s.records {
		record.Document = append([]byte(nil), record.Document...)
		cloned.records[id] = record
	}
	for id, record := range s.dead {
		record.Document = append([]byte(nil), record.Document...)
		cloned.dead[id] = record
	}
	return cloned, nil
}

func cloneFlow(source *flow.Flow) (*flow.Flow, error) {
	raw, err := source.Encode()
	if err != nil {
		return nil, err
	}
	return flow.Decode(raw)
}

var _ application.StateStore = (*Store)(nil)
var _ application.Tx = (*tx)(nil)
