// Package pebble implements durable application state with CockroachDB Pebble.
package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"sync"
	"time"

	dbpebble "github.com/cockroachdb/pebble/v2"
	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

var (
	flowPrefix    = []byte("f/")
	expiryPrefix  = []byte("x/")
	outboxPrefix  = []byte("o/")
	deadPrefix    = []byte("d/")
	dlqDroppedKey = []byte("m/dlq-dropped")
)

// Store is a durable application.StateStore.
type Store struct {
	db         *dbpebble.DB
	syncWrites bool
	maxDLQ     int
	mu         sync.Mutex
}

// Open opens or creates the database at path. Pebble returns corruption and
// other open failures to the caller; the adapter never substitutes a fresh DB.
func Open(path string, syncWrites bool, maxDLQRecords ...int) (*Store, error) {
	db, err := dbpebble.Open(path, &dbpebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("pebble state: open %s: %w", path, err)
	}
	maxDLQ := 10000
	if len(maxDLQRecords) > 0 {
		maxDLQ = maxDLQRecords[0]
	}
	return &Store{db: db, syncWrites: syncWrites, maxDLQ: maxDLQ}, nil
}

// WithTx serializes and atomically commits one application state transaction.
func (s *Store) WithTx(ctx context.Context, fn func(application.Tx) error) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	batch := s.db.NewIndexedBatch()
	defer closeResource(batch, "transaction batch", &err)
	if err := fn(&tx{batch: batch, maxDLQ: s.maxDLQ}); err != nil {
		return err
	}
	options := dbpebble.NoSync
	if s.syncWrites {
		options = dbpebble.Sync
	}
	if err := batch.Commit(options); err != nil {
		return fmt.Errorf("pebble state: commit: %w", err)
	}
	return nil
}

// OpenFlows scans the bounded flow family once for startup gauge seeding.
func (s *Store) OpenFlows(ctx context.Context) (counts map[rule.Reference]int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	iter, err := s.db.NewIter(prefixOptions(flowPrefix))
	if err != nil {
		return nil, fmt.Errorf("pebble state: count open flows: %w", err)
	}
	defer closeResource(iter, "open-flow iterator", &err)
	counts = make(map[rule.Reference]int)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key, err := parseFlowKey(iter.Key())
		if err != nil {
			return nil, err
		}
		current, err := flow.Decode(iter.Value())
		if err != nil {
			return nil, fmt.Errorf("pebble state: count open flows: decode %q: %w", key.CorrelationKey, err)
		}
		counts[rule.Reference{ID: key.RuleID, Version: current.RuleVersion()}]++
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("pebble state: count open flows: %w", err)
	}
	return counts, nil
}

// OutboxRecords returns the number of documents awaiting delivery.
func (s *Store) OutboxRecords(ctx context.Context) (count int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	iter, err := s.db.NewIter(prefixOptions(outboxPrefix))
	if err != nil {
		return 0, fmt.Errorf("pebble state: count outbox: %w", err)
	}
	defer closeResource(iter, "outbox iterator", &err)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		count++
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("pebble state: count outbox: %w", err)
	}
	return count, nil
}

// DeadLetterRecords returns the number of retained permanent rejections.
func (s *Store) DeadLetterRecords(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return countPrefix(ctx, s.db, deadPrefix, "dead letters")
}

// DeadLetters summarizes the retained dead-letter set without exposing payloads.
func (s *Store) DeadLetters(ctx context.Context) (summary outbox.DeadLetterSummary, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	summary = outbox.DeadLetterSummary{Reasons: make(map[string]int), Indices: make(map[string]int)}
	iter, err := s.db.NewIter(prefixOptions(deadPrefix))
	if err != nil {
		return summary, fmt.Errorf("pebble state: summarize dead letters: %w", err)
	}
	defer closeResource(iter, "dead-letter summary iterator", &err)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		var record outbox.Record
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			return summary, fmt.Errorf("pebble state: summarize dead letter %q: %w", iter.Key(), err)
		}
		summary.Records++
		summary.Reasons[record.RejectionType]++
		summary.Indices[record.Index]++
	}
	if err := iter.Error(); err != nil {
		return summary, fmt.Errorf("pebble state: summarize dead letters: %w", err)
	}
	dropped, closer, err := s.db.Get(dlqDroppedKey)
	if err == nil {
		if len(dropped) != 8 {
			_ = closer.Close()
			return summary, errors.New("pebble state: invalid dead-letter drop count")
		}
		summary.Dropped = int(binary.BigEndian.Uint64(dropped))
		if err := closer.Close(); err != nil {
			return summary, err
		}
	} else if !errors.Is(err, dbpebble.ErrNotFound) {
		return summary, fmt.Errorf("pebble state: read dead-letter drop count: %w", err)
	}
	return summary, nil
}

// ListDeadLetters returns one payload-free page after the exclusive cursor.
func (s *Store) ListDeadLetters(ctx context.Context, filter outbox.DeadLetterFilter, cursor projection.OutputID) (page outbox.DeadLetterPage, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	iter, err := s.db.NewIter(prefixOptions(deadPrefix))
	if err != nil {
		return outbox.DeadLetterPage{}, fmt.Errorf("pebble state: list dead letters: %w", err)
	}
	defer closeResource(iter, "dead-letter list iterator", &err)
	for valid := iter.SeekGE(deadKey(cursor)); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return outbox.DeadLetterPage{}, err
		}
		var record outbox.Record
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			return outbox.DeadLetterPage{}, fmt.Errorf("pebble state: decode dead letter %q: %w", iter.Key(), err)
		}
		if record.OutputID <= cursor || !filter.Matches(record) {
			continue
		}
		if filter.Limit > 0 && len(page.Records) == filter.Limit {
			page.NextCursor = page.Records[len(page.Records)-1].OutputID
			break
		}
		page.Records = append(page.Records, record.Metadata())
	}
	if err := iter.Error(); err != nil {
		return outbox.DeadLetterPage{}, fmt.Errorf("pebble state: list dead letters: %w", err)
	}
	return page, nil
}

// DeadLetter returns one explicitly requested record including its document body.
func (s *Store) DeadLetter(ctx context.Context, id projection.OutputID) (record outbox.Record, found bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return outbox.Record{}, false, err
	}
	raw, closer, err := s.db.Get(deadKey(id))
	if errors.Is(err, dbpebble.ErrNotFound) {
		return outbox.Record{}, false, nil
	}
	if err != nil {
		return outbox.Record{}, false, fmt.Errorf("pebble state: fetch dead letter: %w", err)
	}
	defer closeResource(closer, "dead-letter value", &err)
	if err := json.Unmarshal(raw, &record); err != nil {
		return outbox.Record{}, false, fmt.Errorf("pebble state: decode dead letter: %w", err)
	}
	return record, true, nil
}

func countPrefix(ctx context.Context, reader interface {
	NewIter(*dbpebble.IterOptions) (*dbpebble.Iterator, error)
}, prefix []byte, name string) (count int, err error) {
	iter, err := reader.NewIter(prefixOptions(prefix))
	if err != nil {
		return 0, fmt.Errorf("pebble state: count %s: %w", name, err)
	}
	defer closeResource(iter, name+" iterator", &err)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		count++
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("pebble state: count %s: %w", name, err)
	}
	return count, nil
}

// Close flushes durable state before releasing the Pebble database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.Flush(); err != nil {
		return fmt.Errorf("pebble state: flush: %w", err)
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("pebble state: close: %w", err)
	}
	return nil
}

type tx struct {
	batch  *dbpebble.Batch
	maxDLQ int
}

func (t *tx) LoadFlow(ctx context.Context, key flow.Key) (*flow.Flow, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	raw, closer, err := t.batch.Get(flowKey(key))
	if errors.Is(err, dbpebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("pebble state: load flow: %w", err)
	}
	owned := append([]byte(nil), raw...)
	if err := closer.Close(); err != nil {
		return nil, false, fmt.Errorf("pebble state: close flow value: %w", err)
	}
	f, err := flow.Decode(owned)
	if err != nil {
		return nil, false, fmt.Errorf("pebble state: load flow %q/%q: %w", key.RuleID, key.CorrelationKey, err)
	}
	return f, true, nil
}

func (t *tx) SaveFlow(ctx context.Context, f *flow.Flow) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if previous, found, err := t.LoadFlow(ctx, f.Key()); err != nil {
		return err
	} else if found {
		if err := t.batch.Delete(expiryKey(previous.ExpiresAt(), previous.Key()), nil); err != nil {
			return fmt.Errorf("pebble state: delete prior expiry: %w", err)
		}
	}
	raw, err := f.Encode()
	if err != nil {
		return err
	}
	if err := t.batch.Set(flowKey(f.Key()), raw, nil); err != nil {
		return fmt.Errorf("pebble state: save flow: %w", err)
	}
	if err := t.batch.Set(expiryKey(f.ExpiresAt(), f.Key()), nil, nil); err != nil {
		return fmt.Errorf("pebble state: save expiry: %w", err)
	}
	return nil
}

func (t *tx) DeleteFlow(ctx context.Context, key flow.Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	previous, found, err := t.LoadFlow(ctx, key)
	if err != nil {
		return err
	}
	if found {
		if err := t.batch.Delete(expiryKey(previous.ExpiresAt(), key), nil); err != nil {
			return fmt.Errorf("pebble state: delete expiry: %w", err)
		}
	}
	if err := t.batch.Delete(flowKey(key), nil); err != nil {
		return fmt.Errorf("pebble state: delete flow: %w", err)
	}
	return nil
}

func (t *tx) DueFlows(ctx context.Context, before time.Time, limit int) (due []flow.Key, err error) {
	upper := append(expiryTimePrefix(before), 0xff)
	iter, err := t.batch.NewIter(&dbpebble.IterOptions{LowerBound: expiryPrefix, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("pebble state: scan expiry: %w", err)
	}
	defer closeResource(iter, "expiry iterator", &err)
	type candidate struct {
		key      flow.Key
		deadline time.Time
	}
	var candidates []candidate
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key, err := parseExpiryKey(iter.Key())
		if err != nil {
			return nil, err
		}
		current, found, err := t.LoadFlow(ctx, key)
		if err != nil {
			return nil, err
		}
		if !found {
			candidates = append(candidates, candidate{key: key, deadline: expiryDeadline(iter.Key())})
		} else if !current.ExpiresAt().After(before) {
			// Do not deduplicate index entries here. A stale deadline must be
			// observable through the port so the conformance suite catches it.
			candidates = append(candidates, candidate{key: key, deadline: current.ExpiresAt()})
		}
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("pebble state: scan expiry: %w", err)
	}
	ordered := candidates
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].deadline.Equal(ordered[j].deadline) {
			if ordered[i].key.RuleID == ordered[j].key.RuleID {
				return ordered[i].key.CorrelationKey < ordered[j].key.CorrelationKey
			}
			return ordered[i].key.RuleID < ordered[j].key.RuleID
		}
		return ordered[i].deadline.Before(ordered[j].deadline)
	})
	if limit > 0 && len(ordered) > limit {
		ordered = ordered[:limit]
	}
	due = make([]flow.Key, len(ordered))
	for i := range ordered {
		due[i] = ordered[i].key
	}
	return due, nil
}

func (t *tx) EnqueueOutbox(ctx context.Context, record outbox.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("pebble state: encode outbox %q: %w", record.OutputID, err)
	}
	if err := t.batch.Set(outboxKey(record.OutputID), raw, nil); err != nil {
		return fmt.Errorf("pebble state: enqueue outbox: %w", err)
	}
	return nil
}

func (t *tx) PendingOutbox(ctx context.Context, now time.Time, limit int) (pending []outbox.Record, err error) {
	iter, err := t.batch.NewIter(prefixOptions(outboxPrefix))
	if err != nil {
		return nil, fmt.Errorf("pebble state: scan outbox: %w", err)
	}
	defer closeResource(iter, "pending-outbox iterator", &err)
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var record outbox.Record
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			return nil, fmt.Errorf("pebble state: decode outbox %q: %w", iter.Key(), err)
		}
		if record.Ready(now) {
			pending = append(pending, record)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("pebble state: scan outbox: %w", err)
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

func (t *tx) ResolveOutbox(ctx context.Context, results []outbox.Result) (application.DeadLetterChange, error) {
	deadChanged := false
	change := application.DeadLetterChange{}
	for _, result := range results {
		if err := ctx.Err(); err != nil {
			return application.DeadLetterChange{}, err
		}
		raw, closer, err := t.batch.Get(outboxKey(result.OutputID))
		if errors.Is(err, dbpebble.ErrNotFound) {
			continue
		}
		if err != nil {
			return application.DeadLetterChange{}, fmt.Errorf("pebble state: resolve outbox %q: %w", result.OutputID, err)
		}
		owned := append([]byte(nil), raw...)
		if err := closer.Close(); err != nil {
			return application.DeadLetterChange{}, err
		}
		var record outbox.Record
		if err := json.Unmarshal(owned, &record); err != nil {
			return application.DeadLetterChange{}, fmt.Errorf("pebble state: decode outbox %q: %w", result.OutputID, err)
		}
		if result.Err != nil {
			record.LastError = result.Err.Error()
		}
		switch result.Disposition {
		case outbox.Delivered:
			if err := t.batch.Delete(outboxKey(result.OutputID), nil); err != nil {
				return application.DeadLetterChange{}, err
			}
		case outbox.Permanent:
			deadChanged = true
			if result.Attempts > 0 {
				record.Attempts = result.Attempts
			}
			record.RejectionType = result.RejectionType
			record.DeadLetteredAt = result.DeadLetteredAt
			if previous, found, err := t.deadLetter(result.OutputID); err != nil {
				return application.DeadLetterChange{}, err
			} else if found {
				change.Removed = append(change.Removed, deadLetterRef(previous))
			}
			dead, err := json.Marshal(record)
			if err != nil {
				return application.DeadLetterChange{}, err
			}
			if err := t.batch.Set(deadKey(result.OutputID), dead, nil); err != nil {
				return application.DeadLetterChange{}, err
			}
			change.Added = append(change.Added, deadLetterRef(record))
			if err := t.batch.Delete(outboxKey(result.OutputID), nil); err != nil {
				return application.DeadLetterChange{}, err
			}
		case outbox.Retryable:
			if result.Attempts > 0 {
				record.Attempts = result.Attempts
			} else {
				record.Attempts++
			}
			record.NextAttemptAt = result.NextAttemptAt
			updated, err := json.Marshal(record)
			if err != nil {
				return application.DeadLetterChange{}, err
			}
			if err := t.batch.Set(outboxKey(result.OutputID), updated, nil); err != nil {
				return application.DeadLetterChange{}, err
			}
		default:
			return application.DeadLetterChange{}, fmt.Errorf("pebble state: unknown outbox disposition %q", result.Disposition)
		}
	}
	if !deadChanged {
		return application.DeadLetterChange{}, nil
	}
	dropped, records, err := t.trimDeadLetters(ctx)
	if err != nil {
		return application.DeadLetterChange{}, err
	}
	for _, record := range dropped {
		change.Removed = append(change.Removed, deadLetterRef(record))
	}
	change.Records = records
	change.Dropped = len(dropped)
	change.Changed = true
	return change, nil
}

func (t *tx) ReplayDeadLetters(ctx context.Context, filter outbox.DeadLetterFilter, now time.Time) ([]outbox.DeadLetterMetadata, application.DeadLetterChange, error) {
	iter, err := t.batch.NewIter(prefixOptions(deadPrefix))
	if err != nil {
		return nil, application.DeadLetterChange{}, fmt.Errorf("pebble state: scan dead letters for replay: %w", err)
	}
	type candidate struct {
		key    []byte
		record outbox.Record
	}
	var candidates []candidate
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			_ = iter.Close()
			return nil, application.DeadLetterChange{}, err
		}
		var record outbox.Record
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			_ = iter.Close()
			return nil, application.DeadLetterChange{}, fmt.Errorf("pebble state: decode dead letter %q: %w", iter.Key(), err)
		}
		if filter.Matches(record) {
			candidates = append(candidates, candidate{key: append([]byte(nil), iter.Key()...), record: record})
			if filter.Limit > 0 && len(candidates) == filter.Limit {
				break
			}
		}
	}
	if err := iter.Error(); err != nil {
		_ = iter.Close()
		return nil, application.DeadLetterChange{}, fmt.Errorf("pebble state: scan dead letters for replay: %w", err)
	}
	if err := iter.Close(); err != nil {
		return nil, application.DeadLetterChange{}, err
	}
	metadata := make([]outbox.DeadLetterMetadata, 0, len(candidates))
	change := application.DeadLetterChange{}
	for _, candidate := range candidates {
		record := candidate.record
		metadata = append(metadata, record.Metadata())
		change.Removed = append(change.Removed, deadLetterRef(record))
		record.Attempts = 0
		record.LastError = ""
		record.RejectionType = ""
		record.DeadLetteredAt = time.Time{}
		record.NextAttemptAt = now
		record.ReplayCount++
		raw, err := json.Marshal(record)
		if err != nil {
			return nil, application.DeadLetterChange{}, err
		}
		if err := t.batch.Set(outboxKey(record.OutputID), raw, nil); err != nil {
			return nil, application.DeadLetterChange{}, err
		}
		if err := t.batch.Delete(candidate.key, nil); err != nil {
			return nil, application.DeadLetterChange{}, err
		}
	}
	if len(metadata) > 0 {
		change.Changed = true
		change.Records, err = countPrefix(ctx, t.batch, deadPrefix, "dead letters after replay")
		if err != nil {
			return nil, application.DeadLetterChange{}, err
		}
	}
	return metadata, change, nil
}

func (t *tx) deadLetter(id projection.OutputID) (outbox.Record, bool, error) {
	raw, closer, err := t.batch.Get(deadKey(id))
	if errors.Is(err, dbpebble.ErrNotFound) {
		return outbox.Record{}, false, nil
	}
	if err != nil {
		return outbox.Record{}, false, err
	}
	owned := append([]byte(nil), raw...)
	if err := closer.Close(); err != nil {
		return outbox.Record{}, false, err
	}
	var record outbox.Record
	if err := json.Unmarshal(owned, &record); err != nil {
		return outbox.Record{}, false, err
	}
	return record, true, nil
}

func (t *tx) trimDeadLetters(ctx context.Context) (dropped []outbox.Record, retainedCount int, err error) {
	type retained struct {
		key       []byte
		createdAt time.Time
		outputID  projection.OutputID
		record    outbox.Record
	}
	iter, err := t.batch.NewIter(prefixOptions(deadPrefix))
	if err != nil {
		return nil, 0, fmt.Errorf("pebble state: scan dead letters: %w", err)
	}
	defer closeResource(iter, "dead-letter trim iterator", &err)
	var records []retained
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		var record outbox.Record
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			return nil, 0, fmt.Errorf("pebble state: decode dead letter %q: %w", iter.Key(), err)
		}
		records = append(records, retained{key: append([]byte(nil), iter.Key()...), createdAt: record.CreatedAt, outputID: record.OutputID, record: record})
	}
	if err := iter.Error(); err != nil {
		return nil, 0, fmt.Errorf("pebble state: scan dead letters: %w", err)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].createdAt.Equal(records[j].createdAt) {
			return records[i].outputID < records[j].outputID
		}
		return records[i].createdAt.Before(records[j].createdAt)
	})
	if t.maxDLQ >= 0 && len(records) > t.maxDLQ {
		dropCount := len(records) - t.maxDLQ
		for _, record := range records[:dropCount] {
			if err := t.batch.Delete(record.key, nil); err != nil {
				return nil, 0, err
			}
			dropped = append(dropped, record.record)
		}
	}
	if len(dropped) > 0 {
		current, err := t.droppedTotal()
		if err != nil {
			return nil, 0, err
		}
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], current+uint64(len(dropped)))
		if err := t.batch.Set(dlqDroppedKey, encoded[:], nil); err != nil {
			return nil, 0, err
		}
	}
	return dropped, len(records) - len(dropped), nil
}

func (t *tx) droppedTotal() (total uint64, err error) {
	raw, closer, err := t.batch.Get(dlqDroppedKey)
	if errors.Is(err, dbpebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closeResource(closer, "dead-letter drop counter", &err)
	if len(raw) != 8 {
		return 0, errors.New("pebble state: invalid dead-letter drop count")
	}
	return binary.BigEndian.Uint64(raw), nil
}

func closeResource(closer interface{ Close() error }, name string, target *error) {
	if err := closer.Close(); err != nil {
		*target = errors.Join(*target, fmt.Errorf("pebble state: close %s: %w", name, err))
	}
}

func deadLetterRef(record outbox.Record) outbox.DeadLetterRef {
	return outbox.DeadLetterRef{Type: record.RejectionType, Index: record.Index}
}

func flowKey(key flow.Key) []byte {
	return []byte("f/" + url.PathEscape(string(key.RuleID)) + "/" + url.PathEscape(key.CorrelationKey))
}

func parseFlowKey(raw []byte) (flow.Key, error) {
	if !bytes.HasPrefix(raw, flowPrefix) {
		return flow.Key{}, fmt.Errorf("pebble state: malformed flow key %x", raw)
	}
	parts := bytes.SplitN(raw[len(flowPrefix):], []byte{'/'}, 2)
	if len(parts) != 2 {
		return flow.Key{}, fmt.Errorf("pebble state: malformed flow key %x", raw)
	}
	rulePart, err := url.PathUnescape(string(parts[0]))
	if err != nil {
		return flow.Key{}, fmt.Errorf("pebble state: malformed flow rule in key %x: %w", raw, err)
	}
	correlationPart, err := url.PathUnescape(string(parts[1]))
	if err != nil {
		return flow.Key{}, fmt.Errorf("pebble state: malformed flow correlation in key %x: %w", raw, err)
	}
	return flow.Key{RuleID: rule.ID(rulePart), CorrelationKey: correlationPart}, nil
}
func outboxKey(id projection.OutputID) []byte {
	return append(append([]byte(nil), outboxPrefix...), []byte(id)...)
}
func deadKey(id projection.OutputID) []byte {
	return append(append([]byte(nil), deadPrefix...), []byte(id)...)
}

func expiryTimePrefix(at time.Time) []byte {
	key := make([]byte, 10)
	copy(key, expiryPrefix)
	// Flipping the sign bit keeps byte order chronological across the epoch.
	binary.BigEndian.PutUint64(key[2:], uint64(at.UnixMilli())^(uint64(1)<<63))
	return key
}

func expiryKey(at time.Time, key flow.Key) []byte {
	encoded := expiryTimePrefix(at)
	encoded = append(encoded, '/')
	encoded = append(encoded, url.PathEscape(string(key.RuleID))...)
	encoded = append(encoded, '/')
	encoded = append(encoded, url.PathEscape(key.CorrelationKey)...)
	return encoded
}

func parseExpiryKey(raw []byte) (flow.Key, error) {
	if len(raw) < 13 || !bytes.HasPrefix(raw, expiryPrefix) || raw[10] != '/' {
		return flow.Key{}, fmt.Errorf("pebble state: malformed expiry key %x", raw)
	}
	parts := bytes.SplitN(raw[11:], []byte{'/'}, 2)
	if len(parts) != 2 {
		return flow.Key{}, fmt.Errorf("pebble state: malformed expiry key %x", raw)
	}
	rulePart, err := url.PathUnescape(string(parts[0]))
	if err != nil {
		return flow.Key{}, fmt.Errorf("pebble state: malformed expiry rule in key %x: %w", raw, err)
	}
	correlationPart, err := url.PathUnescape(string(parts[1]))
	if err != nil {
		return flow.Key{}, fmt.Errorf("pebble state: malformed expiry correlation in key %x: %w", raw, err)
	}
	return flow.Key{RuleID: rule.ID(rulePart), CorrelationKey: correlationPart}, nil
}

func expiryDeadline(raw []byte) time.Time {
	encoded := binary.BigEndian.Uint64(raw[2:10]) ^ (uint64(1) << 63)
	return time.UnixMilli(int64(encoded)).UTC()
}

func prefixOptions(prefix []byte) *dbpebble.IterOptions {
	upper := append(append([]byte(nil), prefix...), 0xff)
	return &dbpebble.IterOptions{LowerBound: prefix, UpperBound: upper}
}

var _ application.StateStore = (*Store)(nil)
var _ application.Tx = (*tx)(nil)
