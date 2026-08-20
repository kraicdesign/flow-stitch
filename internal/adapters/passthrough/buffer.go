// Package passthrough implements the bounded in-memory queue for unmatched events.
package passthrough

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/indexname"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/path"
)

// Options configures a fixed-capacity buffer.
type Options struct {
	Index         string
	Timestamp     path.Path
	BufferSize    int
	BatchSize     int
	FlushInterval time.Duration
	Clock         application.Clock
	Recorder      application.Recorder
}

// Buffer never writes to the state store (ADR-0010, section 2).
type Buffer struct {
	mu      sync.Mutex
	opts    Options
	records []application.PassthroughRecord
	next    uint64
	ready   chan struct{}
	enabled bool
}

// New creates an enabled buffer with the configured fixed capacity.
func New(opts Options) *Buffer {
	return &Buffer{opts: opts, records: make([]application.PassthroughRecord, 0, opts.BufferSize), ready: make(chan struct{}, 1), enabled: true}
}

// Accept appends one unmatched event or reports that buffering is unavailable.
func (b *Buffer) Accept(e event.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.enabled {
		return application.ErrPassthroughDisabled
	}
	raw := e.Raw
	if len(raw) == 0 {
		var err error
		raw, err = json.Marshal(e.Doc)
		if err != nil {
			return err
		}
	}
	document := append([]byte(nil), raw...)
	at := b.opts.Clock.Now()
	if rawTimestamp, ok := b.opts.Timestamp.Resolve(e.Doc); ok {
		if parsed, err := time.Parse(time.RFC3339Nano, rawTimestamp); err == nil {
			at = parsed
		}
	}

	if len(b.records) >= b.opts.BufferSize {
		return application.ErrPassthroughFull
	}
	b.next++
	b.records = append(b.records, application.PassthroughRecord{Sequence: b.next, Index: indexname.Resolve(b.opts.Index, at, at), Document: document})
	depth := len(b.records)
	b.opts.Recorder.PassthroughBuffer(depth)
	b.opts.Recorder.PassthroughEvent()
	if depth >= b.opts.BatchSize {
		select {
		case b.ready <- struct{}{}:
		default:
		}
	}
	return nil
}

// Reconfigure atomically applies reloadable pass-through settings while
// preserving records already waiting for delivery (ADR-0011).
func (b *Buffer) Reconfigure(opts Options, enabled bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.opts = opts
	b.enabled = enabled
}

// Pending returns a copy of the next delivery batch without removing it.
func (b *Buffer) Pending() []application.PassthroughRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	count := min(len(b.records), b.opts.BatchSize)
	return append([]application.PassthroughRecord(nil), b.records[:count]...)
}

// Resolve removes delivered or permanently rejected records from the buffer.
func (b *Buffer) Resolve(results []application.PassthroughResult) {
	resolved := make(map[uint64]outbox.Disposition, len(results))
	for _, result := range results {
		resolved[result.Sequence] = result.Disposition
	}
	b.mu.Lock()
	kept := make([]application.PassthroughRecord, 0, cap(b.records))
	dropped := 0
	for _, record := range b.records {
		switch resolved[record.Sequence] {
		case outbox.Delivered:
		case outbox.Permanent:
			dropped++
		default:
			kept = append(kept, record)
		}
	}
	b.records = kept
	depth := len(b.records)
	b.opts.Recorder.PassthroughBuffer(depth)
	b.mu.Unlock()
	for range dropped {
		b.opts.Recorder.PassthroughDropped()
	}
}

// Depth returns the number of records currently buffered.
func (b *Buffer) Depth() int { b.mu.Lock(); defer b.mu.Unlock(); return len(b.records) }

// Ready signals that the configured delivery batch size has been reached.
func (b *Buffer) Ready() <-chan struct{} { return b.ready }

// FlushInterval returns the currently configured maximum buffering delay.
func (b *Buffer) FlushInterval() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.opts.FlushInterval
}

var _ application.PassthroughBuffer = (*Buffer)(nil)
