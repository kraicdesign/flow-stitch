// Package event models an accepted producer document without reshaping it (ADR-0004).
package event

import (
	"fmt"
	"time"
)

// Event is the raw producer document plus the time FlowStitch observed it.
type Event struct {
	Doc        map[string]any
	ObservedAt time.Time
	// Raw is the producer document as received by the ingress. It is needed only
	// for pass-through and must never be persisted with correlated flow state.
	Raw []byte `json:"-"`
}

// String deliberately excludes producer data because internal logs must never carry it.
func (e Event) String() string {
	return fmt.Sprintf("event(observed_at=%s)", e.ObservedAt.Format(time.RFC3339Nano))
}
