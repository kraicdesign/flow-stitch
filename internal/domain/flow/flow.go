// Package flow contains the pure correlation aggregate.
package flow

import (
	"encoding/json"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

// Key identifies an open flow within the rule that claimed it.
type Key struct {
	RuleID         rule.ID
	CorrelationKey string
}

// AnomalyKind classifies evidence that could not be merged normally.
type AnomalyKind string

const (
	// AnomalyCardinality marks a second distinct event for an occupied role.
	AnomalyCardinality AnomalyKind = "cardinality_exceeded"
	// AnomalyClockSkew marks a negative producer-time invocation duration.
	AnomalyClockSkew AnomalyKind = "clock_skew"
	// AnomalyPromotion marks a value that could not satisfy its declared output type.
	AnomalyPromotion AnomalyKind = "promotion_failed"
)

// Anomaly preserves non-fatal correlation or projection irregularities.
type Anomaly struct {
	Kind   AnomalyKind `json:"kind"`
	Detail string      `json:"detail,omitempty"`
	At     time.Time   `json:"at"`
}

// Invocation is one stitch group inside a flow (ADR-0005, section 1).
type Invocation struct {
	StitchID        string
	Group           map[string]string
	Members         map[string]event.Event
	Complete        bool
	DuplicateCount  int
	firstObservedAt time.Time
	order           int
}

// PlainEntry is an event that did not occupy a stitch slot.
type PlainEntry struct {
	Event event.Event
	order int
}

// Entry is one projected timeline position, either stitched or plain.
type Entry struct {
	Invocation *Invocation
	Plain      *event.Event
	observedAt time.Time
	order      int
}

// Flow is the correlation aggregate for one rule and correlation key.
type Flow struct {
	key             Key
	ruleVersion     rule.Version
	pinnedRule      rule.Rule
	events          []event.Event
	invocations     []*Invocation
	invocationByKey map[string]*Invocation
	plain           []PlainEntry
	firstObservedAt time.Time
	updatedAt       time.Time
	timeoutAt       time.Time
	expiresAt       time.Time
	settleUntil     time.Time
	duplicateCount  int
	duplicates      map[string]int
	anomalies       []Anomaly
	terminalSeen    bool
	limitExceeded   bool
	bytes           int64
	nextOrder       int
}

// Open records the batch identity and total timeout budget. The caller applies
// the first event through [Flow.Apply] just like every later event (ADR-0003, section 1).
func Open(key Key, r rule.Rule, first event.Event) *Flow {
	timeoutAt := first.ObservedAt.Add(r.Lifecycle.Timeout)
	return &Flow{
		key:             key,
		ruleVersion:     r.Version,
		pinnedRule:      r,
		invocationByKey: make(map[string]*Invocation),
		firstObservedAt: first.ObservedAt,
		updatedAt:       first.ObservedAt,
		timeoutAt:       timeoutAt,
		expiresAt:       timeoutAt,
		duplicates:      make(map[string]int),
	}
}

// Key returns the aggregate's rule-scoped identity.
func (f *Flow) Key() Key { return f.key }

// RuleVersion returns the immutable rule version retained by this flow.
func (f *Flow) RuleVersion() rule.Version { return f.ruleVersion }

// PinnedRule returns the immutable rule definition this flow opened under.
func (f *Flow) PinnedRule() rule.Rule { return f.pinnedRule }

// ExpiresAt returns the next persisted deadline for timeout or settling.
func (f *Flow) ExpiresAt() time.Time { return f.expiresAt }

// Events returns a shallow copy of the aggregate's preserved evidence.
func (f *Flow) Events() []event.Event { return append([]event.Event(nil), f.events...) }

// Anomalies returns a copy of the irregularities observed by the aggregate.
func (f *Flow) Anomalies() []Anomaly { return append([]Anomaly(nil), f.anomalies...) }

// FirstObservedAt returns the observed time that started the total timeout budget.
func (f *Flow) FirstObservedAt() time.Time { return f.firstObservedAt }

// DuplicateCount returns the number of byte-identical occupied-slot repeats.
func (f *Flow) DuplicateCount() int { return f.duplicateCount }

// Duplicates returns duplicate counts grouped by producer event type.
func (f *Flow) Duplicates() map[string]int {
	out := make(map[string]int, len(f.duplicates))
	for typ, count := range f.duplicates {
		out[typ] = count
	}
	return out
}

// IncompleteInvocations counts invocations still missing a required role.
func (f *Flow) IncompleteInvocations() int {
	count := 0
	for _, invocation := range f.invocations {
		if !invocation.Complete {
			count++
		}
	}
	return count
}

// CompleteInvocations counts invocations whose required roles are present.
func (f *Flow) CompleteInvocations() int {
	count := 0
	for _, invocation := range f.invocations {
		if invocation.Complete {
			count++
		}
	}
	return count
}

// Entries interleaves stitched and plain entries by earliest observed arrival.
func (f *Flow) Entries() []Entry {
	entries := make([]Entry, 0, len(f.invocations)+len(f.plain))
	for _, invocation := range f.invocations {
		entries = append(entries, Entry{Invocation: invocation, observedAt: invocation.firstObservedAt, order: invocation.order})
	}
	for i := range f.plain {
		plain := &f.plain[i]
		entries = append(entries, Entry{Plain: &plain.Event, observedAt: plain.Event.ObservedAt, order: plain.order})
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].observedAt.Equal(entries[j].observedAt) {
			return entries[i].order < entries[j].order
		}
		return entries[i].observedAt.Before(entries[j].observedAt)
	})
	return entries
}

// Outcome reports correlation facts needed by the ingest use case.
type Outcome struct {
	Duplicate bool
}

// Apply assigns an event to a stitch slot or preserves it as a plain entry
// (ADR-0005, sections 1–2).
func (f *Flow) Apply(e event.Event, r rule.Rule, now time.Time) Outcome {
	eventType, hasType := r.Extract.EventType.Resolve(e.Doc)
	if hasType && r.IsTerminal(eventType) {
		f.terminalSeen = true
	}

	stitch, roleName, stitched := matchingRole(r.Stitch, eventType, hasType)
	if !stitched {
		f.appendPlain(e, r)
		f.updatedAt = now
		return Outcome{}
	}

	group := make(map[string]string, len(stitch.GroupBy))
	values := make([]string, 0, len(stitch.GroupBy))
	groupFields := stitch.GroupFields()
	for i, groupPath := range stitch.GroupBy {
		value, ok := groupPath.Resolve(e.Doc)
		if !ok {
			f.appendPlain(e, r)
			f.updatedAt = now
			return Outcome{}
		}
		group[groupFields[i]] = value
		values = append(values, value)
	}

	identity := invocationKey(stitch.ID, values)
	invocation, exists := f.invocationByKey[identity]
	if !exists {
		invocation = &Invocation{StitchID: stitch.ID, Group: group, Members: make(map[string]event.Event), firstObservedAt: e.ObservedAt, order: f.nextOrder}
		f.nextOrder++
		f.invocationByKey[identity] = invocation
		f.invocations = append(f.invocations, invocation)
	}

	if existing, occupied := invocation.Members[roleName]; occupied {
		if reflect.DeepEqual(existing.Doc, e.Doc) {
			f.duplicateCount++
			f.duplicates[eventType]++
			invocation.DuplicateCount++
			f.updatedAt = now
			return Outcome{Duplicate: true}
		}
		f.anomalies = append(f.anomalies, Anomaly{Kind: AnomalyCardinality, Detail: "stitch " + stitch.ID + " role " + roleName + " is already occupied", At: now})
		f.appendPlain(e, r)
		f.updatedAt = now
		return Outcome{}
	}

	invocation.Members[roleName] = e
	f.storeEvent(e, r)
	if !invocation.Complete {
		invocation.Complete = invocationComplete(invocation, requiredRoles(stitch))
	}
	f.updatedAt = now
	return Outcome{}
}

// ShouldClose evaluates closers in their specified priority order (ADR-0005, section 3).
func (f *Flow) ShouldClose(r rule.Rule, now time.Time) (Reason, bool) {
	if f.terminalSeen {
		return ReasonTerminalEvent, true
	}
	if f.limitExceeded || (r.Limits.MaxEvents > 0 && len(f.events) >= r.Limits.MaxEvents) {
		return ReasonLimitExceeded, true
	}

	condition := r.Lifecycle.CloseWhen == rule.CloseAllInvocationsComplete && f.CompleteInvocations() > 0 && f.IncompleteInvocations() == 0
	if !condition {
		if !f.settleUntil.IsZero() {
			f.settleUntil = time.Time{}
			f.expiresAt = f.timeoutAt
		}
		return "", false
	}
	if r.Lifecycle.Settle <= 0 {
		return ReasonInvocationsComplete, true
	}
	if f.settleUntil.IsZero() {
		f.settleUntil = now.Add(r.Lifecycle.Settle)
		f.expiresAt = minTime(f.timeoutAt, f.settleUntil)
		return "", false
	}
	if now.Before(f.settleUntil) {
		return "", false
	}
	return ReasonInvocationsComplete, true
}

func (f *Flow) appendPlain(e event.Event, configured rule.Rule) {
	f.storeEvent(e, configured)
	f.plain = append(f.plain, PlainEntry{Event: e, order: f.nextOrder})
	f.nextOrder++
}

func (f *Flow) storeEvent(e event.Event, configured rule.Rule) {
	f.events = append(f.events, e)
	raw, err := json.Marshal(e.Doc)
	if err != nil {
		return
	}
	size := int64(len(raw))
	f.bytes += size
	if configured.Limits.MaxEventBytes > 0 && size > configured.Limits.MaxEventBytes {
		f.limitExceeded = true
	}
	if configured.Limits.MaxFlowBytes > 0 && f.bytes > configured.Limits.MaxFlowBytes {
		f.limitExceeded = true
	}
}

func matchingRole(stitches []rule.Stitch, eventType string, hasType bool) (rule.Stitch, string, bool) {
	if !hasType {
		return rule.Stitch{}, "", false
	}
	for _, stitch := range stitches {
		for _, role := range stitch.Roles {
			for _, typ := range role.Types {
				if typ == eventType {
					return stitch, role.Name, true
				}
			}
		}
	}
	return rule.Stitch{}, "", false
}

func invocationComplete(invocation *Invocation, required []string) bool {
	for _, role := range required {
		if _, ok := invocation.Members[role]; !ok {
			return false
		}
	}
	return true
}

func requiredRoles(stitch rule.Stitch) []string {
	if len(stitch.Requires) > 0 {
		return stitch.Requires
	}
	required := make([]string, 0, len(stitch.Roles))
	for _, role := range stitch.Roles {
		required = append(required, role.Name)
	}
	return required
}

func invocationKey(stitchID string, values []string) string {
	var key strings.Builder
	key.WriteString(strconv.Itoa(len(stitchID)))
	key.WriteByte(':')
	key.WriteString(stitchID)
	for _, value := range values {
		key.WriteByte(':')
		key.WriteString(strconv.Itoa(len(value)))
		key.WriteByte(':')
		key.WriteString(value)
	}
	return key.String()
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
