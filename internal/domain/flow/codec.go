package flow

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

const formatVersion byte = 2

type encodedFlow struct {
	Key             Key                 `json:"key"`
	RuleVersion     rule.Version        `json:"rule_version"`
	PinnedRule      rule.Rule           `json:"pinned_rule"`
	Events          []event.Event       `json:"events"`
	Invocations     []encodedInvocation `json:"invocations"`
	Plain           []encodedPlainEntry `json:"plain"`
	FirstObservedAt time.Time           `json:"first_observed_at"`
	UpdatedAt       time.Time           `json:"updated_at"`
	TimeoutAt       time.Time           `json:"timeout_at"`
	ExpiresAt       time.Time           `json:"expires_at"`
	SettleUntil     time.Time           `json:"settle_until"`
	DuplicateCount  int                 `json:"duplicate_count"`
	Duplicates      map[string]int      `json:"duplicates"`
	Anomalies       []Anomaly           `json:"anomalies"`
	TerminalSeen    bool                `json:"terminal_seen"`
	LimitExceeded   bool                `json:"limit_exceeded"`
	Bytes           int64               `json:"bytes"`
	NextOrder       int                 `json:"next_order"`
}

type encodedInvocation struct {
	Identity        string                 `json:"identity"`
	StitchID        string                 `json:"stitch_id"`
	Group           map[string]string      `json:"group"`
	Members         map[string]event.Event `json:"members"`
	Complete        bool                   `json:"complete"`
	DuplicateCount  int                    `json:"duplicate_count"`
	FirstObservedAt time.Time              `json:"first_observed_at"`
	Order           int                    `json:"order"`
}

type encodedPlainEntry struct {
	Event event.Event `json:"event"`
	Order int         `json:"order"`
}

// Encode returns the versioned durable representation of the aggregate (ADR-0006).
func (f *Flow) Encode() ([]byte, error) {
	body := encodedFlow{
		Key: f.key, RuleVersion: f.ruleVersion, PinnedRule: f.pinnedRule, Events: f.events,
		FirstObservedAt: f.firstObservedAt, UpdatedAt: f.updatedAt,
		TimeoutAt: f.timeoutAt, ExpiresAt: f.expiresAt, SettleUntil: f.settleUntil,
		DuplicateCount: f.duplicateCount, Duplicates: f.duplicates, Anomalies: f.anomalies,
		TerminalSeen: f.terminalSeen, LimitExceeded: f.limitExceeded, Bytes: f.bytes, NextOrder: f.nextOrder,
	}
	identities := make(map[*Invocation]string, len(f.invocationByKey))
	for identity, invocation := range f.invocationByKey {
		identities[invocation] = identity
	}
	for _, invocation := range f.invocations {
		body.Invocations = append(body.Invocations, encodedInvocation{
			Identity: identities[invocation], StitchID: invocation.StitchID,
			Group: invocation.Group, Members: invocation.Members, Complete: invocation.Complete,
			DuplicateCount: invocation.DuplicateCount, FirstObservedAt: invocation.firstObservedAt, Order: invocation.order,
		})
	}
	for _, plain := range f.plain {
		body.Plain = append(body.Plain, encodedPlainEntry{Event: plain.Event, Order: plain.order})
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("flow: encode version %d: %w", formatVersion, err)
	}
	return append([]byte{formatVersion}, raw...), nil
}

// Decode restores a flow from its durable representation. Unknown versions fail
// closed so a rolled-back binary cannot silently discard newer state (ADR-0006).
func Decode(raw []byte) (*Flow, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("flow: decode: missing format version")
	}
	if raw[0] != formatVersion {
		return nil, fmt.Errorf("flow: decode: unknown format version %d", raw[0])
	}
	var body encodedFlow
	if err := json.Unmarshal(raw[1:], &body); err != nil {
		return nil, fmt.Errorf("flow: decode version %d: %w", raw[0], err)
	}
	f := &Flow{
		key: body.Key, ruleVersion: body.RuleVersion, pinnedRule: body.PinnedRule, events: body.Events,
		invocationByKey: make(map[string]*Invocation, len(body.Invocations)),
		firstObservedAt: body.FirstObservedAt, updatedAt: body.UpdatedAt,
		timeoutAt: body.TimeoutAt, expiresAt: body.ExpiresAt, settleUntil: body.SettleUntil,
		duplicateCount: body.DuplicateCount, duplicates: body.Duplicates, anomalies: body.Anomalies,
		terminalSeen: body.TerminalSeen, limitExceeded: body.LimitExceeded, bytes: body.Bytes, nextOrder: body.NextOrder,
	}
	if f.duplicates == nil {
		f.duplicates = make(map[string]int)
	}
	for _, stored := range body.Invocations {
		invocation := &Invocation{
			StitchID: stored.StitchID, Group: stored.Group, Members: stored.Members,
			Complete: stored.Complete, DuplicateCount: stored.DuplicateCount,
			firstObservedAt: stored.FirstObservedAt, order: stored.Order,
		}
		if invocation.Group == nil {
			invocation.Group = make(map[string]string)
		}
		if invocation.Members == nil {
			invocation.Members = make(map[string]event.Event)
		}
		f.invocations = append(f.invocations, invocation)
		f.invocationByKey[stored.Identity] = invocation
	}
	for _, stored := range body.Plain {
		f.plain = append(f.plain, PlainEntry{Event: stored.Event, order: stored.Order})
	}
	return f, nil
}
