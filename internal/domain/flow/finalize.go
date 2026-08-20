package flow

import (
	"time"

	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

// Reason identifies the condition that made a flow immutable.
type Reason string

const (
	// ReasonInvocationsComplete means every observed invocation has its required roles.
	ReasonInvocationsComplete Reason = "invocations_complete"
	// ReasonTerminalEvent means a configured terminal producer event arrived.
	ReasonTerminalEvent Reason = "terminal_event"
	// ReasonTimeout means the total observed-time budget elapsed.
	ReasonTimeout Reason = "timeout"
	// ReasonLimitExceeded means the flow crossed a configured evidence bound.
	ReasonLimitExceeded Reason = "limit_exceeded"
	// ReasonRuleUnavailable means recovery could not resolve the flow's pinned rule.
	ReasonRuleUnavailable Reason = "rule_unavailable"
)

// Finalized is an immutable aggregate snapshot ready for projection.
type Finalized struct {
	Key                   Key
	RuleID                rule.ID
	RuleVersion           rule.Version
	Reason                Reason
	Events                []event.Event
	Entries               []Entry
	Invocations           []Invocation
	PlainEntries          []PlainEntry
	Anomalies             []Anomaly
	DuplicateCount        int
	Duplicates            map[string]int
	IncompleteInvocations int
	FirstObservedAt       time.Time
	FinalizedAt           time.Time
}

// Finalize snapshots the flow without retaining mutable aggregate collections.
func (f *Flow) Finalize(reason Reason, now time.Time) Finalized {
	invocations := make([]Invocation, 0, len(f.invocations))
	for _, invocation := range f.invocations {
		invocations = append(invocations, cloneInvocation(invocation))
	}
	entries := f.Entries()
	for i := range entries {
		if entries[i].Invocation != nil {
			cloned := cloneInvocation(entries[i].Invocation)
			entries[i].Invocation = &cloned
		}
	}
	return Finalized{
		Key: f.key, RuleID: f.key.RuleID, RuleVersion: f.ruleVersion, Reason: reason,
		Events: f.Events(), Entries: entries, Invocations: invocations, PlainEntries: append([]PlainEntry(nil), f.plain...),
		Anomalies: f.Anomalies(), DuplicateCount: f.duplicateCount, Duplicates: f.Duplicates(),
		IncompleteInvocations: f.IncompleteInvocations(), FirstObservedAt: f.firstObservedAt, FinalizedAt: now,
	}
}

func cloneInvocation(source *Invocation) Invocation {
	group := make(map[string]string, len(source.Group))
	for key, value := range source.Group {
		group[key] = value
	}
	members := make(map[string]event.Event, len(source.Members))
	for roleName, member := range source.Members {
		members[roleName] = member
	}
	cloned := *source
	cloned.Group = group
	cloned.Members = members
	return cloned
}
