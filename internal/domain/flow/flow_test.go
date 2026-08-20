package flow_test

import (
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/path"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

func TestOpenAnchorsDeadlineToFirstObservedAt(t *testing.T) {
	first := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	r := rule.Rule{ID: "r", Version: "1", Lifecycle: rule.Lifecycle{Timeout: 10 * time.Second}}
	f := flow.Open(flow.Key{RuleID: r.ID, CorrelationKey: "abc"}, r, event.Event{Doc: map[string]any{"id": 1}, ObservedAt: first})
	f.Apply(event.Event{Doc: map[string]any{"id": 2}, ObservedAt: first.Add(9 * time.Second)}, r, first.Add(9*time.Second))
	if got, want := f.ExpiresAt(), first.Add(10*time.Second); !got.Equal(want) {
		t.Fatalf("ExpiresAt() = %v, want %v", got, want)
	}
}

func TestSettleShortensDeadlineAndRechecksCompletion(t *testing.T) {
	eventType, _ := path.Compile("$.event")
	groupBy, _ := path.Compile("$.id")
	first := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	r := rule.Rule{ID: "r", Extract: rule.Extract{EventType: eventType}, Stitch: []rule.Stitch{{ID: "call", GroupBy: []path.Path{groupBy}, Roles: []rule.Role{{Name: "request", Types: []string{"request"}}}, Requires: []string{"request"}}}, Lifecycle: rule.Lifecycle{Timeout: 10 * time.Second, CloseWhen: rule.CloseAllInvocationsComplete, Settle: time.Second}}
	e := event.Event{Doc: map[string]any{"event": "request", "id": "one"}, ObservedAt: first}
	f := flow.Open(flow.Key{RuleID: r.ID, CorrelationKey: "abc"}, r, e)
	f.Apply(e, r, first)
	if reason, close := f.ShouldClose(r, first); close || reason != "" {
		t.Fatalf("ShouldClose() = (%q, %v), want settle wait", reason, close)
	}
	if got, want := f.ExpiresAt(), first.Add(time.Second); !got.Equal(want) {
		t.Fatalf("ExpiresAt() = %v, want %v", got, want)
	}
	if reason, close := f.ShouldClose(r, first.Add(time.Second)); !close || reason != flow.ReasonInvocationsComplete {
		t.Fatalf("ShouldClose() = (%q, %v), want invocations_complete", reason, close)
	}
}

func TestFinalizeUsesNewReasons(t *testing.T) {
	now := time.Now()
	r := rule.Rule{ID: "r", Lifecycle: rule.Lifecycle{Timeout: time.Second}}
	f := flow.Open(flow.Key{RuleID: r.ID, CorrelationKey: "abc"}, r, event.Event{ObservedAt: now})
	if got := f.Finalize(flow.ReasonInvocationsComplete, now).Reason; got != flow.ReasonInvocationsComplete {
		t.Fatalf("Reason = %q, want %q", got, flow.ReasonInvocationsComplete)
	}
}
