package flow_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/path"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

func TestCodecRoundTripPreservesBehaviourAndProjection(t *testing.T) {
	now := time.Date(2026, 8, 21, 10, 0, 0, 123000000, time.UTC)
	r := codecRule(t)
	f := flow.Open(flow.Key{RuleID: r.ID, CorrelationKey: "codec"}, r, codecEvent(now, "request", "one", "a"))
	f.Apply(codecEvent(now, "request", "one", "a"), r, now)
	f.Apply(codecEvent(now.Add(time.Millisecond), "request", "one", "a"), r, now.Add(time.Millisecond)) // duplicate
	f.Apply(codecEvent(now.Add(2*time.Millisecond), "request", "one", "different"), r, now.Add(2*time.Millisecond))
	f.Apply(codecEvent(now.Add(3*time.Millisecond), "response", "one", "b"), r, now.Add(3*time.Millisecond))
	if reason, close := f.ShouldClose(r, now.Add(3*time.Millisecond)); close || reason != "" {
		t.Fatalf("ShouldClose before settle = (%q, %v), want wait", reason, close)
	}
	f.Apply(codecEvent(now.Add(4*time.Millisecond), "request", "two", "c"), r, now.Add(4*time.Millisecond))

	raw, err := f.Encode()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := flow.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if pinned := decoded.PinnedRule(); pinned.Version != r.Version || pinned.Output != r.Output || pinned.Extract.EventType.Canonical() != r.Extract.EventType.Canonical() {
		t.Fatalf("decoded pinned rule = %+v, want version/output/extraction from %+v", pinned, r)
	}
	checkAt := now.Add(5 * time.Millisecond)
	gotReason, gotClose := decoded.ShouldClose(r, checkAt)
	wantReason, wantClose := f.ShouldClose(r, checkAt)
	if gotReason != wantReason || gotClose != wantClose {
		t.Fatalf("decoded ShouldClose = (%q, %v), want (%q, %v)", gotReason, gotClose, wantReason, wantClose)
	}
	finalizedAt := now.Add(time.Minute)
	got, err := projection.Project(decoded.Finalize(flow.ReasonTimeout, finalizedAt), r)
	if err != nil {
		t.Fatal(err)
	}
	want, err := projection.Project(f.Finalize(flow.ReasonTimeout, finalizedAt), r)
	if err != nil {
		t.Fatal(err)
	}
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if !reflect.DeepEqual(gotJSON, wantJSON) {
		t.Fatalf("decoded projection = %s, want %s", gotJSON, wantJSON)
	}
}

func TestDecodeRejectsUnknownFormatVersion(t *testing.T) {
	_, err := flow.Decode([]byte{99, '{', '}'})
	if err == nil || !strings.Contains(err.Error(), "unknown format version 99") {
		t.Fatalf("Decode() = %v, want error naming version 99", err)
	}
}

func TestCodecPreservesLatchedCloseFlags(t *testing.T) {
	now := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	eventType, _ := path.Compile("$.event")
	for _, test := range []struct {
		name  string
		rule  rule.Rule
		event event.Event
		want  flow.Reason
	}{
		{"terminal", rule.Rule{ID: "r", Extract: rule.Extract{EventType: eventType}, Lifecycle: rule.Lifecycle{Timeout: time.Minute, TerminalEvents: []string{"done"}}}, event.Event{Doc: map[string]any{"event": "done"}, ObservedAt: now}, flow.ReasonTerminalEvent},
		{"limit", rule.Rule{ID: "r", Lifecycle: rule.Lifecycle{Timeout: time.Minute}, Limits: rule.Limits{MaxEventBytes: 1}}, event.Event{Doc: map[string]any{"payload": "too large"}, ObservedAt: now}, flow.ReasonLimitExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := flow.Open(flow.Key{RuleID: "r", CorrelationKey: test.name}, test.rule, test.event)
			f.Apply(test.event, test.rule, now)
			raw, err := f.Encode()
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := flow.Decode(raw)
			if err != nil {
				t.Fatal(err)
			}
			withoutTriggers := test.rule
			withoutTriggers.Lifecycle.TerminalEvents = nil
			withoutTriggers.Limits = rule.Limits{}
			got, close := decoded.ShouldClose(withoutTriggers, now)
			if !close || got != test.want {
				t.Fatalf("decoded ShouldClose() = (%q, %v), want (%q, true)", got, close, test.want)
			}
		})
	}
}

func codecRule(t *testing.T) rule.Rule {
	t.Helper()
	eventType, err := path.Compile("$.event")
	if err != nil {
		t.Fatal(err)
	}
	timestamp, err := path.Compile("$.timestamp")
	if err != nil {
		t.Fatal(err)
	}
	group, err := path.Compile("$.id")
	if err != nil {
		t.Fatal(err)
	}
	return rule.Rule{ID: "codec-rule", Version: "7", Extract: rule.Extract{EventType: eventType, Timestamp: timestamp},
		Stitch:    []rule.Stitch{{ID: "call", GroupBy: []path.Path{group}, Roles: []rule.Role{{Name: "request", Types: []string{"request"}}, {Name: "response", Types: []string{"response"}}}, Requires: []string{"request", "response"}}},
		Lifecycle: rule.Lifecycle{Timeout: time.Minute, CloseWhen: rule.CloseAllInvocationsComplete, Settle: 10 * time.Second},
		Limits:    rule.Limits{MaxEvents: 20, MaxEventBytes: 1024, MaxFlowBytes: 4096}, Output: rule.Output{Index: "flows", TimestampSource: rule.TimestampFirstEvent}}
}

func codecEvent(observed time.Time, typ, id, value string) event.Event {
	return event.Event{Doc: map[string]any{"event": typ, "id": id, "value": value, "timestamp": observed.Format(time.RFC3339Nano)}, ObservedAt: observed}
}
