package projection_test

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

func TestOutputIDIsDeterministic(t *testing.T) {
	started := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	first := projection.NewOutputID("http-flow", "abc", started)
	second := projection.NewOutputID("http-flow", "abc", started)
	if first != second {
		t.Fatalf("NewOutputID is not deterministic: %q != %q", first, second)
	}
}

func TestProjectUsesCorrelationKeyForFlowIDAndKeepsHashAsOutputID(t *testing.T) {
	started := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	snapshot := flow.Finalized{
		Key: flow.Key{RuleID: "http-flow", CorrelationKey: "abc"}, RuleID: "http-flow",
		FirstObservedAt: started, FinalizedAt: started,
	}
	document, err := projection.Project(snapshot, rule.Rule{ID: "http-flow"})
	if err != nil {
		t.Fatal(err)
	}
	if document.Flow.ID != "abc" {
		t.Fatalf("flow.id = %q, want correlation key %q", document.Flow.ID, "abc")
	}
	outputID := projection.NewOutputID("http-flow", "abc", started)
	if string(outputID) == document.Flow.ID || len(outputID) != 64 {
		t.Fatalf("output ID = %q, want distinct sha256 document ID", outputID)
	}
}

func TestProjectInvocationShapeUsesNamedGroupsAndExplicitAbsenceValues(t *testing.T) {
	eventType, _ := path.Compile("$.event")
	timestamp, _ := path.Compile("$.datetime")
	service, _ := path.Compile("$.service")
	contextID, _ := path.Compile("$.context.id")
	payloadID, _ := path.Compile("$.payload.id")
	configured := rule.Rule{
		ID: "http-flow", Version: "1", Extract: rule.Extract{EventType: eventType, Timestamp: timestamp},
		Stitch: []rule.Stitch{{ID: "call", GroupBy: []path.Path{service, contextID, payloadID},
			Roles: []rule.Role{{Name: "request", Types: []string{"request"}}, {Name: "response", Types: []string{"response"}}}, Requires: []string{"request", "response"}}},
		Lifecycle: rule.Lifecycle{Timeout: time.Minute}, Output: rule.Output{Index: "flows"},
	}
	observed := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	request := event.Event{Doc: map[string]any{"event": "request", "datetime": "2026-08-20T12:00:00Z", "service": "web", "context": map[string]any{"id": "one"}, "payload": map[string]any{"id": "two"}}, ObservedAt: observed}
	current := flow.Open(flow.Key{RuleID: configured.ID, CorrelationKey: "abc"}, configured, request)
	current.Apply(request, configured, observed)
	incomplete, err := projection.Project(current.Finalize(flow.ReasonTimeout, observed.Add(time.Second)), configured)
	if err != nil {
		t.Fatal(err)
	}
	entry := incomplete.Events[0]
	wantGroup := map[string]string{"service": "web", "id": "one", "id2": "two"}
	if !reflect.DeepEqual(entry["group"], wantGroup) {
		t.Fatalf("group = %#v, want %#v", entry["group"], wantGroup)
	}
	if entry["duration_ms"] != int64(-1) {
		t.Fatalf("duration_ms = %#v, want -1", entry["duration_ms"])
	}
	if _, exists := entry["ended_at"]; exists {
		t.Fatalf("incomplete entry contains ended_at: %#v", entry)
	}
	if entry["duplicate_count"] != 0 {
		t.Fatalf("duplicate_count = %#v, want 0", entry["duplicate_count"])
	}

	response := event.Event{Doc: map[string]any{"event": "response", "datetime": "2026-08-20T12:00:00.041Z", "service": "web", "context": map[string]any{"id": "one"}, "payload": map[string]any{"id": "two"}}, ObservedAt: observed.Add(time.Second)}
	current.Apply(response, configured, observed.Add(time.Second))
	complete, err := projection.Project(current.Finalize(flow.ReasonInvocationsComplete, observed.Add(time.Second)), configured)
	if err != nil {
		t.Fatal(err)
	}
	if complete.Events[0]["duration_ms"] != int64(41) {
		t.Fatalf("complete duration_ms = %#v, want 41", complete.Events[0]["duration_ms"])
	}
	if _, exists := complete.Events[0]["ended_at"]; !exists {
		t.Fatalf("complete entry omits ended_at: %#v", complete.Events[0])
	}
}

func TestProjectPromotesTypedFieldsFromConfiguredSources(t *testing.T) {
	mustPath := func(expression string) path.Path {
		compiled, err := path.Compile(expression)
		if err != nil {
			t.Fatal(err)
		}
		return compiled
	}
	observed := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	request := event.Event{ObservedAt: observed, Doc: map[string]any{
		"event": "request", "payload": map[string]any{"selected": "400", "flag": true, "bad_long": "abc", "bad_date": "tomorrow"},
	}}
	response := event.Event{ObservedAt: observed.Add(time.Second), Doc: map[string]any{
		"event": "response", "payload": map[string]any{"selected": "500", "fallback": "222", "date": "2026-08-21T12:00:01Z", "ratio": "1.25", "active": "true"},
	}}
	last := event.Event{ObservedAt: observed.Add(2 * time.Second), Doc: map[string]any{
		"event": "log", "payload": map[string]any{"selected": "700", "fallback": "333"},
	}}
	configured := rule.Rule{
		ID: "promoted", Version: "1",
		Stitch: []rule.Stitch{{ID: "call", Roles: []rule.Role{{Name: "request"}, {Name: "response"}}}},
		Promote: []rule.Promotion{
			{Name: "response_status", Path: mustPath("$.payload.selected"), Type: rule.PromotionLong, From: "response"},
			{Name: "last_status", Path: mustPath("$.payload.selected"), Type: rule.PromotionLong, From: "last"},
			{Name: "first_resolving", Path: mustPath("$.payload.fallback"), Type: rule.PromotionLong},
			{Name: "bool_keyword", Path: mustPath("$.payload.flag"), Type: rule.PromotionKeyword},
			{Name: "happened_at", Path: mustPath("$.payload.date"), Type: rule.PromotionDate, From: "response"},
			{Name: "ratio", Path: mustPath("$.payload.ratio"), Type: rule.PromotionDouble, From: "response"},
			{Name: "active", Path: mustPath("$.payload.active"), Type: rule.PromotionBoolean, From: "response"},
			{Name: "bad_long", Path: mustPath("$.payload.bad_long"), Type: rule.PromotionLong},
			{Name: "bad_date", Path: mustPath("$.payload.bad_date"), Type: rule.PromotionDate},
			{Name: "absent", Path: mustPath("$.payload.missing"), Type: rule.PromotionKeyword},
		},
	}
	snapshot := flow.Finalized{
		Key: flow.Key{RuleID: configured.ID, CorrelationKey: "one"}, RuleID: configured.ID, RuleVersion: "1",
		Events:          []event.Event{request, response, last},
		Invocations:     []flow.Invocation{{StitchID: "call", Members: map[string]event.Event{"request": request, "response": response}}},
		FirstObservedAt: observed, FinalizedAt: observed.Add(3 * time.Second), Reason: flow.ReasonTimeout,
	}

	first, err := projection.Project(snapshot, configured)
	if err != nil {
		t.Fatal(err)
	}
	if got := first.Fields["response_status"]; got != int64(500) {
		t.Fatalf("response_status = %#v, want int64(500)", got)
	}
	if got := first.Fields["last_status"]; got != int64(700) {
		t.Fatalf("last_status = %#v, want int64(700)", got)
	}
	if got := first.Fields["first_resolving"]; got != int64(222) {
		t.Fatalf("first_resolving = %#v, want int64(222)", got)
	}
	if got := first.Fields["bool_keyword"]; got != "true" {
		t.Fatalf("bool_keyword = %#v, want true keyword", got)
	}
	if got := first.Fields["happened_at"]; got != time.Date(2026, 8, 21, 12, 0, 1, 0, time.UTC) {
		t.Fatalf("happened_at = %#v", got)
	}
	if got := first.Fields["ratio"]; got != float64(1.25) {
		t.Fatalf("ratio = %#v, want float64(1.25)", got)
	}
	if got := first.Fields["active"]; got != true {
		t.Fatalf("active = %#v, want true", got)
	}
	if _, ok := first.Fields["absent"]; ok {
		t.Fatal("absent path emitted a field")
	}
	if _, ok := first.Fields["bad_long"]; ok {
		t.Fatal("malformed long emitted a field")
	}
	if len(first.Anomalies) != 2 || first.Anomalies[0].Kind != flow.AnomalyPromotion || first.Anomalies[1].Kind != flow.AnomalyPromotion {
		t.Fatalf("anomalies = %+v, want two promotion anomalies", first.Anomalies)
	}
	details := first.Anomalies[0].Detail + " " + first.Anomalies[1].Detail
	if !strings.Contains(details, "bad_long") || !strings.Contains(details, "abc") || !strings.Contains(details, "bad_date") || !strings.Contains(details, "tomorrow") {
		t.Fatalf("anomaly details = %q", details)
	}

	second, err := projection.Project(snapshot, configured)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, _ := json.Marshal(first)
	secondJSON, _ := json.Marshal(second)
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("promoted projection is not deterministic\nfirst  %s\nsecond %s", firstJSON, secondJSON)
	}
}

func TestProjectOmitsFieldsWhenNothingIsPromoted(t *testing.T) {
	observed := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	snapshot := flow.Finalized{Key: flow.Key{RuleID: "plain", CorrelationKey: "one"}, RuleID: "plain", Events: []event.Event{{Doc: map[string]any{"value": "x"}, ObservedAt: observed}}, FirstObservedAt: observed, FinalizedAt: observed}
	document, err := projection.Project(snapshot, rule.Rule{ID: "plain"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"fields"`) {
		t.Fatalf("document contains empty fields: %s", raw)
	}
}

func TestProjectIsByteDeterministicAndPreservesNegativeDuration(t *testing.T) {
	eventType, _ := path.Compile("$.event")
	timestamp, _ := path.Compile("$.datetime")
	groupBy, _ := path.Compile("$.invocation_id")
	configured := rule.Rule{ID: "http-flow", Version: "1", Extract: rule.Extract{EventType: eventType, Timestamp: timestamp},
		Stitch:    []rule.Stitch{{ID: "call", GroupBy: []path.Path{groupBy}, Roles: []rule.Role{{Name: "request", Types: []string{"request"}}, {Name: "response", Types: []string{"response"}}}, Requires: []string{"request", "response"}}},
		Lifecycle: rule.Lifecycle{Timeout: time.Second}, Output: rule.Output{Index: "flows"}}
	observed := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	current := flow.Open(flow.Key{RuleID: configured.ID, CorrelationKey: "abc"}, configured, event.Event{ObservedAt: observed})
	current.Apply(event.Event{Doc: map[string]any{"event": "response", "datetime": "2026-08-20T12:00:00Z", "invocation_id": "one"}, ObservedAt: observed}, configured, observed)
	current.Apply(event.Event{Doc: map[string]any{"event": "request", "datetime": "2026-08-20T12:00:01Z", "invocation_id": "one"}, ObservedAt: observed}, configured, observed)
	snapshot := current.Finalize(flow.ReasonInvocationsComplete, observed.Add(time.Second))

	first, err := projection.Project(snapshot, configured)
	if err != nil {
		t.Fatal(err)
	}
	second, err := projection.Project(snapshot, configured)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("Project bytes differ\nfirst  %s\nsecond %s", firstJSON, secondJSON)
	}
	if got := int64(first.Events[0]["duration_ms"].(int64)); got != -1000 {
		t.Fatalf("duration_ms = %d, want -1000", got)
	}
	if len(first.Anomalies) != 1 || first.Anomalies[0].Kind != flow.AnomalyClockSkew {
		t.Fatalf("anomalies = %+v, want clock skew", first.Anomalies)
	}
}

// ADR-0003, section 3: the first observed time prevents a later batch for one key overwriting its predecessor.
func TestOutputIDSeparatesRulesKeysAndStartTimes(t *testing.T) {
	started := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	base := projection.NewOutputID("http-flow", "abc", started)
	cases := map[string]projection.OutputID{
		"different rule":  projection.NewOutputID("other-flow", "abc", started),
		"different key":   projection.NewOutputID("http-flow", "xyz", started),
		"different start": projection.NewOutputID("http-flow", "abc", started.Add(time.Nanosecond)),
	}
	for name, got := range cases {
		if got == base {
			t.Errorf("%s: collides with base id %q", name, base)
		}
	}
}
