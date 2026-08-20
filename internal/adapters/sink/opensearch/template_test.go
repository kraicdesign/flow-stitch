package opensearch_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/adapters/sink/opensearch"
	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/application/alerts"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/path"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

func TestGenerateTemplatesUnionsRulesByIndexAndMapsPromotedTypes(t *testing.T) {
	rules := []rule.Rule{
		{ID: "one", Output: rule.Output{Index: "application-flows-{yyyy}.{MM}.{dd}"}, Stitch: []rule.Stitch{{Roles: []rule.Role{{Name: "request"}, {Name: "response"}}}}, Promote: []rule.Promotion{{Name: "status", Type: rule.PromotionLong}}},
		{ID: "two", Output: rule.Output{Index: "application-flows-{yyyy}.{MM}.{dd}"}, Stitch: []rule.Stitch{{Roles: []rule.Role{{Name: "error"}}}}, Promote: []rule.Promotion{{Name: "happened_at", Type: rule.PromotionDate}}},
		{ID: "audit", Output: rule.Output{Index: "audit-sessions"}, Promote: []rule.Promotion{{Name: "active", Type: rule.PromotionBoolean}}},
	}
	first, err := opensearch.MarshalTemplates(rules)
	if err != nil {
		t.Fatal(err)
	}
	second, err := opensearch.MarshalTemplates(rules)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatalf("template output changed across runs\nfirst: %s\nsecond: %s", first, second)
	}
	var generated []struct {
		Index    string         `json:"index"`
		Template map[string]any `json:"template"`
	}
	if err := json.Unmarshal(first, &generated); err != nil {
		t.Fatalf("generated output is invalid JSON: %v", err)
	}
	if len(generated) != 2 {
		t.Fatalf("templates = %d, want 2", len(generated))
	}
	shared := generated[0]
	if generated[1].Index == "application-flows-{yyyy}.{MM}.{dd}" {
		shared = generated[1]
	}
	if shared.Index != "application-flows-{yyyy}.{MM}.{dd}" {
		t.Fatalf("shared index not found: %+v", generated)
	}
	raw, _ := json.Marshal(shared.Template)
	text := string(raw)
	for _, fragment := range []string{
		`"index_patterns":["application-flows-*"]`,
		`"status":{"type":"long"}`,
		`"happened_at":{"type":"date"}`,
		`"request"`, `"response"`, `"error"`,
		`"event":{"type":"flat_object"}`,
		`"dynamic":false`,
	} {
		if !strings.Contains(text, fragment) {
			t.Errorf("shared template missing %s: %s", fragment, text)
		}
	}
}

func TestMarshalTemplatePrintsAPIReadySelectedTemplate(t *testing.T) {
	rules := []rule.Rule{{ID: "one", Output: rule.Output{Index: "flows-{yyyy}.{MM}.{dd}"}, Promote: []rule.Promotion{{Name: "latency", Type: rule.PromotionDouble}}}}
	raw, err := opensearch.MarshalTemplate(rules, "flows-{yyyy}.{MM}.{dd}")
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["index_patterns"]; !ok {
		t.Fatalf("selected template is not API-ready: %s", raw)
	}
}

func TestGeneratedTemplateEnumeratesCollisionSafeGroupFields(t *testing.T) {
	service, _ := path.Compile("$.service")
	contextID, _ := path.Compile("$.context.id")
	payloadID, _ := path.Compile("$.payload.id")
	rules := []rule.Rule{{Output: rule.Output{Index: "flows"}, Stitch: []rule.Stitch{{GroupBy: []path.Path{service, contextID, payloadID}}}}}
	raw, err := opensearch.MarshalTemplate(rules, "flows")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, fragment := range []string{`"service": {`, `"id": {`, `"id2": {`} {
		if !strings.Contains(text, fragment) {
			t.Errorf("template missing group field %s: %s", fragment, text)
		}
	}
	if strings.Contains(text, "dynamic_templates") || strings.Contains(text, "$.service") {
		t.Fatalf("template contains obsolete dynamic/path mapping: %s", text)
	}
}

func TestGeneratedAlertsTemplateIsFixedAndContainsNoProducerFields(t *testing.T) {
	rules := []rule.Rule{{Output: rule.Output{Index: "flows"}, Stitch: []rule.Stitch{{Roles: []rule.Role{{Name: "producer_role"}}}}, Promote: []rule.Promotion{{Name: "producer_promotion", Type: rule.PromotionKeyword}}}}
	raw, err := opensearch.MarshalTemplate(rules, "flowstitch-alerts-{yyyy}.{MM}.{dd}", "flowstitch-alerts-{yyyy}.{MM}.{dd}")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, want := range []string{`"dynamic": false`, `"kind"`, `"state"`, `"counts"`, `"reasons"`, `"indices"`, `"oldest_outbox_age_seconds"`} {
		if !strings.Contains(text, want) {
			t.Errorf("alerts template missing %s: %s", want, text)
		}
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	template := body["template"].(map[string]any)
	mappings := template["mappings"].(map[string]any)
	properties := mappings["properties"].(map[string]any)
	for _, forbidden := range []string{"events", "flow", "fields", "producer_role", "producer_promotion"} {
		if _, exists := properties[forbidden]; exists {
			t.Errorf("alerts template contains producer-derived field %q: %s", forbidden, text)
		}
	}
}

func TestDisabledAlertsAddNoTemplate(t *testing.T) {
	rules := []rule.Rule{{Output: rule.Output{Index: "flows"}}}
	raw, err := opensearch.MarshalTemplates(rules)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "flowstitch-alerts") {
		t.Fatalf("disabled alerts index referenced by templates: %s", raw)
	}
}

func TestGeneratedTemplatesAcceptRepresentativeProjectedDocuments(t *testing.T) {
	configured := representativeRule(t)
	document := representativeFlowDocument(t, configured)
	flowTemplate := opensearch.GenerateTemplates([]rule.Rule{configured})[0].Template
	assertTemplateAcceptsJSON(t, flowTemplate, document)

	sink := &capturingAlertSink{}
	now := time.Date(2026, 8, 21, 12, 10, 0, 0, time.UTC)
	reporter := alerts.New(alerts.Options{
		Enabled: true, Index: "flowstitch-alerts-{yyyy}.{MM}.{dd}",
		MinInterval: time.Minute, OutboxAgeThreshold: time.Minute,
	}, outbox.DeadLetterSummary{
		Records: 1, Dropped: 2,
		Reasons: map[string]int{"mapper_parsing_exception": 1},
		Indices: map[string]int{"flows-2026.08.21": 1},
	}, sink, application.NoopRecorder{}, nil)
	reporter.Observe(context.Background(), []outbox.Record{{Index: "flows-2026.08.21", CreatedAt: now.Add(-2 * time.Minute)}}, now)
	if len(sink.documents) == 0 {
		t.Fatal("representative alert was not projected")
	}
	alertTemplates := opensearch.GenerateTemplates(nil, "flowstitch-alerts-{yyyy}.{MM}.{dd}")
	assertTemplateAcceptsJSON(t, alertTemplates[0].Template, sink.documents[0])
}

type capturingAlertSink struct{ documents [][]byte }

func (s *capturingAlertSink) DeliverAlert(_ context.Context, record application.AlertRecord) error {
	s.documents = append(s.documents, record.Document)
	return nil
}

func representativeRule(t *testing.T) rule.Rule {
	t.Helper()
	compile := func(raw string) path.Path {
		compiled, err := path.Compile(raw)
		if err != nil {
			t.Fatalf("compile path %q: %v", raw, err)
		}
		return compiled
	}
	configured := rule.Rule{
		ID: "representative", Extract: rule.Extract{EventType: compile("$.event"), Timestamp: compile("$.datetime")},
		Stitch: []rule.Stitch{{
			ID: "call", GroupBy: []path.Path{compile("$.service"), compile("$.context.invocation_id")},
			Roles:    []rule.Role{{Name: "request", Types: []string{"request"}}, {Name: "response", Types: []string{"response"}}},
			Requires: []string{"request", "response"},
		}},
		Promote: []rule.Promotion{
			{Name: "as_long", Path: compile("$.promoted.as_long"), Type: rule.PromotionLong},
			{Name: "as_double", Path: compile("$.promoted.as_double"), Type: rule.PromotionDouble},
			{Name: "as_keyword", Path: compile("$.promoted.as_keyword"), Type: rule.PromotionKeyword},
			{Name: "as_boolean", Path: compile("$.promoted.as_boolean"), Type: rule.PromotionBoolean},
			{Name: "as_date", Path: compile("$.promoted.as_date"), Type: rule.PromotionDate},
		},
		Lifecycle: rule.Lifecycle{Timeout: time.Minute}, Output: rule.Output{Index: "flows-{yyyy}.{MM}.{dd}"},
	}
	version, err := rule.ContentVersion(configured)
	if err != nil {
		t.Fatal(err)
	}
	configured.Version = version
	return configured
}

func representativeFlowDocument(t *testing.T, configured rule.Rule) projection.Document {
	t.Helper()
	observed := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	makeEvent := func(offset time.Duration, eventType, service, invocationID, timestamp string, extra map[string]any) event.Event {
		doc := map[string]any{
			"event": eventType, "service": service, "datetime": timestamp,
			"context": map[string]any{"invocation_id": invocationID},
		}
		for key, value := range extra {
			doc[key] = value
		}
		return event.Event{Doc: doc, ObservedAt: observed.Add(offset)}
	}
	request := makeEvent(0, "request", "web", "complete", "2026-08-21T12:00:00Z", map[string]any{
		"promoted": map[string]any{
			"as_long": 42, "as_double": 1.25, "as_keyword": "value",
			"as_boolean": true, "as_date": "2026-08-21T12:00:00Z",
		},
	})
	response := makeEvent(time.Second, "response", "web", "complete", "2026-08-21T12:00:01Z", nil)
	incomplete := makeEvent(2*time.Second, "request", "worker", "incomplete", "2026-08-21T12:00:02Z", nil)
	plain := makeEvent(3*time.Second, "plain", "worker", "plain", "2026-08-21T12:00:03Z", map[string]any{"payload": map[string]any{"arbitrary": true}})
	conflict := makeEvent(4*time.Second, "response", "web", "complete", "2026-08-21T12:00:04Z", nil)

	current := flow.Open(flow.Key{RuleID: configured.ID, CorrelationKey: "flow-1"}, configured, request)
	for _, item := range []event.Event{request, response, response, incomplete, plain, conflict} {
		current.Apply(item, configured, item.ObservedAt)
	}
	document, err := projection.Project(current.Finalize(flow.ReasonTimeout, observed.Add(time.Minute)), configured)
	if err != nil {
		t.Fatal(err)
	}
	return document
}

func assertTemplateAcceptsJSON(t *testing.T, template map[string]any, document any) {
	t.Helper()
	raw, ok := document.([]byte)
	if !ok {
		var err error
		raw, err = json.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	mappings := template["template"].(map[string]any)["mappings"].(map[string]any)
	walkMappedJSON(t, "", decoded, mappings, dynamicMappings(mappings))
}

func walkMappedJSON(t *testing.T, fieldPath string, value any, mapping map[string]any, dynamic []map[string]any) {
	t.Helper()
	if mapping["type"] == "flat_object" {
		return
	}
	switch typed := value.(type) {
	case map[string]any:
		properties, _ := mapping["properties"].(map[string]any)
		for name, child := range typed {
			path := name
			if fieldPath != "" {
				path = fieldPath + "." + name
			}
			childMapping, ok := properties[name].(map[string]any)
			if !ok {
				childMapping, ok = matchingDynamicMapping(path, name, child, dynamic)
			}
			if !ok {
				t.Errorf("field %s emits %s but has no explicit, flat_object, or dynamic-template mapping", path, jsonValueType(child))
				continue
			}
			walkMappedJSON(t, path, child, childMapping, dynamic)
		}
	case []any:
		for _, child := range typed {
			walkMappedJSON(t, fieldPath, child, mapping, dynamic)
		}
	default:
		mappedType, _ := mapping["type"].(string)
		if !compatibleJSONType(typed, mappedType) {
			t.Errorf("field %s emits %s but is mapped as %q", fieldPath, jsonValueType(typed), mappedType)
		}
	}
}

func compatibleJSONType(value any, mappedType string) bool {
	switch typed := value.(type) {
	case string:
		return mappedType == "keyword" || mappedType == "date"
	case bool:
		return mappedType == "boolean"
	case json.Number:
		if mappedType == "double" {
			return true
		}
		return mappedType == "long" && !strings.ContainsAny(typed.String(), ".eE")
	case nil:
		return true
	default:
		return false
	}
}

func jsonValueType(value any) string {
	switch typed := value.(type) {
	case string:
		return "string"
	case bool:
		return "boolean"
	case json.Number:
		if strings.ContainsAny(typed.String(), ".eE") {
			return "number (non-integer)"
		}
		return "number (integer)"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case nil:
		return "null"
	default:
		return fmt.Sprintf("%T", value)
	}
}

func dynamicMappings(mappings map[string]any) []map[string]any {
	declared, _ := mappings["dynamic_templates"].([]any)
	result := make([]map[string]any, 0, len(declared))
	for _, entry := range declared {
		for _, definition := range entry.(map[string]any) {
			result = append(result, definition.(map[string]any))
		}
	}
	return result
}

func matchingDynamicMapping(fieldPath, fieldName string, value any, templates []map[string]any) (map[string]any, bool) {
	for _, candidate := range templates {
		if pattern, ok := candidate["path_match"].(string); ok && !wildcardMatch(pattern, fieldPath) {
			continue
		}
		if pattern, ok := candidate["match"].(string); ok && !wildcardMatch(pattern, fieldName) {
			continue
		}
		if expected, ok := candidate["match_mapping_type"].(string); ok && expected != jsonDynamicType(value) {
			continue
		}
		mapping, ok := candidate["mapping"].(map[string]any)
		if ok {
			return mapping, true
		}
	}
	return nil, false
}

func wildcardMatch(pattern, value string) bool {
	matched, err := filepath.Match(pattern, value)
	return err == nil && matched
}

func jsonDynamicType(value any) string {
	switch value.(type) {
	case string:
		return "string"
	case bool:
		return "boolean"
	case json.Number:
		return "long"
	case map[string]any:
		return "object"
	default:
		return ""
	}
}
