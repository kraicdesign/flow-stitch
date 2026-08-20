package opensearch

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/kraicdesign/flow-stitch/internal/domain/indexname"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

// GeneratedTemplate pairs one configured output index with its OpenSearch template body.
type GeneratedTemplate struct {
	Index    string         `json:"index"`
	Template map[string]any `json:"template"`
}

// GenerateTemplates returns one stable template per distinct configured output index.
func GenerateTemplates(rules []rule.Rule, alertsIndex ...string) []GeneratedTemplate {
	byIndex := make(map[string][]rule.Rule)
	for _, configured := range rules {
		byIndex[configured.Output.Index] = append(byIndex[configured.Output.Index], configured)
	}
	indices := make([]string, 0, len(byIndex))
	for index := range byIndex {
		indices = append(indices, index)
	}
	sort.Strings(indices)

	generated := make([]GeneratedTemplate, 0, len(indices))
	for _, index := range indices {
		generated = append(generated, GeneratedTemplate{Index: index, Template: templateFor(index, byIndex[index])})
	}
	if len(alertsIndex) > 0 && alertsIndex[0] != "" {
		generated = append(generated, GeneratedTemplate{Index: alertsIndex[0], Template: alertTemplate(alertsIndex[0])})
		sort.Slice(generated, func(i, j int) bool { return generated[i].Index < generated[j].Index })
	}
	return generated
}

// MarshalTemplates prints the stable, multi-index command output.
func MarshalTemplates(rules []rule.Rule, alertsIndex ...string) ([]byte, error) {
	return json.MarshalIndent(GenerateTemplates(rules, alertsIndex...), "", "  ")
}

// MarshalTemplate prints one API-ready template selected by configured index name.
func MarshalTemplate(rules []rule.Rule, index string, alertsIndex ...string) ([]byte, error) {
	for _, generated := range GenerateTemplates(rules, alertsIndex...) {
		if generated.Index == index {
			return json.MarshalIndent(generated.Template, "", "  ")
		}
	}
	return nil, fmt.Errorf("opensearch: no rules write to output index %q", index)
}

func templateFor(index string, rules []rule.Rule) map[string]any {
	roles := make(map[string]struct{})
	groups := make(map[string]struct{})
	promotions := make(map[string]rule.PromotionType)
	for _, configured := range rules {
		for _, stitch := range configured.Stitch {
			for _, group := range stitch.GroupFields() {
				groups[group] = struct{}{}
			}
			for _, role := range stitch.Roles {
				roles[role.Name] = struct{}{}
			}
		}
		for _, promoted := range configured.Promote {
			promotions[promoted.Name] = promoted.Type
		}
	}

	eventProperties := map[string]any{
		"complete":        typeMapping("boolean"),
		"duration_ms":     typeMapping("long"),
		"started_at":      typeMapping("date"),
		"ended_at":        typeMapping("date"),
		"type":            typeMapping("keyword"),
		"timestamp":       typeMapping("date"),
		"duplicate_count": typeMapping("long"),
		"group": map[string]any{
			"dynamic":    false,
			"properties": groupProperties(groups),
		},
		"event": typeMapping("flat_object"),
	}
	roleNames := sortedKeys(roles)
	for _, roleName := range roleNames {
		eventProperties[roleName] = map[string]any{
			"dynamic": false,
			"properties": map[string]any{
				"type": typeMapping("keyword"), "timestamp": typeMapping("date"), "event": typeMapping("flat_object"),
			},
		}
	}

	fieldProperties := make(map[string]any, len(promotions))
	for _, name := range sortedKeys(promotions) {
		fieldProperties[name] = typeMapping(string(promotions[name]))
	}

	properties := map[string]any{
		"@timestamp": typeMapping("date"),
		"flow": map[string]any{
			"dynamic": false,
			"properties": map[string]any{
				"id": typeMapping("keyword"), "rule_id": typeMapping("keyword"), "rule_version": typeMapping("keyword"),
				"finalization_reason": typeMapping("keyword"), "started_at": typeMapping("date"), "ended_at": typeMapping("date"),
				"duration_ms": typeMapping("long"), "age_ms": typeMapping("long"), "event_count": typeMapping("long"),
				"entry_count": typeMapping("long"), "duplicate_count": typeMapping("long"), "incomplete_invocations": typeMapping("long"),
				"duplicates": map[string]any{"properties": map[string]any{"type": typeMapping("keyword"), "count": typeMapping("long")}},
			},
		},
		"events": map[string]any{"type": "object", "dynamic": false, "properties": eventProperties},
		"anomalies": map[string]any{"type": "object", "dynamic": false, "properties": map[string]any{
			"kind": typeMapping("keyword"), "detail": typeMapping("keyword"), "at": typeMapping("date"),
		}},
	}
	if len(fieldProperties) > 0 {
		properties["fields"] = map[string]any{"dynamic": false, "properties": fieldProperties}
	}

	return map[string]any{
		"index_patterns": []string{indexname.WildcardPattern(index)},
		"template": map[string]any{
			"mappings": map[string]any{
				"dynamic":    false,
				"properties": properties,
			},
		},
		"priority": 100,
		"_meta":    map[string]any{"description": "FlowStitch engine and configured promoted-field mappings."},
	}
}

func alertTemplate(index string) map[string]any {
	properties := map[string]any{
		"@timestamp":                typeMapping("date"),
		"kind":                      typeMapping("keyword"),
		"state":                     typeMapping("keyword"),
		"summary":                   typeMapping("keyword"),
		"indices":                   typeMapping("keyword"),
		"oldest_outbox_age_seconds": typeMapping("double"),
		"counts": map[string]any{"dynamic": false, "properties": map[string]any{
			"dead_letter_records": typeMapping("long"),
			"dead_letter_dropped": typeMapping("long"),
			"outbox_records":      typeMapping("long"),
		}},
		"reasons": map[string]any{"type": "object", "dynamic": false, "properties": map[string]any{
			"type": typeMapping("keyword"), "count": typeMapping("long"),
		}},
	}
	return map[string]any{
		"index_patterns": []string{indexname.WildcardPattern(index)},
		"template": map[string]any{"mappings": map[string]any{
			"dynamic": false, "properties": properties,
		}},
		"priority": 100,
		"_meta":    map[string]any{"description": "FlowStitch engine-owned stuck-document alerts."},
	}
}

func groupProperties(groups map[string]struct{}) map[string]any {
	properties := make(map[string]any, len(groups))
	for _, name := range sortedKeys(groups) {
		properties[name] = typeMapping("keyword")
	}
	return properties
}

func typeMapping(name string) map[string]any { return map[string]any{"type": name} }

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
