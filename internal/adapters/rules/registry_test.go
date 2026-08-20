package rules_test

import (
	"testing"

	adapter "github.com/kraicdesign/flow-stitch/internal/adapters/rules"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/path"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

func mustPath(t *testing.T, expression string) path.Path {
	t.Helper()
	compiled, err := path.Compile(expression)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestMatchUsesFirstResolvableEnabledRule(t *testing.T) {
	registry := adapter.NewRegistry([]rule.Rule{
		{ID: "disabled", Enabled: false, Key: mustPath(t, "$.shared")},
		{ID: "first", Enabled: true, Key: mustPath(t, "$.shared")},
		{ID: "second", Enabled: true, Key: mustPath(t, "$.other")},
	})
	got, ok := registry.MatchAndRetain(event.Event{Doc: map[string]any{"shared": "a", "other": "b"}})
	if !ok || got.ID != "first" {
		t.Fatalf("Match() = (%q, %v), want (first, true)", got.ID, ok)
	}
}

func TestMatchReturnsNothingWhenNoKeyResolves(t *testing.T) {
	registry := adapter.NewRegistry([]rule.Rule{{ID: "one", Enabled: true, Key: mustPath(t, "$.flow_id")}})
	if got, ok := registry.MatchAndRetain(event.Event{Doc: map[string]any{"other": "x"}}); ok {
		t.Fatalf("Match() = (%q, true), want no match", got.ID)
	}
}

func TestOldVersionIsRetainedUntilEveryOpenFlowReleasesIt(t *testing.T) {
	old := rule.Rule{ID: "one", Version: "old", Enabled: true, Key: mustPath(t, "$.flow_id")}
	registry := adapter.NewRegistry([]rule.Rule{old})
	e := event.Event{Doc: map[string]any{"flow_id": "a"}}
	for range 2 {
		if _, ok := registry.MatchAndRetain(e); !ok {
			t.Fatal("MatchAndRetain() = false, want true")
		}
	}
	newRule := old
	newRule.Version = "new"
	registry.Publish([]rule.Rule{newRule})
	if _, ok := registry.Get("one", "old"); !ok {
		t.Fatal("old version reclaimed while flows still reference it")
	}
	registry.Release(rule.Reference{ID: "one", Version: "old"})
	if _, ok := registry.Get("one", "old"); !ok {
		t.Fatal("old version reclaimed while one flow still references it")
	}
	registry.Release(rule.Reference{ID: "one", Version: "old"})
	if _, ok := registry.Get("one", "old"); ok {
		t.Fatal("old version retained after its final reference was released")
	}
}
