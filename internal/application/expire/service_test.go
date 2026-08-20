package expire_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	adapterrules "github.com/kraicdesign/flow-stitch/internal/adapters/rules"
	"github.com/kraicdesign/flow-stitch/internal/adapters/state/memory"
	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/application/expire"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/path"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

type expiryClock struct{ now time.Time }

func (c expiryClock) Now() time.Time { return c.now }

type expiryLogger struct{ calls int }

func (l *expiryLogger) ErrorContext(context.Context, string, ...any) { l.calls++ }

func TestMissingPinnedRuleDoesNotStallOtherDueFlows(t *testing.T) {
	keyPath, _ := path.Compile("$.flow_id")
	eventType, _ := path.Compile("$.event")
	timestamp, _ := path.Compile("$.datetime")
	configured := rule.Rule{ID: "r", Version: "1", Enabled: true, Key: keyPath, Extract: rule.Extract{EventType: eventType, Timestamp: timestamp}, Lifecycle: rule.Lifecycle{Timeout: time.Second}, Output: rule.Output{Index: "flows"}}
	store := memory.New()
	registry := adapterrules.NewRegistry([]rule.Rule{configured})
	logger := &expiryLogger{}
	start := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	missingRule := configured
	missingRule.Version = "2"
	missingEvent := event.Event{Doc: map[string]any{"flow_id": "missing", "event": "log", "datetime": start.Format(time.RFC3339Nano)}, ObservedAt: start}
	validEvent := event.Event{Doc: map[string]any{"flow_id": "valid", "event": "log", "datetime": start.Add(time.Second).Format(time.RFC3339Nano)}, ObservedAt: start.Add(time.Second)}
	missing := flow.Open(flow.Key{RuleID: "r", CorrelationKey: "missing"}, missingRule, missingEvent)
	missing.Apply(missingEvent, missingRule, start)
	valid := flow.Open(flow.Key{RuleID: "r", CorrelationKey: "valid"}, configured, validEvent)
	valid.Apply(validEvent, configured, start.Add(time.Second))
	if err := store.WithTx(context.Background(), func(tx application.Tx) error {
		if err := tx.SaveFlow(context.Background(), missing); err != nil {
			return err
		}
		return tx.SaveFlow(context.Background(), valid)
	}); err != nil {
		t.Fatal(err)
	}

	service := expire.New(store, registry, expiryClock{now: start.Add(time.Minute)}, 1, application.NoopRecorder{}, logger)
	count, err := service.Sweep(context.Background(), start.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("Sweep() = %d, want 1", count)
	}
	if logger.calls != 1 {
		t.Fatalf("logger calls = %d, want 1", logger.calls)
	}
	counts, err := store.OpenFlows(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := counts[rule.Reference{ID: "r", Version: "1"}]; got != 1 {
		t.Fatalf("OpenFlows = %d, want unresolved flow only", got)
	}
	if got := store.PendingRecords(); got != 1 {
		t.Fatalf("PendingRecords = %d, want 1", got)
	}
	if got := pendingReason(t, store); got != flow.ReasonRuleUnavailable {
		t.Fatalf("reason = %q, want rule_unavailable", got)
	}
}

func TestRestartWithUnchangedConfigResolvesRecoveredFlow(t *testing.T) {
	configured := expiryRule(t, "stable")
	start := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	e := event.Event{Doc: map[string]any{"flow_id": "one", "event": "log", "datetime": start.Format(time.RFC3339Nano)}, ObservedAt: start}
	original := flow.Open(flow.Key{RuleID: configured.ID, CorrelationKey: "one"}, configured, e)
	original.Apply(e, configured, start)
	raw, err := original.Encode()
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := flow.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	if err := store.WithTx(context.Background(), func(tx application.Tx) error { return tx.SaveFlow(context.Background(), recovered) }); err != nil {
		t.Fatal(err)
	}
	registry := adapterrules.NewRegistry([]rule.Rule{configured})
	counts, _ := store.OpenFlows(context.Background())
	registry.SeedOpenFlows(counts)
	service := expire.New(store, registry, expiryClock{now: start.Add(time.Minute)}, 10, application.NoopRecorder{})
	if count, err := service.Sweep(context.Background(), start.Add(time.Minute)); err != nil || count != 1 {
		t.Fatalf("Sweep() = %d, %v; want 1, nil", count, err)
	}
	if got := pendingReason(t, store); got != flow.ReasonTimeout {
		t.Fatalf("reason = %q, want timeout", got)
	}
}

func expiryRule(t *testing.T, version rule.Version) rule.Rule {
	t.Helper()
	key, _ := path.Compile("$.flow_id")
	eventType, _ := path.Compile("$.event")
	timestamp, _ := path.Compile("$.datetime")
	return rule.Rule{ID: "r", Version: version, Enabled: true, Key: key, Extract: rule.Extract{EventType: eventType, Timestamp: timestamp}, Lifecycle: rule.Lifecycle{Timeout: time.Second}, Output: rule.Output{Index: "flows"}}
}

func pendingReason(t *testing.T, store *memory.Store) flow.Reason {
	t.Helper()
	var records []outbox.Record
	if err := store.WithTx(context.Background(), func(tx application.Tx) error {
		var err error
		records, err = tx.PendingOutbox(context.Background(), time.Now().Add(24*time.Hour), 10)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(records) == 0 {
		t.Fatal("no pending outbox record")
	}
	var document projection.Document
	if err := json.Unmarshal(records[0].Document, &document); err != nil {
		t.Fatal(err)
	}
	return document.Flow.Reason
}
