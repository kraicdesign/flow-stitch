package pebble

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	dbpebble "github.com/cockroachdb/pebble/v2"
	adapterrules "github.com/kraicdesign/flow-stitch/internal/adapters/rules"
	"github.com/kraicdesign/flow-stitch/internal/adapters/state/statetest"
	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/application/expire"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/path"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

func TestConformance(t *testing.T) {
	statetest.Run(t, func(t *testing.T) application.StateStore {
		store, err := Open(t.TempDir(), true)
		if err != nil {
			t.Fatal(err)
		}
		return store
	})
}

func TestDeadLetterCapDropsOldestRecord(t *testing.T) {
	ctx := context.Background()
	store, err := Open(t.TempDir(), true, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	records := []outbox.Record{
		{OutputID: "oldest", CreatedAt: now},
		{OutputID: "middle", CreatedAt: now.Add(time.Second)},
		{OutputID: "newest", CreatedAt: now.Add(2 * time.Second)},
	}
	var change application.DeadLetterChange
	err = store.WithTx(ctx, func(tx application.Tx) error {
		for _, record := range records {
			if err := tx.EnqueueOutbox(ctx, record); err != nil {
				return err
			}
		}
		results := make([]outbox.Result, len(records))
		for i := range records {
			results[i] = outbox.Result{OutputID: records[i].OutputID, Disposition: outbox.Permanent, RejectionType: "mapper_parsing_exception"}
		}
		var err error
		change, err = tx.ResolveOutbox(ctx, results)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if change.Records != 2 || change.Dropped != 1 || !change.Changed {
		t.Fatalf("dead-letter change = %+v, want records 2 dropped 1", change)
	}
	iter, err := store.db.NewIter(prefixOptions(deadPrefix))
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	retained := make(map[projection.OutputID]bool)
	for valid := iter.First(); valid; valid = iter.Next() {
		var record outbox.Record
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			t.Fatal(err)
		}
		retained[record.OutputID] = true
	}
	if retained["oldest"] || !retained["middle"] || !retained["newest"] {
		t.Fatalf("retained dead letters = %v, want middle and newest", retained)
	}
	summary, err := store.DeadLetters(ctx)
	if err != nil || summary.Records != 2 || summary.Dropped != 1 || summary.Reasons["mapper_parsing_exception"] != 2 {
		t.Fatalf("DeadLetters() = %+v, %v", summary, err)
	}
}

func TestDeadLetterDropCountSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	store, err := Open(directory, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := store.WithTx(ctx, func(tx application.Tx) error {
		for _, id := range []projection.OutputID{"one", "two"} {
			if err := tx.EnqueueOutbox(ctx, outbox.Record{OutputID: id, Index: "flows", CreatedAt: now}); err != nil {
				return err
			}
		}
		_, err := tx.ResolveOutbox(ctx, []outbox.Result{
			{OutputID: "one", Disposition: outbox.Permanent, RejectionType: "bad"},
			{OutputID: "two", Disposition: outbox.Permanent, RejectionType: "bad"},
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(directory, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	summary, err := reopened.DeadLetters(ctx)
	if err != nil || summary.Records != 1 || summary.Dropped != 1 || summary.Reasons["bad"] != 1 {
		t.Fatalf("restarted summary = %+v, %v", summary, err)
	}
}

func TestRestartRecoveryPreservesProjectedFlow(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	configured := persistentRule(t)
	now := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	f := persistentFlow(configured, "restart", now)
	want, err := projection.Project(f.Finalize(flow.ReasonTimeout, now.Add(time.Minute)), configured)
	if err != nil {
		t.Fatal(err)
	}

	first, err := Open(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.WithTx(ctx, func(tx application.Tx) error { return tx.SaveFlow(ctx, f) }); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := Open(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.WithTx(ctx, func(tx application.Tx) error {
		loaded, found, err := tx.LoadFlow(ctx, f.Key())
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("flow missing after reopen")
		}
		got, err := projection.Project(loaded.Finalize(flow.ReasonTimeout, now.Add(time.Minute)), configured)
		if err != nil {
			return err
		}
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want)
		if !reflect.DeepEqual(gotJSON, wantJSON) {
			t.Fatalf("projection after reopen = %s, want %s", gotJSON, wantJSON)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOverdueFlowFinalizesAfterReopen(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	configured := persistentRule(t)
	start := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	f := persistentFlow(configured, "overdue", start)
	first, err := Open(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.WithTx(ctx, func(tx application.Tx) error { return tx.SaveFlow(ctx, f) }); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := Open(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	service := expire.New(second, adapterrules.NewRegistry([]rule.Rule{configured}), fixedClock{start.Add(time.Minute)}, 128, application.NoopRecorder{})
	count, err := service.Sweep(ctx, start.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("Sweep() = %d, want 1", count)
	}
	if err := second.WithTx(ctx, func(tx application.Tx) error {
		if _, found, err := tx.LoadFlow(ctx, f.Key()); err != nil || found {
			t.Fatalf("LoadFlow after sweep = found %v, err %v", found, err)
		}
		records, err := tx.PendingOutbox(ctx, start.Add(2*time.Minute), 10)
		if err != nil {
			return err
		}
		if len(records) != 1 {
			t.Fatalf("PendingOutbox() = %d, want 1", len(records))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLoadFlowRejectsUnknownFormatVersion(t *testing.T) {
	directory := t.TempDir()
	key := flow.Key{RuleID: "rule", CorrelationKey: "unknown"}
	db, err := dbpebble.Open(directory, &dbpebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set(flowKey(key), []byte{42, '{', '}'}, dbpebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	err = store.WithTx(context.Background(), func(tx application.Tx) error { _, _, err := tx.LoadFlow(context.Background(), key); return err })
	if err == nil || !strings.Contains(err.Error(), "unknown format version 42") {
		t.Fatalf("LoadFlow() = %v, want error naming version 42", err)
	}
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func persistentRule(t *testing.T) rule.Rule {
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
	return rule.Rule{ID: "rule", Version: "1", Enabled: true, Extract: rule.Extract{EventType: eventType, Timestamp: timestamp},
		Stitch:    []rule.Stitch{{ID: "call", GroupBy: []path.Path{group}, Roles: []rule.Role{{Name: "request", Types: []string{"request"}}, {Name: "response", Types: []string{"response"}}}, Requires: []string{"request", "response"}}},
		Lifecycle: rule.Lifecycle{Timeout: 10 * time.Second}, Output: rule.Output{Index: "flows", TimestampSource: rule.TimestampFirstEvent}}
}

func persistentFlow(configured rule.Rule, key string, now time.Time) *flow.Flow {
	e := event.Event{Doc: map[string]any{"event": "request", "id": "one", "timestamp": now.Format(time.RFC3339Nano)}, ObservedAt: now}
	f := flow.Open(flow.Key{RuleID: configured.ID, CorrelationKey: key}, configured, e)
	f.Apply(e, configured, now)
	f.Apply(event.Event{Doc: map[string]any{"event": "response", "id": "one", "timestamp": now.Add(time.Second).Format(time.RFC3339Nano)}, ObservedAt: now.Add(time.Second)}, configured, now.Add(time.Second))
	return f
}
