// Package statetest provides the conformance suite for state-store adapters.
package statetest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/path"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

// Run exercises the behaviour every application.StateStore must provide.
func Run(t *testing.T, newStore func(t *testing.T) application.StateStore) {
	t.Helper()
	t.Run("transaction rollback", func(t *testing.T) {
		store := openStore(t, newStore)
		ctx := context.Background()
		f, _ := simpleFlow(t, "rollback", time.Now().UTC(), 10*time.Second)
		sentinel := errors.New("rollback")
		err := store.WithTx(ctx, func(tx application.Tx) error {
			if err := tx.SaveFlow(ctx, f); err != nil {
				return err
			}
			if err := tx.EnqueueOutbox(ctx, testRecord("rollback", time.Now().UTC())); err != nil {
				return err
			}
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("WithTx() = %v, want %v", err, sentinel)
		}
		mustTx(t, store, func(tx application.Tx) error {
			if _, found, err := tx.LoadFlow(ctx, f.Key()); err != nil || found {
				t.Fatalf("LoadFlow after rollback = found %v, err %v", found, err)
			}
			records, err := tx.PendingOutbox(ctx, time.Now().Add(time.Hour), 10)
			if err != nil || len(records) != 0 {
				t.Fatalf("PendingOutbox after rollback = %d, err %v, want 0", len(records), err)
			}
			return nil
		})
	})

	t.Run("flow round trip", func(t *testing.T) {
		store := openStore(t, newStore)
		ctx := context.Background()
		f, configured := richFlow(t)
		mustTx(t, store, func(tx application.Tx) error { return tx.SaveFlow(ctx, f) })
		mustTx(t, store, func(tx application.Tx) error {
			loaded, found, err := tx.LoadFlow(ctx, f.Key())
			if err != nil {
				return err
			}
			if !found {
				t.Fatal("LoadFlow() did not find saved flow")
			}
			assertEquivalentFlow(t, loaded, f, configured)
			return nil
		})
	})

	t.Run("open flow counts per rule", func(t *testing.T) {
		store := openStore(t, newStore)
		ctx := context.Background()
		now := time.Now().UTC()
		for _, item := range []struct {
			ruleID rule.ID
			key    string
		}{{"one", "a"}, {"one", "b"}, {"two", "c"}} {
			configured := rule.Rule{ID: item.ruleID, Version: "1", Lifecycle: rule.Lifecycle{Timeout: time.Minute}}
			e := event.Event{Doc: map[string]any{"key": item.key}, ObservedAt: now}
			f := flow.Open(flow.Key{RuleID: item.ruleID, CorrelationKey: item.key}, configured, e)
			f.Apply(e, configured, now)
			mustTx(t, store, func(tx application.Tx) error { return tx.SaveFlow(ctx, f) })
		}
		got, err := store.OpenFlows(ctx)
		if err != nil {
			t.Fatal(err)
		}
		want := map[rule.Reference]int{{ID: "one", Version: "1"}: 2, {ID: "two", Version: "1"}: 1}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("OpenFlows() = %v, want %v", got, want)
		}
	})

	t.Run("expiry ordering and limit", func(t *testing.T) {
		store := openStore(t, newStore)
		ctx := context.Background()
		now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
		var want []flow.Key
		for _, item := range []struct {
			key    string
			offset time.Duration
		}{{"middle", 2 * time.Second}, {"oldest", time.Second}, {"future", 4 * time.Second}} {
			f, _ := simpleFlow(t, item.key, now, item.offset)
			mustTx(t, store, func(tx application.Tx) error { return tx.SaveFlow(ctx, f) })
			if item.key != "future" {
				want = append(want, f.Key())
			}
		}
		want[0], want[1] = want[1], want[0]
		mustTx(t, store, func(tx application.Tx) error {
			got, err := tx.DueFlows(ctx, now.Add(3*time.Second), 1)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(got, want[:1]) {
				t.Fatalf("DueFlows(limit 1) = %v, want %v", got, want[:1])
			}
			got, err = tx.DueFlows(ctx, now.Add(3*time.Second), 10)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("DueFlows() = %v, want %v", got, want)
			}
			return nil
		})
	})

	t.Run("deadline movement", func(t *testing.T) {
		store := openStore(t, newStore)
		ctx := context.Background()
		now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
		f, configured := completedSettleFlow(t, "move", now)
		mustTx(t, store, func(tx application.Tx) error { return tx.SaveFlow(ctx, f) })
		if _, close := f.ShouldClose(configured, now); close {
			t.Fatal("ShouldClose() closed before settle")
		}
		mustTx(t, store, func(tx application.Tx) error { return tx.SaveFlow(ctx, f) })
		mustTx(t, store, func(tx application.Tx) error {
			got, err := tx.DueFlows(ctx, now.Add(2*time.Second), 10)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(got, []flow.Key{f.Key()}) {
				t.Fatalf("DueFlows at moved deadline = %v, want %v", got, []flow.Key{f.Key()})
			}
			got, err = tx.DueFlows(ctx, now.Add(time.Minute), 10)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(got, []flow.Key{f.Key()}) {
				t.Fatalf("DueFlows contains stale deadline: %v", got)
			}
			return nil
		})
	})

	t.Run("outbox idempotency", func(t *testing.T) {
		store := openStore(t, newStore)
		ctx := context.Background()
		first := testRecord("same", time.Now().UTC())
		second := first
		second.Document = []byte(`{"revision":2}`)
		mustTx(t, store, func(tx application.Tx) error {
			if err := tx.EnqueueOutbox(ctx, first); err != nil {
				return err
			}
			return tx.EnqueueOutbox(ctx, second)
		})
		mustTx(t, store, func(tx application.Tx) error {
			got, err := tx.PendingOutbox(ctx, time.Now().Add(time.Hour), 10)
			if err != nil {
				return err
			}
			if len(got) != 1 || string(got[0].Document) != string(second.Document) {
				t.Fatalf("PendingOutbox() = %+v, want one overwritten record", got)
			}
			return nil
		})
	})

	t.Run("outbox count", func(t *testing.T) {
		store := openStore(t, newStore)
		ctx := context.Background()
		mustTx(t, store, func(tx application.Tx) error {
			if err := tx.EnqueueOutbox(ctx, testRecord("one", time.Now().UTC())); err != nil {
				return err
			}
			return tx.EnqueueOutbox(ctx, testRecord("two", time.Now().UTC()))
		})
		got, err := store.OutboxRecords(ctx)
		if err != nil || got != 2 {
			t.Fatalf("OutboxRecords() = (%d, %v), want (2, nil)", got, err)
		}
	})

	t.Run("outbox resolution", func(t *testing.T) {
		store := openStore(t, newStore)
		ctx := context.Background()
		now := time.Now().UTC()
		for _, id := range []projection.OutputID{"delivered", "permanent", "retryable"} {
			record := testRecord(id, now)
			mustTx(t, store, func(tx application.Tx) error { return tx.EnqueueOutbox(ctx, record) })
		}
		mustTx(t, store, func(tx application.Tx) error {
			_, err := tx.ResolveOutbox(ctx, []outbox.Result{
				{OutputID: "delivered", Disposition: outbox.Delivered},
				{OutputID: "permanent", Disposition: outbox.Permanent, Err: errors.New("bad document"), RejectionType: "mapper_parsing_exception"},
				{OutputID: "retryable", Disposition: outbox.Retryable, Err: errors.New("try again")},
			})
			return err
		})
		mustTx(t, store, func(tx application.Tx) error {
			got, err := tx.PendingOutbox(ctx, now.Add(time.Hour), 10)
			if err != nil {
				return err
			}
			if len(got) != 1 || got[0].OutputID != "retryable" || got[0].Attempts != 1 || got[0].LastError != "try again" {
				t.Fatalf("PendingOutbox after resolution = %+v, want incremented retryable only", got)
			}
			return nil
		})
		deadLetters, err := store.DeadLetterRecords(ctx)
		if err != nil || deadLetters != 1 {
			t.Fatalf("DeadLetterRecords after resolution = (%d, %v), want (1, nil)", deadLetters, err)
		}
		summary, err := store.DeadLetters(ctx)
		if err != nil || summary.Records != 1 || summary.Reasons["mapper_parsing_exception"] != 1 || summary.Indices["flows"] != 1 {
			t.Fatalf("DeadLetters after resolution = (%+v, %v)", summary, err)
		}
	})

	t.Run("dead-letter inspection and replay", func(t *testing.T) {
		store := openStore(t, newStore)
		ctx := context.Background()
		now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
		for i, id := range []projection.OutputID{"one", "two"} {
			record := testRecord(id, now.Add(time.Duration(i)*time.Second))
			record.Document = []byte(`{"secret":"payload-value"}`)
			mustTx(t, store, func(tx application.Tx) error { return tx.EnqueueOutbox(ctx, record) })
		}
		mustTx(t, store, func(tx application.Tx) error {
			_, err := tx.ResolveOutbox(ctx, []outbox.Result{
				{OutputID: "one", Disposition: outbox.Permanent, Attempts: 3, RejectionType: "mapping", DeadLetteredAt: now.Add(time.Minute)},
				{OutputID: "two", Disposition: outbox.Permanent, Attempts: 2, RejectionType: "mapping", DeadLetteredAt: now.Add(2 * time.Minute)},
			})
			return err
		})

		page, err := store.ListDeadLetters(ctx, outbox.DeadLetterFilter{Limit: 1}, "")
		if err != nil || len(page.Records) != 1 || page.Records[0].OutputID != "one" || page.Records[0].ByteSize == 0 || page.NextCursor != "one" {
			t.Fatalf("ListDeadLetters() = (%+v, %v), want first metadata page", page, err)
		}
		if !reflect.DeepEqual(page.Records[0], (outbox.Record{OutputID: "one", Index: "flows", Document: []byte(`{"secret":"payload-value"}`), CreatedAt: now, DeadLetteredAt: now.Add(time.Minute), Attempts: 3, RejectionType: "mapping"}).Metadata()) {
			t.Fatalf("dead-letter metadata = %+v, want persisted fields", page.Records[0])
		}
		fetched, found, err := store.DeadLetter(ctx, "one")
		if err != nil || !found || string(fetched.Document) != `{"secret":"payload-value"}` {
			t.Fatalf("DeadLetter() = (%+v, %v, %v), want body", fetched, found, err)
		}

		replayAt := now.Add(3 * time.Minute)
		mustTx(t, store, func(tx application.Tx) error {
			replayed, change, err := tx.ReplayDeadLetters(ctx, outbox.DeadLetterFilter{ReasonType: "mapping", Limit: 1}, replayAt)
			if err != nil || len(replayed) != 1 || replayed[0].OutputID != "one" || change.Records != 1 {
				t.Fatalf("ReplayDeadLetters() = (%+v, %+v, %v), want one replay and one retained", replayed, change, err)
			}
			return nil
		})
		mustTx(t, store, func(tx application.Tx) error {
			pending, err := tx.PendingOutbox(ctx, replayAt, 10)
			if err != nil || len(pending) != 1 || pending[0].OutputID != "one" || pending[0].Attempts != 0 || pending[0].ReplayCount != 1 || !pending[0].NextAttemptAt.Equal(replayAt) {
				t.Fatalf("PendingOutbox after replay = (%+v, %v)", pending, err)
			}
			_, err = tx.ResolveOutbox(ctx, []outbox.Result{{OutputID: "one", Disposition: outbox.Permanent, Attempts: 1, RejectionType: "mapping", DeadLetteredAt: replayAt.Add(time.Minute)}})
			return err
		})
		fetched, found, err = store.DeadLetter(ctx, "one")
		if err != nil || !found || fetched.ReplayCount != 1 || fetched.Attempts != 1 {
			t.Fatalf("rejected replay = (%+v, %v, %v), want replay count preserved", fetched, found, err)
		}
	})

	t.Run("delete removes flow and expiry", func(t *testing.T) {
		store := openStore(t, newStore)
		ctx := context.Background()
		f, _ := simpleFlow(t, "delete", time.Now().UTC().Add(-time.Hour), time.Second)
		mustTx(t, store, func(tx application.Tx) error {
			if err := tx.SaveFlow(ctx, f); err != nil {
				return err
			}
			return tx.DeleteFlow(ctx, f.Key())
		})
		mustTx(t, store, func(tx application.Tx) error {
			if _, found, err := tx.LoadFlow(ctx, f.Key()); err != nil || found {
				t.Fatalf("LoadFlow after delete = found %v, err %v", found, err)
			}
			got, err := tx.DueFlows(ctx, time.Now().UTC(), 10)
			if err != nil || len(got) != 0 {
				t.Fatalf("DueFlows after delete = %v, err %v", got, err)
			}
			return nil
		})
	})
}

func openStore(t *testing.T, factory func(t *testing.T) application.StateStore) application.StateStore {
	t.Helper()
	store := factory(t)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() = %v", err)
		}
	})
	return store
}

func mustTx(t *testing.T, store application.StateStore, fn func(application.Tx) error) {
	t.Helper()
	if err := store.WithTx(context.Background(), fn); err != nil {
		t.Fatal(err)
	}
}

func testRecord(id projection.OutputID, now time.Time) outbox.Record {
	return outbox.Record{OutputID: id, Index: "flows", Document: []byte(`{"ok":true}`), CreatedAt: now}
}

func simpleFlow(t *testing.T, key string, observed time.Time, timeout time.Duration) (*flow.Flow, rule.Rule) {
	t.Helper()
	r := rule.Rule{ID: "rule", Version: "3", Lifecycle: rule.Lifecycle{Timeout: timeout}}
	e := event.Event{Doc: map[string]any{"value": key}, ObservedAt: observed}
	f := flow.Open(flow.Key{RuleID: r.ID, CorrelationKey: key}, r, e)
	f.Apply(e, r, observed)
	return f, r
}

func completedSettleFlow(t *testing.T, key string, now time.Time) (*flow.Flow, rule.Rule) {
	t.Helper()
	eventType, _ := path.Compile("$.event")
	group, _ := path.Compile("$.id")
	r := rule.Rule{ID: "rule", Version: "3", Extract: rule.Extract{EventType: eventType}, Stitch: []rule.Stitch{{ID: "one", GroupBy: []path.Path{group}, Roles: []rule.Role{{Name: "member", Types: []string{"member"}}}, Requires: []string{"member"}}}, Lifecycle: rule.Lifecycle{Timeout: time.Minute, CloseWhen: rule.CloseAllInvocationsComplete, Settle: time.Second}}
	e := event.Event{Doc: map[string]any{"event": "member", "id": key}, ObservedAt: now}
	f := flow.Open(flow.Key{RuleID: r.ID, CorrelationKey: key}, r, e)
	f.Apply(e, r, now)
	return f, r
}

func richFlow(t *testing.T) (*flow.Flow, rule.Rule) {
	t.Helper()
	now := time.Date(2026, 8, 21, 11, 0, 0, 0, time.UTC)
	eventType, _ := path.Compile("$.event")
	group, _ := path.Compile("$.id")
	r := rule.Rule{ID: "rule", Version: "3", Extract: rule.Extract{EventType: eventType}, Stitch: []rule.Stitch{{ID: "call", GroupBy: []path.Path{group}, Roles: []rule.Role{{Name: "request", Types: []string{"request"}}, {Name: "response", Types: []string{"response"}}}, Requires: []string{"request", "response"}}}, Lifecycle: rule.Lifecycle{Timeout: time.Minute, CloseWhen: rule.CloseAllInvocationsComplete, Settle: time.Second}, Output: rule.Output{Index: "flows"}}
	first := event.Event{Doc: map[string]any{"event": "request", "id": "rich"}, ObservedAt: now}
	f := flow.Open(flow.Key{RuleID: r.ID, CorrelationKey: "rich"}, r, first)
	f.Apply(first, r, now)
	f.Apply(first, r, now.Add(time.Millisecond))
	f.Apply(event.Event{Doc: map[string]any{"event": "request", "id": "rich", "different": true}, ObservedAt: now.Add(2 * time.Millisecond)}, r, now.Add(2*time.Millisecond))
	f.Apply(event.Event{Doc: map[string]any{"event": "response", "id": "rich"}, ObservedAt: now.Add(3 * time.Millisecond)}, r, now.Add(3*time.Millisecond))
	if _, close := f.ShouldClose(r, now.Add(3*time.Millisecond)); close {
		t.Fatal("unexpected immediate close")
	}
	f.Apply(event.Event{Doc: map[string]any{"event": "request", "id": "incomplete"}, ObservedAt: now.Add(4 * time.Millisecond)}, r, now.Add(4*time.Millisecond))
	if f.DuplicateCount() != 1 || len(f.Anomalies()) != 1 || f.IncompleteInvocations() != 1 {
		t.Fatalf("rich fixture = duplicates %d, anomalies %d, incomplete %d; want 1 each", f.DuplicateCount(), len(f.Anomalies()), f.IncompleteInvocations())
	}
	return f, r
}

func assertEquivalentFlow(t *testing.T, got, want *flow.Flow, configured rule.Rule) {
	t.Helper()
	if got.Key() != want.Key() || got.RuleVersion() != want.RuleVersion() || !got.ExpiresAt().Equal(want.ExpiresAt()) || !got.FirstObservedAt().Equal(want.FirstObservedAt()) || got.DuplicateCount() != want.DuplicateCount() || !reflect.DeepEqual(got.Duplicates(), want.Duplicates()) || !reflect.DeepEqual(got.Anomalies(), want.Anomalies()) || len(got.Events()) != len(want.Events()) || got.IncompleteInvocations() != want.IncompleteInvocations() {
		t.Fatalf("loaded flow does not preserve aggregate fields\ngot: %+v\nwant: %+v", got.Finalize(flow.ReasonTimeout, time.Time{}), want.Finalize(flow.ReasonTimeout, time.Time{}))
	}
	finalizedAt := time.Date(2026, 8, 21, 13, 0, 0, 0, time.UTC)
	gotDoc, err := projection.Project(got.Finalize(flow.ReasonTimeout, finalizedAt), configured)
	if err != nil {
		t.Fatal(err)
	}
	wantDoc, err := projection.Project(want.Finalize(flow.ReasonTimeout, finalizedAt), configured)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotDoc, wantDoc) {
		t.Fatalf("loaded projection = %#v, want %#v", gotDoc, wantDoc)
	}
}
