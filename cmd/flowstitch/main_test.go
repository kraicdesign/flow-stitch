package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/adapters/state/memory"
	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
)

func TestStoreCapacityRejectsOnlyWhenOutboxLimitIsExhausted(t *testing.T) {
	store := memory.New()
	capacity := storeCapacity{store: store, maxOpenFlows: 100, maxOutboxRecords: 2}
	if ok, reason := capacity.AcceptingEvents(context.Background()); !ok || reason != "" {
		t.Fatalf("empty capacity = (%v, %q), want accepting", ok, reason)
	}
	if err := store.WithTx(context.Background(), func(tx application.Tx) error {
		if err := tx.EnqueueOutbox(context.Background(), outbox.Record{OutputID: "one", CreatedAt: time.Now()}); err != nil {
			return err
		}
		return tx.EnqueueOutbox(context.Background(), outbox.Record{OutputID: "two", CreatedAt: time.Now()})
	}); err != nil {
		t.Fatal(err)
	}
	if ok, reason := capacity.AcceptingEvents(context.Background()); ok || !strings.Contains(reason, "outbox record limit") {
		t.Fatalf("full capacity = (%v, %q), want outbox refusal", ok, reason)
	}
}

func TestPrintIndexTemplateEmitsStableValidJSONForExample(t *testing.T) {
	configPath := filepath.Join("..", "..", "config", "flowstitch.example.yaml")
	var first, second bytes.Buffer
	args := []string{"-config", configPath}
	if err := printIndexTemplate(args, &first); err != nil {
		t.Fatal(err)
	}
	if err := printIndexTemplate(args, &second); err != nil {
		t.Fatal(err)
	}
	if first.String() != second.String() {
		t.Fatal("print-index-template output changed across runs")
	}
	var output any
	if err := json.Unmarshal(first.Bytes(), &output); err != nil {
		t.Fatalf("output is invalid JSON: %v\n%s", err, first.String())
	}
	if !strings.Contains(first.String(), `"status"`) || !strings.Contains(first.String(), `"long"`) {
		t.Fatalf("output omits example promotion: %s", first.String())
	}
	if !strings.Contains(first.String(), `"flowstitch-alerts-{yyyy}.{MM}.{dd}"`) || !strings.Contains(first.String(), `"oldest_outbox_age_seconds"`) {
		t.Fatalf("output omits alerts template: %s", first.String())
	}
}
