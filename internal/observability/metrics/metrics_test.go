package metrics_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	adapterrules "github.com/kraicdesign/flow-stitch/internal/adapters/rules"
	"github.com/kraicdesign/flow-stitch/internal/adapters/state/memory"
	pebblestate "github.com/kraicdesign/flow-stitch/internal/adapters/state/pebble"
	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/application/ingest"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/path"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
	"github.com/kraicdesign/flow-stitch/internal/observability/metrics"
	dto "github.com/prometheus/client_model/go"
)

type metricClock struct{ now time.Time }

func (c metricClock) Now() time.Time { return c.now }

type metricCapacity struct{}

func (metricCapacity) AcceptingEvents(context.Context) (bool, string) { return true, "" }

type metricQuarantine struct{}

func (metricQuarantine) CaptureEvent(context.Context, event.Event, string) error    { return nil }
func (metricQuarantine) CaptureRecord(context.Context, outbox.Record, string) error { return nil }

// ADR-0007: producer values must never become Prometheus label values.
func TestProducerValuesCannotBecomeMetricLabels(t *testing.T) {
	uniqueType := fmt.Sprintf("producer-controlled-%d", rand.Uint64())
	key, err := path.Compile("$.flow_id")
	if err != nil {
		t.Fatal(err)
	}
	eventType, err := path.Compile("$.event")
	if err != nil {
		t.Fatal(err)
	}
	configured := rule.Rule{ID: "bounded-rule", Version: "1", Enabled: true, Key: key, Extract: rule.Extract{EventType: eventType}, Lifecycle: rule.Lifecycle{Timeout: time.Minute}, Output: rule.Output{Index: "flows"}}
	m := metrics.New()
	svc := ingest.New(memory.New(), adapterrules.NewRegistry([]rule.Rule{configured}), metricQuarantine{}, metricCapacity{}, metricClock{time.Now().UTC()}, m)
	_, err = svc.Accept(context.Background(), event.Event{Doc: map[string]any{"flow_id": "one", "event": uniqueType}, ObservedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetValue() == uniqueType {
					t.Fatalf("metric %q contains producer label value %q", family.GetName(), uniqueType)
				}
			}
		}
	}
	family := findFamily(t, families, "flowstitch_events_received_total")
	if len(family.Metric) != 1 || len(family.Metric[0].Label) != 1 || family.Metric[0].Label[0].GetName() != "rule" {
		t.Fatalf("events_received labels = %+v, want only rule", family.Metric)
	}
}

func TestRecorderTracksFinalizationAndIncompleteInvocations(t *testing.T) {
	m := metrics.New()
	m.FlowOpened("rule")
	m.FlowFinalized("rule", flow.ReasonTimeout, 30*time.Second, 2)
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	open := findFamily(t, families, "flowstitch_flows_open")
	if got := open.Metric[0].GetGauge().GetValue(); got != 0 {
		t.Fatalf("flows_open = %v, want 0", got)
	}
	finalized := findFamily(t, families, "flowstitch_flows_finalized_total")
	if got := finalized.Metric[0].GetCounter().GetValue(); got != 1 || labelValue(finalized.Metric[0], "reason") != "timeout" {
		t.Fatalf("flows_finalized = %+v, want timeout=1", finalized.Metric)
	}
	incomplete := findFamily(t, families, "flowstitch_incomplete_invocations_total")
	if got := incomplete.Metric[0].GetCounter().GetValue(); got != 2 {
		t.Fatalf("incomplete_invocations = %v, want 2", got)
	}
}

func TestRecorderTracksDeadLetterDepthAndDrops(t *testing.T) {
	m := metrics.New()
	m.DeadLetterDepth(7, 2)
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if got := findFamily(t, families, "flowstitch_dlq_records").Metric[0].GetGauge().GetValue(); got != 7 {
		t.Fatalf("dlq_records = %v, want 7", got)
	}
	if got := findFamily(t, families, "flowstitch_dlq_dropped_total").Metric[0].GetCounter().GetValue(); got != 2 {
		t.Fatalf("dlq_dropped_total = %v, want 2", got)
	}
}

func TestRecorderTracksAlertsOnlyByEngineKind(t *testing.T) {
	m := metrics.New()
	m.AlertEmitted("dlq")
	m.AlertEmitted("outbox_backlog")
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	family := findFamily(t, families, "flowstitch_alerts_emitted_total")
	if len(family.Metric) != 2 {
		t.Fatalf("alerts metrics = %+v, want two kinds", family.Metric)
	}
	for _, metric := range family.Metric {
		if len(metric.Label) != 1 || metric.Label[0].GetName() != "kind" {
			t.Fatalf("alert labels = %+v, want only kind", metric.Label)
		}
	}
}

func TestRecorderTracksPassthroughWithoutLabels(t *testing.T) {
	m := metrics.New()
	m.PassthroughEvent()
	m.PassthroughBuffer(7)
	m.PassthroughDropped()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]float64{
		"flowstitch_passthrough_events_total":  1,
		"flowstitch_passthrough_buffer":        7,
		"flowstitch_passthrough_dropped_total": 1,
	} {
		family := findFamily(t, families, name)
		if len(family.Metric) != 1 || len(family.Metric[0].Label) != 0 {
			t.Fatalf("%s metrics = %+v, want one unlabeled metric", name, family.Metric)
		}
		got := family.Metric[0].GetCounter().GetValue()
		if family.GetType() == dto.MetricType_GAUGE {
			got = family.Metric[0].GetGauge().GetValue()
		}
		if got != want {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
}

func TestPebbleRestartSeedsOpenFlowGaugePerRule(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	store, err := pebblestate.Open(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, item := range []struct {
		id  rule.ID
		key string
	}{{"alpha", "one"}, {"alpha", "two"}, {"beta", "three"}} {
		configured := rule.Rule{ID: item.id, Version: "1", Lifecycle: rule.Lifecycle{Timeout: time.Minute}}
		e := event.Event{Doc: map[string]any{"key": item.key}, ObservedAt: now}
		f := flow.Open(flow.Key{RuleID: item.id, CorrelationKey: item.key}, configured, e)
		f.Apply(e, configured, now)
		if err := store.WithTx(ctx, func(tx application.Tx) error { return tx.SaveFlow(ctx, f) }); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = pebblestate.Open(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	counts, err := store.OpenFlows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New()
	m.SeedOpenFlows(counts)
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	family := findFamily(t, families, "flowstitch_flows_open")
	got := make(map[string]float64)
	for _, metric := range family.Metric {
		got[labelValue(metric, "rule")] = metric.GetGauge().GetValue()
	}
	if got["alpha"] != 2 || got["beta"] != 1 {
		t.Fatalf("flows_open = %v, want alpha=2 beta=1", got)
	}
}

func findFamily(t *testing.T, families []*dto.MetricFamily, name string) *dto.MetricFamily {
	t.Helper()
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %q not found", name)
	return nil
}

func labelValue(metric *dto.Metric, name string) string {
	for _, label := range metric.Label {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}
