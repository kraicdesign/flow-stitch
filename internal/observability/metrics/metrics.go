// Package metrics implements application instrumentation with Prometheus.
package metrics

import (
	"time"

	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var flowAgeBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 30, 60, 120, 300, 600}

// Metrics holds every collector and implements [application.Recorder]. Labels
// come only from configuration or engine-owned enums (ADR-0007).
type Metrics struct {
	Registry *prometheus.Registry

	EventsReceived  *prometheus.CounterVec
	EventsRejected  *prometheus.CounterVec
	EventsDuplicate *prometheus.CounterVec

	FlowsOpen             *prometheus.GaugeVec
	FlowsFinalized        *prometheus.CounterVec
	FlowAge               *prometheus.HistogramVec
	IncompleteInvocations *prometheus.CounterVec

	StateBytes          prometheus.Gauge
	OutboxPending       prometheus.Gauge
	OutboxOldestSeconds prometheus.Gauge
	DLQRecords          prometheus.Gauge
	DLQDropped          prometheus.Counter
	DLQReplayed         prometheus.Counter

	SinkRequests              *prometheus.CounterVec
	SinkRetries               *prometheus.CounterVec
	PassthroughEventsCounter  prometheus.Counter
	PassthroughDepthGauge     prometheus.Gauge
	PassthroughDroppedCounter prometheus.Counter
	ConfigReloads             *prometheus.CounterVec
	ConfigLoadedTimestamp     prometheus.Gauge
	AlertsEmitted             *prometheus.CounterVec

	IngestLatencySeconds   prometheus.Histogram
	FinalizeLatencySeconds prometheus.Histogram
}

// New builds and registers the bounded-cardinality metric set from ADR-0007.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		EventsReceived: counterVec(reg, "flowstitch_events_received_total",
			"Events accepted for correlation.", "rule"),
		EventsRejected: counterVec(reg, "flowstitch_events_rejected_total",
			"Events refused or quarantined.", "reason"),
		EventsDuplicate: counterVec(reg, "flowstitch_events_duplicate_total",
			"Events already present in their flow.", "rule"),
		FlowsOpen: gaugeVec(reg, "flowstitch_flows_open",
			"Flows currently waiting for more events.", "rule"),
		FlowsFinalized: counterVec(reg, "flowstitch_flows_finalized_total",
			"Flows finalized by reason.", "rule", "reason"),
		FlowAge: histogramVec(reg, "flowstitch_flow_age_seconds",
			"Age of flows at finalization.", flowAgeBuckets, "rule"),
		IncompleteInvocations: counterVec(reg, "flowstitch_incomplete_invocations_total",
			"Incomplete invocations observed when flows finalize.", "rule"),
		StateBytes: gauge(reg, "flowstitch_state_bytes",
			"Bytes held by the durable state store."),
		OutboxPending: gauge(reg, "flowstitch_outbox_pending",
			"Finalized documents waiting for the sink."),
		OutboxOldestSeconds: gauge(reg, "flowstitch_outbox_oldest_seconds",
			"Age of the oldest undelivered document."),
		DLQRecords: gauge(reg, "flowstitch_dlq_records",
			"Permanent rejections retained in the dead-letter store."),
		DLQDropped: counter(reg, "flowstitch_dlq_dropped_total",
			"Dead-letter records dropped because the retention cap was exceeded."),
		DLQReplayed: counter(reg, "flowstitch_dlq_replayed_total",
			"Dead-letter records moved back to the outbox for delivery."),
		SinkRequests: counterVec(reg, "flowstitch_sink_requests_total",
			"Sink delivery attempts by result.", "sink", "result"),
		SinkRetries: counterVec(reg, "flowstitch_sink_retry_total",
			"Sink retries by reason.", "sink", "reason"),
		PassthroughEventsCounter: counter(reg, "flowstitch_passthrough_events_total",
			"Unmatched events accepted into the pass-through buffer."),
		PassthroughDepthGauge: gauge(reg, "flowstitch_passthrough_buffer",
			"Unmatched events currently held in the pass-through buffer."),
		PassthroughDroppedCounter: counter(reg, "flowstitch_passthrough_dropped_total",
			"Unmatched events dropped because pass-through is disabled or delivery permanently rejected them."),
		ConfigReloads: counterVec(reg, "flowstitch_config_reloads_total",
			"Configuration reload attempts by result.", "result"),
		ConfigLoadedTimestamp: gauge(reg, "flowstitch_config_loaded_timestamp_seconds",
			"Unix timestamp when the active configuration was loaded."),
		AlertsEmitted: counterVec(reg, "flowstitch_alerts_emitted_total",
			"Best-effort diagnostic alerts emitted by kind.", "kind"),
		IngestLatencySeconds: histogram(reg, "flowstitch_ingest_latency_seconds",
			"Time to durably accept one event.", prometheus.DefBuckets),
		FinalizeLatencySeconds: histogram(reg, "flowstitch_finalize_latency_seconds",
			"Time to finalize a flow and enqueue its document.", prometheus.DefBuckets),
	}
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// EventReceived records one event accepted for correlation.
func (m *Metrics) EventReceived(ruleID rule.ID) {
	m.EventsReceived.WithLabelValues(string(ruleID)).Inc()
}

// EventRejected records one permanently rejected ingress event.
func (m *Metrics) EventRejected(reason string) { m.EventsRejected.WithLabelValues(reason).Inc() }

// EventDuplicate records one identical repeat of an occupied stitch slot.
func (m *Metrics) EventDuplicate(ruleID rule.ID) {
	m.EventsDuplicate.WithLabelValues(string(ruleID)).Inc()
}

// FlowOpened increments the current-flow gauge for a configured rule.
func (m *Metrics) FlowOpened(ruleID rule.ID) { m.FlowsOpen.WithLabelValues(string(ruleID)).Inc() }

// FlowFinalized records the reason, age, and missing members of a closed flow.
func (m *Metrics) FlowFinalized(ruleID rule.ID, reason flow.Reason, age time.Duration, incomplete int) {
	m.FlowsOpen.WithLabelValues(string(ruleID)).Dec()
	m.FlowsFinalized.WithLabelValues(string(ruleID), string(reason)).Inc()
	m.FlowAge.WithLabelValues(string(ruleID)).Observe(age.Seconds())
	if incomplete > 0 {
		m.IncompleteInvocations.WithLabelValues(string(ruleID)).Add(float64(incomplete))
	}
}

// OutboxDepth publishes current delivery backlog size and maximum age.
func (m *Metrics) OutboxDepth(pending int, oldest time.Duration) {
	m.OutboxPending.Set(float64(pending))
	seconds := oldest.Seconds()
	if seconds < 0 {
		seconds = 0
	}
	m.OutboxOldestSeconds.Set(seconds)
}

// DeadLetterDepth publishes retained and retention-evicted dead-letter counts.
func (m *Metrics) DeadLetterDepth(records, dropped int) {
	m.DLQRecords.Set(float64(records))
	if dropped > 0 {
		m.DLQDropped.Add(float64(dropped))
	}
}

// SinkAttempt records one downstream delivery verdict.
func (m *Metrics) SinkAttempt(sink, result string) {
	m.SinkRequests.WithLabelValues(sink, result).Inc()
}

// PassthroughEvent records one unmatched event accepted into memory.
func (m *Metrics) PassthroughEvent() { m.PassthroughEventsCounter.Inc() }

// PassthroughBuffer publishes the current unmatched-event buffer depth.
func (m *Metrics) PassthroughBuffer(depth int) { m.PassthroughDepthGauge.Set(float64(depth)) }

// PassthroughDropped records one permanent pass-through loss.
func (m *Metrics) PassthroughDropped() { m.PassthroughDroppedCounter.Inc() }

// IngestLatency observes the time needed to durably accept one event.
func (m *Metrics) IngestLatency(d time.Duration) { m.IngestLatencySeconds.Observe(d.Seconds()) }

// FinalizeLatency observes the time needed to enqueue and close one flow.
func (m *Metrics) FinalizeLatency(d time.Duration) {
	m.FinalizeLatencySeconds.Observe(d.Seconds())
}

// ConfigLoaded publishes when the active configuration took effect.
func (m *Metrics) ConfigLoaded(at time.Time) { m.ConfigLoadedTimestamp.Set(float64(at.Unix())) }

// ConfigReload records one successful or rejected reload attempt.
func (m *Metrics) ConfigReload(result string) {
	m.ConfigReloads.WithLabelValues(result).Inc()
}

// AlertEmitted records one best-effort diagnostic attempt by bounded kind.
func (m *Metrics) AlertEmitted(kind string) { m.AlertsEmitted.WithLabelValues(kind).Inc() }

// DeadLetterReplayed records documents returned to the delivery outbox.
func (m *Metrics) DeadLetterReplayed(records int) {
	if records > 0 {
		m.DLQReplayed.Add(float64(records))
	}
}

// SeedDeadLetters initializes retained depth and the durable cumulative drop count.
func (m *Metrics) SeedDeadLetters(summary outbox.DeadLetterSummary) {
	m.DLQRecords.Set(float64(summary.Records))
	if summary.Dropped > 0 {
		m.DLQDropped.Add(float64(summary.Dropped))
	}
}

// SeedOpenFlows resets and seeds the process-maintained gauge from durable state.
func (m *Metrics) SeedOpenFlows(counts map[rule.Reference]int) {
	m.FlowsOpen.Reset()
	aggregated := make(map[rule.ID]int)
	for reference, count := range counts {
		aggregated[reference.ID] += count
	}
	for ruleID, count := range aggregated {
		m.FlowsOpen.WithLabelValues(string(ruleID)).Set(float64(count))
	}
}

func counterVec(reg prometheus.Registerer, name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	reg.MustRegister(c)
	return c
}

func gaugeVec(reg prometheus.Registerer, name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	reg.MustRegister(g)
	return g
}

func gauge(reg prometheus.Registerer, name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	reg.MustRegister(g)
	return g
}

func counter(reg prometheus.Registerer, name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
	reg.MustRegister(c)
	return c
}

func histogramVec(reg prometheus.Registerer, name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
	reg.MustRegister(h)
	return h
}

func histogram(reg prometheus.Registerer, name, help string, buckets []float64) prometheus.Histogram {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets})
	reg.MustRegister(h)
	return h
}

var _ application.Recorder = (*Metrics)(nil)
