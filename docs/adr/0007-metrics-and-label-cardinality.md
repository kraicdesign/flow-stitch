# ADR-0007: Metrics and label cardinality

- **Status:** accepted
- **Date:** 2026-08-21

## Context

The metric collectors exist but nothing writes to them. Instrumenting the correlation path
raises two questions that are expensive to get wrong, because dashboards and alerts come to
depend on both.

The first is cardinality. The metric set inherited from the original design included
`flowstitch_events_received_total{rule,type,source}`, where `type` comes from a producer
document via `extract.event_type`. A producer is free to emit any string there. One service
emitting a per-request type — `http.request.GET./users/8412` — creates a new time series per
request and takes Prometheus down. `source` no longer exists as a concept at all: after
ADR-0004 there is no privileged source field, only whatever path a rule happens to read.

The second is layering. Domain and application must not import Prometheus (ADR-0002).

## Decision

### 1. A label value may only come from configuration or from a fixed set in our code

Never from a producer document.

Allowed: `rule` (bounded by config), and enums the engine defines — `reason`, `limit`,
`result`, `sink`.

Removed: `type` and `source`.

The rule is deliberately absolute rather than case-by-case. "This particular producer field
is low cardinality" is a claim about someone else's code that stays true until it doesn't,
and the failure mode is an outage in the monitoring system you would be using to diagnose it.

The breakdown that `type` would have given is not lost — it is in the documents, where
`flow.duplicates[]` and the entries themselves carry event types, and where OpenSearch can
aggregate over them. Metrics answer *is something wrong, and where*. Documents answer
*what exactly*.

### 2. One finalization counter, not three

`flowstitch_flows_finalized_total{rule,reason}` replaces the separate completed, timed-out
and limit-finalized counters. `reason` is the same bounded enum the output document carries,
so a single series answers all three questions and any future reason arrives without a new
metric.

### 3. Gauges are process-maintained and seeded at startup

`flows_open` is state, not an event. Counting it in the store means a scan on every scrape,
which an LSM engine cannot do cheaply.

Instead the process maintains it — incremented when a flow opens, decremented when it
finalizes — and seeds it at startup with a single count from the store. The seed is what
keeps a restart honest; without it the gauge would silently under-report every flow recovered
from disk.

`outbox_pending` and `outbox_oldest_seconds` follow the same pattern, refreshed by the
delivery loop rather than by the scrape.

### 4. Instrumentation enters through a port

`application.Recorder` is an interface declared where it is consumed, with a no-op
implementation for tests and a Prometheus adapter in `internal/observability/metrics`. No
Prometheus import ever appears in `internal/domain` or `internal/application`.

The port takes domain values — a rule ID, a reason, a duration. It does not take label maps,
because a `map[string]string` parameter is how producer-derived labels would find their way
back in.

### 5. Every metric has a stated purpose

The set is small and each one answers a question an operator actually asks:

| Metric | Question |
|---|---|
| `events_received_total{rule}` | Is data arriving, and for which rules? |
| `events_rejected_total{reason}` | Is something being turned away, and why? |
| `events_duplicate_total{rule}` | Is a forwarder replaying? |
| `flows_open{rule}` | Is state building up? |
| `flows_finalized_total{rule,reason}` | How do flows end — and is the timeout share rising? |
| `flow_age_seconds{rule}` | How long do flows actually take, versus the configured timeout? |
| `incomplete_invocations_total{rule}` | Are calls going unanswered? |
| `outbox_pending` / `outbox_oldest_seconds` | Is delivery keeping up? |
| `sink_requests_total{sink,result}` | Is OpenSearch accepting documents? |
| `ingest_latency_seconds` / `finalize_latency_seconds` | Where is time being spent? |

A metric that answers no question does not get added. `state_bytes` stays because disk
exhaustion stops acknowledgements, which is the one failure that silently drops data.

The pairing that matters most: a rising `flows_finalized_total{reason="timeout"}` share
together with a rising `incomplete_invocations_total` is the signal that a timeout is set too
short — the operating loop of noticing one logical operation split across two documents,
made visible before anyone reads a document.

## Consequences

- Cardinality is bounded by configuration, so a new producer can never affect it.
- Losing the `type` breakdown means a service that stops responding shows up a timeout later,
  in `flows_finalized_total{reason="timeout"}`, rather than immediately in a per-type receive
  rate.
- The recorder port makes metrics testable: assertions on recorded calls, with no scraping.
- Adding a label later is easy; removing one breaks every dashboard built on it. Starting
  narrow is the reversible direction.

## Alternatives considered

- **`type` restricted to values declared in the rule's stitch roles**, everything else
  collapsing to `other`. Genuinely bounded, since those strings are config, and it would give
  a leading indicator instead of a lagging one. Rejected for v1 as too clever: a label whose
  values depend on unrelated parts of rule config is hard to explain and easy to misread.
  This is the natural extension if a leading indicator is wanted later.
- **An explicit allow-list of event types for labelling.** Same benefit, at the cost of new
  configuration that has to be maintained in step with producers.
- **Counting open flows in the store at scrape time.** Exact, and a full scan per scrape on an
  LSM engine.
