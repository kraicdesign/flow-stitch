# ADR-0013: Self-reported alerts for stuck documents

- **Status:** accepted
- **Date:** 2026-08-21
- **Relates to:** [0012](0012-dead-letter-retention.md), [0007](0007-metrics-and-label-cardinality.md)

## Context

Documents that OpenSearch permanently refuses are dead-lettered, capped, and eventually
dropped. A Prometheus metric alone requires someone to have built alerting on a counter they
have never seen fire.

The people who would act on it are already looking at OpenSearch. Telling them there is a
problem, in the place they are already looking, needs no extra infrastructure.

## Decision

### 1. FlowStitch writes a diagnostic document when documents are stuck

A small, plainly-shaped document announcing the condition: how many records are held, how many
have been dropped, the breakdown by rejection reason, and the affected index.

Two conditions qualify:

- dead-lettered records exist;
- the outbox backlog is older than a configured threshold.

Both mean the same thing operationally — data that should be in OpenSearch is not.

### 2. Alerts go to their own index, with their own mapping

```yaml
alerts:
  enabled: true
  index: flowstitch-alerts-{yyyy}.{MM}.{dd}
  min_interval: 5m
  outbox_age_threshold: 5m
```

This is the load-bearing part. The most likely reason a flow document was rejected is a
mapping conflict **in the flows index** — so an alert written to that same index would be
rejected by the same fault, and the warning would be lost exactly when it matters.

The alert index has a fixed, engine-owned mapping and contains **no producer data at all**.
There is nothing in it that a producer can influence and therefore nothing that can make it
unindexable.

### 3. Alerts are rate-limited, not per-document

Emit on transition into a stuck state, then at most once per `min_interval` while it lasts,
and once when it clears. A thousand rejected documents produce one alert, not a thousand.

### 4. An alert can never trigger an alert

If an alert document is itself rejected, it is logged and discarded. It is never
dead-lettered, never counted toward the depth that produces alerts, and never retried
indefinitely.

Without this the failure mode is a service that responds to a full disk by writing more
documents about the full disk.

### 5. Rejection reasons are recorded, not labelled

The reason type — `mapper_parsing_exception` and friends — goes in the alert document, where
a breakdown is useful and cardinality costs nothing.

It does **not** become a metric label. That vocabulary belongs to OpenSearch rather than to
us, and ADR-0007's rule is that label values come only from our own code or from
configuration. The metrics stay `flowstitch_dlq_records` and `flowstitch_dlq_dropped_total`.

Only the error *type* is recorded, never OpenSearch's `reason` string, which quotes the
offending field value and could echo a secret.

### 6. Alert emission has one bounded metric

`flowstitch_alerts_emitted_total{kind}` counts diagnostic attempts. `kind` is the fixed
engine enum `dlq` or `outbox_backlog`; rejection types remain in documents and never become
Prometheus labels (ADR-0007).

## Consequences

- A stuck pipeline is visible in Dashboards without any alerting stack.
- One more index to create, with a template generated alongside the others.
- Alerts are best-effort by design: if OpenSearch is entirely unreachable, no alert can be
  written, and the metrics remain the only signal. Alerting complements monitoring rather
  than replacing it.
- The alert says what is stuck and why. Recovery remains a separate replay operation.

## Alternatives considered

- **Metrics only** — no new machinery, and it requires alerting rules that nobody writes for a
  counter they have never seen fire.
- **Alerts into the flows index** — one less index, and unwritable in the most common failure
  it exists to report.
- **A document per rejection** — precise, and a rejection storm becomes a write storm against
  the cluster that is already refusing writes.
