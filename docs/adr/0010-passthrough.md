# ADR-0010: Pass-through for unmatched events

- **Status:** accepted
- **Date:** 2026-08-21

## Context

An event matching no rule is currently counted and dropped. That is fine when FlowStitch
sits beside the pipeline and only receives logs worth correlating, but it is wrong for the
intended topology:

```text
Fluent Bit ──> FlowStitch ──> OpenSearch
```

Here FlowStitch is an inline hop. Anything it does not forward disappears. The alternative —
configuring the forwarder to route correlatable logs one way and everything else another —
means two sets of rules that must agree forever, and they drift silently.

FlowStitch should stitch what it can and pass everything else through untouched.

## Decision

### 1. Unmatched events are forwarded verbatim

Configured per deployment:

```yaml
passthrough:
  enabled: true
  index: logs-{yyyy}.{MM}.{dd}
  timestamp: $.datetime      # optional, selects the index date
```

The document is the producer's log line exactly as it arrived. Nothing is renamed, wrapped or
annotated — this service does not own other people's logs.

If `passthrough` is absent or disabled, unmatched events are counted and dropped as before.

### 2. Pass-through never touches the durable store

Events are held in a **bounded in-memory buffer** and batched to the sink. No Pebble write,
no outbox record.

Correlated flows are a small fraction of traffic and each is worth a durable write. Ordinary
log lines at tens of thousands per second are not: copying every one to disk on the way past
would make total log volume, rather than correlation, the dominant load on the node.

### 3. Durability lives in the forwarder

When the buffer fills, the ingress returns a retryable failure. Fluent Bit then holds the
events in **its own disk buffer** and retries — which is durability we do not have to build,
in a component that already has it.

The loss window is whatever sits in memory when the process dies: a second or two of logs.
That is exactly what a direct Fluent Bit → OpenSearch pipeline already risks, so this is not
a regression; it is the same trade, made explicit.

### 4. Pass-through documents have no deterministic ID

Correlated documents get an ID derived from rule, key and start time, so a retry after an
ambiguous success overwrites. A pass-through log line has no identity — there is no event ID
by design — so OpenSearch assigns one, and a retry can produce a second copy.

A content hash was considered and rejected: two genuinely identical log lines are two events,
and collapsing them would be worse than an occasional duplicate. This matches a direct
Fluent Bit to OpenSearch pipeline.

### 5. Correlated documents are never starved

Both paths share the sink. Delivery drains the outbox first, then the pass-through buffer, so
a flood of ordinary logs cannot delay the flow documents that took real work to produce.

### 6. The two paths are measured separately

`flowstitch_passthrough_events_total`, `flowstitch_passthrough_buffer` and
`flowstitch_passthrough_dropped_total` sit alongside the correlation metrics. Without the
split, total ingest volume tells you nothing about whether correlation is healthy.

## Consequences

- FlowStitch becomes part of the log path for everything, so its availability now affects all
  logging rather than only correlation. Backpressure and forwarder retry are what keep that
  from being data loss.
- The throughput target is no longer "correlatable events" but total log volume — tens of
  thousands per second rather than hundreds. The buffer and batch sizes are the knobs.
- Sizing questions change: memory now matters as much as disk.
- A misconfigured rule no longer means lost logs, only unstitched ones. That is a meaningful
  safety improvement in its own right.

## Alternatives considered

- **Durable outbox for pass-through** — nothing lost once accepted, at the price of a disk
  write per log line and disk sizing driven by total logging.
- **Synchronous forward**, holding the HTTP response until OpenSearch accepts — no loss and no
  buffer, but ingest latency then includes a cluster round trip, and a partial batch failure
  makes the forwarder resend correlated events whose flows may already have closed, producing
  duplicate flow documents.
- **Routing in Fluent Bit** — keeps FlowStitch off the critical path entirely, at the cost of
  two rule sets that must be kept in agreement by hand.
