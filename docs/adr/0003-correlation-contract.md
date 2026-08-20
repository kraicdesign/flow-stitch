# ADR-0003: The correlation contract

- **Status:** accepted
- **Date:** 2026-08-20
## Context

The flow lifecycle needs one unambiguous contract for opening, timeout, finalization,
acknowledgement and limits. This ADR states the contract that the correlation core implements.

## Decision

### 1. A flow is a time-boxed batch

A flow opens on the first event carrying its correlation key and accepts **any** event
carrying that key, of any type, until it closes. There is no start condition, no completion
predicate, no declared expected set, and no restriction to request/response shapes.

`timeout` is a **total budget** measured from the first event's `observed_at`. It is never
extended by later events, so no producer can hold a flow open indefinitely. Observed time,
not producer time — scheduling must not depend on a clock we do not control.

Consequently `flow.status` is dropped from the output; it only ever restated
`finalization_reason`, whose values are `invocations_complete`, `terminal_event`,
`timeout` and `limit_exceeded`.

### 2. No tombstones

There is no record of finalized flows. An event arriving for a correlation key with no open
flow starts a new one, whether or not a flow for that key existed before.

Two batches for one key are the intended outcome, not a failure: they share the correlation
key, so a search in OpenSearch finds both. Seeing two is the operator's signal to raise the
timeout.

This removes an entire record family, the retention policy that governed it, the sweep that
reclaimed it, and the lookup-ordering invariant it required.

### 3. Finalization is two writes

```text
enqueue outbox → delete open flow
```

A crash between them leaves the flow open; it finalizes again later and the deterministic
document ID overwrites the first attempt. Idempotent, with no recovery logic to write.

The document ID is `sha256(rule_id + ":" + correlation_key + ":" + first_event_observed_at)`.
The start time is what stops a second batch from overwriting the first — without it both
hash identically and OpenSearch keeps only the last one, which would defeat the whole point
of independent batches sharing one key.

### 4. Acknowledgement is per batch and synchronous

The ingress processes a batch and returns. The `202` follows the flow-state write, because
the write is what the request was doing — not because the response is waiting on a
durability gate.

What this rules out is **deferring** the acknowledgement: the ack does not wait for the flow
to finalize, and it does not wait for OpenSearch. Those are the two things a forwarder must
never be held on.

An accepted event survives a crash given a durable store. Duplicate suppression holds within
a flow's lifetime, not absolutely, because no finalized-flow record remains.

Deferred acknowledgement was considered and rejected: Fluent Bit acknowledges per HTTP
request, so holding an ack until finalization means holding the request open for the flow's
timeout. That exceeds the forwarder's own timeout, blocks a batch on the slowest flow it
happens to contain, and pins a connection per in-flight batch. The transport that *can* do
deferred acknowledgement is a broker with offset commits, which is outside this design.

Durable state is what makes the surviving guarantee real. A restart without it loses every
open flow — orders of magnitude more data than any acknowledgement subtlety.

### 5. Limits shard a flow rather than truncating it

A flow that reaches `max_events` finalizes with reason `limit_exceeded`. The next event
carrying that key opens a fresh flow. A runaway correlation key becomes several bounded
documents that share an ID, never one unbounded document and never a silent drop.

## Consequences

- The correlation core is small: open, append, close on one of three conditions, project,
  enqueue. There is no state machine beyond that.
- Duplicate detection exists only inside an open flow, and only for events that occupy a
  stitch slot (see ADR-0005). A replay arriving after its flow closed becomes its own batch.
- Duplicate suppression applies within the flow's lifetime rather than absolutely.
- Recovery is one rule: flows past their deadline finalize on startup before ingestion
  resumes.

## Alternatives considered

- **Tombstones for late/duplicate detection** — an entire durable record family plus a TTL
  policy, to suppress an extra document that the operator can already see and act on.
- **Completion predicates over event counts** — a grammar, a compiler and an open question
  about which operators v1 needs, to express something the stitch structure expresses
  better (ADR-0005, section 3).
- **Durability before acknowledgement** — the stronger guarantee, rejected because
  the loss window it protects is negligible next to the open-flow state it does not.
