# ADR-0008: OpenSearch index shape and delivery

- **Status:** accepted
- **Date:** 2026-08-21

## Context

Documents reach the outbox and stop. Delivering them needs three decisions that are cheap
now and expensive after data exists: how the entries array is mapped, how producer payloads
are mapped, and what the target index is called.

Two hazards drive all three. Producer payloads have field names we do not control, and
OpenSearch stops indexing a document once an index exceeds its field limit. And OpenSearch
flattens arrays of objects by default, so a filter combining two facts about one array entry
silently matches across different entries.

## Decision

### 1. Entries are plain objects, not `nested`

`events[]` is mapped as ordinary objects. Each entry is not indexed as its own sub-document.

This fits the primary use: look a flow up by its correlation ID and read it. Retrieval is
unaffected by this choice — the document returned is identical either way — and the index
stays smaller and faster to write, with no nested query syntax to fight in Dashboards.

**The cost is real and must be documented where operators will see it.** A query combining
two facts about the same entry — "the call to `apm` was incomplete" — can match a document
where one entry is incomplete and a *different* entry went to `apm`. It returns rows without
an error. Flow-level questions are unaffected: `flow.incomplete_invocations > 0` is exact,
because it is a single value.

Reversible forward: because indices roll daily, switching to `nested` later is a
template change that applies to new indices. Only re-querying older data the new way would
require a reindex.

### 2. Producer payloads use OpenSearch `flat_object`

Everything the producer wrote — the `event` object inside each entry — is mapped as a single
`flat_object` field (the OpenSearch equivalent of Elasticsearch's `flattened` type).

Exact-value search still works (`status: 500` finds it), but individual payload keys are
never added to the mapping, so no producer can push the index past its field limit. The
limitation is that flat-object content is treated as keywords: no numeric ranges and no
full-text analysis inside payloads. Engine-computed values — durations, counts, reasons,
group values — are ordinary mapped fields and keep their types.

The index template sets `dynamic: false` at the top level. An unexpected field is stored and
returned but not indexed — never rejected. `strict` was rejected precisely because it turns a
surprising field into a dropped document, which is data loss in the name of tidiness.

### 3. Indices roll daily, and the date comes from the document

`output.index` accepts placeholders: `application-flows-{yyyy}.{MM}.{dd}`.

The date is taken from the document's own `@timestamp`, not from the wall clock at delivery,
so a document always lands in the index matching the time it describes. A flow with no usable
producer timestamp falls back to its finalization time.

Daily indices make retention cheap — deleting an index is nearly free, deleting documents is
not — and they make a future mapping change apply cleanly to new indices.

**Index resolution must be deterministic for a given flow.** A document ID is unique *within*
an index, so if a retry resolved a different index name, the same document would exist twice.
This is already structurally safe: the index is resolved once at finalization and stored on
the outbox record, and retries reuse the stored value. It must stay that way.

### 4. Delivery is bulk, with failures classified

The sink writes through the Bulk API using the deterministic document ID, so a retry after an
ambiguous success overwrites rather than duplicating.

Every bulk response is inspected per item, because a bulk request returns `200` while
individual items fail. Each failure is classified:

- **Retryable** — 429, 503, connection and timeout errors, and any 5xx. The record stays in
  the outbox with a backoff deadline.
- **Permanent** — mapping conflicts, parse failures, and documents rejected as too large.
  These go to the dead-letter path.

The distinction is what keeps one unmappable document from blocking every document behind it.
Retries back off exponentially with jitter, so a recovering cluster is not hit by a
synchronised wave from every FlowStitch instance at once.

### 5. A sink outage is not unreadiness

While finalized documents can still be buffered, FlowStitch is healthy: correlation continues
and the outbox grows on disk. Readiness fails only when durable capacity is genuinely
exhausted, because that is the point where accepting an event would be a promise we cannot
keep.

The signals that distinguish "slow" from "broken" are `outbox_oldest_seconds` and
`outbox_pending`.

## Consequences

- The mapping cannot explode, whatever producers emit.
- Cross-entry queries are a documented sharp edge rather than a solved problem. If they turn
  out to be common, `nested` is a template change on tomorrow's index.
- Retention becomes an index lifecycle policy rather than a delete-by-query job.
- The example ISM policy ships with the project, but is not applied automatically — cluster
  policy belongs to whoever runs the cluster.

## Alternatives considered

- **`nested` entries** — correct within-entry queries from day one, at the cost of storage,
  indexing speed and Dashboards friction, to fix a class of query nobody has needed yet.
- **Fully mapped payloads** — correct types and full-text search inside payloads, and the
  mapping explosion this project would then be famous for.
- **Payloads stored but not indexed** — the smallest index, giving up payload search
  entirely. A reasonable fallback if flat-object fields prove awkward in practice.
- **One fixed index with rollover by alias** — keeps shard sizes even under uneven traffic,
  at the cost of more moving parts and losing the "the index name tells you the day"
  property.
