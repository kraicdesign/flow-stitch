# ADR-0005: Stitching and flow closing

- **Status:** accepted
- **Date:** 2026-08-20
## Context

Grouping events into a flow says which logs belong together. It does not say how they
relate *inside* the flow. A multi-service call produces a request and a response per hop,
and a flow document that lists them as six unrelated entries answers "what happened" but
not "where did the time go".

The producers already carry what is needed: alongside the flow ID that spans the whole
operation, each individual call carries its own shared ID — an API invocation ID — logged
by both the request and its response. That is the flow ID trick one level down, and it
needs no parent pointers, no ordering assumptions and no heuristics.

## Decision

### 1. Stitch rules merge events that share an identity

```yaml
stitch:
  - id: api-invocation
    group_by: [$.service, $.context.invocation_id]
    roles:
      request:  http.request
      response: [http.response, http.error]
    requires: [request, response]        # default: every role
```

`group_by` is the invocation's composite identity — the tuple of values at those paths.
Which events merge is decided entirely by what goes in that list:

- `[$.service, $.invocation_id]` — each service merges its own view of a call. One hop
  becomes two entries, and the gap between them is network and queue time.
- `[$.invocation_id]` — all four log lines of a hop merge into one entry, if the invocation
  ID crosses the wire and both sides log the same value.

`roles` maps a name we control to the producer type(s) that fill it. `requires` names the
roles that must be present for the invocation to be complete, defaulting to all of them —
splitting the two allows optional members that enrich an entry without gating the flow.

An event missing any `group_by` value, or whose type fills no role, is not stitched. It
becomes a plain entry and never gates closing.

Two stitch rules may not claim the same event type; config validation rejects overlapping
`roles` at load, so no event has to choose between groups.

### 2. The stitch slot is the event's identity

An event's identity is `(flow, group_by tuple, role)` — the slot it occupies. This replaces
`event_id`, which is no longer required or used (ADR-0004, section 4).

A second event arriving for an occupied slot is either a re-delivery or a genuinely
different call reusing the invocation ID. Compare the content: a re-delivery is identical,
a real second call differs at least in its timestamp.

- identical → `duplicate_count` increments, nothing is appended;
- different → a cardinality anomaly, and the event stays in `events[]` as its own entry.

Nothing is dropped in either case. Once complete, an invocation is never un-completed by
later arrivals.

The honest limit: **unstitched events are never deduplicated.** Two identical log lines with
only a flow ID are two events, because that is usually what they are.

A retried call should get a fresh invocation ID at the producer. Reusing it produces a
cardinality anomaly the engine can flag but not untangle.

### 3. Flows close when nothing is waiting

```yaml
lifecycle:
  timeout: 30s
  close_when: all_invocations_complete
  settle: 0s
  terminal_events: [flow.completed, flow.failed]   # optional
```

`close_when: all_invocations_complete` finalizes as soon as every invocation the flow has
seen has all its required roles. For a plain request/response flow the document reaches
OpenSearch when the response lands, not 30 seconds later. Each new request adds an expected
response, so a fan-out to three services holds the flow open until all three answer — with
nobody configuring how many services there are.

Three rulings this needs:

- **An invocation missing its first member still gates.** A response for an invocation whose
  request never arrived is incomplete in the same way, and holds the flow open until
  timeout. It surfaces a service that logs responses but drops requests.
- **Vacuous truth does not close a flow.** The condition is *at least one complete
  invocation and none incomplete*. Without it, a flow that has seen no invocation IDs would
  satisfy "all complete" on its first event and close immediately. A flow whose events carry
  no invocation IDs closes on timeout or terminal event only.
- **`settle` is a margin for cross-source reordering, not the mechanism.** When the
  condition becomes true, finalization waits `settle` and re-checks. It covers the case
  where an inner request *happened* before an outer response but *arrived* after it,
  because the two services ship through different forwarders. Default `0`; set 100–500ms
  only if services ship independently and flows are observed splitting.

`terminal_events` stays as an optional third closer, for services that can say explicitly
that an operation is over. The timeout is always underneath all of them.

This replaces the completion-expression language entirely: no grammar, no compiler, no
operator set to define.

### 4. Output shape

`events[]` holds merged entries and plain entries in one array, ordered by the arrival time
of each entry's earliest member — stitched and unstitched interleave in one honest timeline.

```json
"flow": {
  "id": "abc",
  "finalization_reason": "invocations_complete",
  "duration_ms": 50,
  "event_count": 6,
  "entry_count": 4,
  "duplicate_count": 1,
  "duplicates": [{"type": "http.response", "count": 1}],
  "incomplete_invocations": 1
},
"events": [
  {"group": {"service": "web", "invocation_id": "inv-1"}, "complete": true,
   "started_at": "...", "ended_at": "...", "duration_ms": 41,
   "request":  {"type": "http.request",  "timestamp": "...", "event": {}},
   "response": {"type": "http.response", "timestamp": "...", "event": {}}},

  {"group": {"service": "apm", "invocation_id": "inv-2"}, "complete": false,
   "request": {"type": "http.request", "timestamp": "...", "event": {}}},

  {"type": "log.message", "timestamp": "...", "event": {}}
]
```

- Role names become document keys, so `events.response.event.status` is a flat, stable path
  a dashboard queries directly. Producer type strings never become field names — that is
  how a mapping explodes, and it means renaming `http.request` to `api.request`
  later does not move the document shape.
- `complete: false` is the discriminator *and* the signal. `events.complete: false` finds
  every hop that never answered, across every flow, with one term query. No `kind` field —
  it would restate structure that is already visible.
- `flow.duplicate_count` is always present, even at zero. `duplicates[]` is an array of
  `{type, count}` rather than an object keyed by type, because dynamic field names are how
  mappings explode. Per-entry `duplicate_count` is omitted when zero.
- `incomplete_invocations` replaces the declared `missing[]` that ADR-0003 removed, and
  improves on it: observed absence — *this call to this service never answered* — rather
  than a configured expectation that did not materialise.
- `duration_ms` is computed from producer timestamps, because that is the latency that
  actually happened. With `group_by` scoped to one service both timestamps come from one
  machine and the number is sound; a whole-hop merge spans two clocks and inherits their
  skew. A negative duration is emitted as-is with an anomaly rather than clamped — it is the
  clearest clock-skew alarm available.
- No `summary` block. With no privileged fields, anything in a summary is configuration, and
  no dashboard requirement justifies one. Field promotion serves configured query needs; adding
  a mapped field later is cheap, removing one from a live index is not.

## Consequences

- Per-hop latency comes from a shared ID rather than an inferred pairing, so it is exact or
  absent — never plausibly wrong.
- Deduplication and completion both fall out of the same structure, so neither needs its own
  mechanism.
- Stitching is optional. A rule with no `stitch` block still works: every event is a plain
  entry, no dedupe, closes on timeout.
- The engine must track invocation completeness incrementally, since `close_when` is
  evaluated on every append — it cannot be computed only at projection time.

## Alternatives considered

- **Pairing by `parent_event_id`** — requires producers to emit causal pointers, which is a
  bigger ask than propagating an ID they already propagate.
- **Heuristic pairing** by `(source, target)` and nearest-preceding request — needs no
  producer support, and mispairs exactly during retries and parallel fan-out, which is when
  someone is reading the document.
- **Member list instead of roles** (`{"members": [...]}`) — one less concept in config, at
  the cost of a nested query to read a status code, which is the plain-object mapping pain point in daily
  use.
- **Full graph reconstruction** — parent/child trees and critical-path analysis.
  Deferred; it needs the pairing to exist first.
