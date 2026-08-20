# FlowStitch specification

What the system is and how it behaves. This is the current description — where an older
document disagrees, this one wins.

Decisions and the reasoning behind them live in [adr/](adr/README.md). This document says
*what*; the ADRs say *why*.

## 1. Purpose

Applications emit logs that are technically independent but describe one operation. A user
action that crosses three services produces six or more records, and reconstructing the
operation means joining them at query time — every time anyone looks.

FlowStitch moves that reconstruction into the ingestion path. It groups events that share a
correlation ID, merges the ones that describe the same call, waits until the operation is
finished or its time runs out, and writes **one document** to OpenSearch.

Producers keep emitting simple logs. Nothing about them has to change.

## 2. Concepts

| Term | Meaning |
|---|---|
| Event | One log record, exactly as the producer wrote it, plus the time we received it. |
| Correlation key | The value that says which flow an event belongs to. A configured path. |
| Flow | Every event sharing a correlation key, collected over a bounded window. |
| Stitch rule | Configuration that says which events describe the same call. |
| Invocation | One instance of a stitch rule — typically one request and its response. |
| Entry | One item in the output document: a merged invocation, or a single unmerged event. |
| Outbox | Durable queue of finished documents waiting to reach OpenSearch. |

## 3. Events

An event is the producer's document, untouched, plus `observed_at` from our clock. Fields
are never renamed, moved or restructured, and the whole document is preserved into the
output.

Nothing in an event is mandatory except that the correlation key resolves. There is no
required ID, no required type, no required timestamp. An event that cannot supply the values
a feature needs simply does not get that feature — it is never rejected for the omission.

`observed_at` is ours because scheduling must not depend on a producer's clock. Producer
timestamps are used for durations and display, where they are the meaningful number.

## 3.1 Pass-through for unmatched events

When pass-through is enabled, an event that matches no correlation rule is forwarded to
OpenSearch exactly as the producer sent it. Enabling this mode puts FlowStitch in the path of
all logging, rather than only the traffic it can correlate.

Pass-through events use a fixed-size in-memory buffer and never enter the durable state store
or outbox. When that buffer is full, ingestion returns a retryable failure; durability for the
backed-up traffic lives in Fluent Bit's disk buffer. A process crash can therefore lose the
events currently held in memory, matching the loss window of the direct Fluent Bit to
OpenSearch pipeline.

OpenSearch assigns pass-through document IDs. If a delivery succeeds but its acknowledgement
is lost, retrying can produce a duplicate log line. FlowStitch does not content-hash these
documents because two byte-identical producer lines may be two genuine events (ADR-0010).

The pass-through index supports the same date placeholders as correlated output indices. An
optional timestamp path selects the date; an absent, unresolvable, or invalid timestamp falls
back to the current time.

## 4. Paths

Everything the engine reads from an event is a configured path.

```yaml
$.flow_id                    # top level
$.context.invocation_id      # nested, any depth
$.attempts[0].status         # array index
$.attempts[-1].status        # from the end
$.payload["http.status"]     # quoted: any field name, dots included
$.payload["0"]               # quoted digits are a key, not an index
```

Rules:

- A path starts with `$.`. A bare string elsewhere in the config is a literal value, never a
  path.
- Segments split on `.`. Bare digits in brackets are indices; quoted digits are keys.
- `\"` and `\\` escape inside quotes, so no field name is unreachable.
- A path resolves to **at most one value**. There are no wildcards, no filters and no
  recursive descent — a correlation key that could match twice would not be a key.
- A path that does not resolve yields nothing. That is not an error; only the correlation
  key treats absence as fatal.
- Numbers coerce without exponent or trailing zeros (`12345`), booleans as `true`/`false`.
  Objects and arrays resolve to nothing rather than being stringified.

Finding an element inside an object array — "the header whose name is X-Id" — is a filter,
and deliberately out of scope. Flatten it at the producer or the ingress.

## 5. Rules

A rule owns one shape of log. Different producers with different field names coexist as
separate rules.

```yaml
rules:
  - id: application-flow
    enabled: true

    extract:
      event_type: $.event          # whatever the producer calls it
      timestamp:  $.datetime

    correlation:
      key: $.flow_id

    stitch:
      - id: api-invocation
        group_by: [$.service, $.context.invocation_id]
        roles:
          request:  http.request
          response: [http.response, http.error]
        requires: [request, response]

    lifecycle:
      timeout: 30s
      close_when: all_invocations_complete
      settle: 0s
      terminal_events: [flow.completed, flow.failed]

    limits:
      max_events: 64
      max_event_bytes: 65536
      max_flow_bytes: 1048576

    output:
      index: application-flows
      timestamp: first_event.timestamp
```

**Rule selection**: the first rule whose correlation key resolves claims the event. Rule
order is config order. Two enabled rules may not share a correlation key path — the config
is rejected at boot if they do.

Configuration is fully validated before ingestion starts. A malformed path, an unknown key,
a `requires` entry naming a role that does not exist, or two stitch rules claiming the same
event type all fail the process rather than the first event that hits them.

### 5.1 Configuration reload

`SIGHUP` reads and fully validates the configured file, then atomically publishes its rules.
Events that open flows after publication use the new rules. Open flows keep the immutable
rule definition they started with, including lifecycle, stitching, promotion, limits and
output settings, until they finalize (ADR-0011).

Rules, pass-through settings and alert settings are reloadable, as is the log level. The listen address,
state driver, state path and state sync policy are boot-only. A reload that requests a
different boot-only value applies the reloadable changes and logs each ignored setting by
name.

If reading, parsing, path compilation, rule validation or a cross-rule check fails, the
running configuration remains active and the process continues. The failure is logged with
the configuration filename and validation problem.

## 6. Flow lifecycle

A flow **opens** on the first event carrying its correlation key.

It **accepts any event** carrying that key, of any type. There is no start condition and no
restriction to request/response shapes. An event that fills no role becomes a plain entry.

It **closes** on the first of these, in this order:

1. a terminal event arrives;
2. a limit is exceeded;
3. `close_when: all_invocations_complete` is satisfied — meaning **at least one invocation is
   complete and none are incomplete**. The first half of that condition is not optional: a
   flow that has seen no invocations would otherwise close on its first event;
4. the timeout expires.

`timeout` is a **total budget** measured from the first event's `observed_at`. Later events
never extend it, so no producer can hold a flow open indefinitely.

`settle` is a small margin, default zero. When the close condition first becomes true and
settle is set, the flow waits that long and re-checks — covering the case where an inner
request happened before an outer response but arrived after it, because the two services
ship through different forwarders.

Then it **finalizes**: project the document, write it to the outbox, delete the open flow.
Two writes, in that order. A crash between them leaves the flow open; it finalizes again
later and the deterministic document ID overwrites the first attempt.

There is no record of finished flows. An event arriving for a key with no open flow starts a
new one. Two documents sharing a correlation key is the intended outcome, not a failure —
they are both queryable, and seeing two is the signal to raise the timeout.

## 7. Stitching and duplicates

A stitch rule merges events that describe the same call.

`group_by` is the invocation's identity — the tuple of values at those paths. What goes in
that list decides what merges:

- `[$.service, $.invocation_id]` — each service merges its own view. One network hop becomes
  two entries, and the gap between them is network and queue time.
- `[$.invocation_id]` — all four log lines of a hop merge into one entry, if the ID crosses
  the wire and both sides log it.

The last path segment names each field in the output `group` object. Collisions receive a
numeric suffix in `group_by` order (`id`, `id2`), so reordering paths is a mapping change;
append paths when extending an established rule.

`roles` maps a name we control to the producer type(s) that fill it. Role names become the
keys in the output document, so producer strings never become field names and renaming an
event type does not reshape the document.

`requires` names the roles that must be present for the invocation to be complete. It
defaults to all of them; naming fewer allows optional members that enrich an entry without
holding the flow open.

**An event's identity is the slot it occupies**: `(flow, group tuple, role)`. That is what
replaces a producer-supplied event ID.

A second event arriving for a filled slot is compared with the one already there:

- identical content → a duplicate. `duplicate_count` increments, nothing is stored twice.
- different content → a cardinality anomaly. The event is kept as its own entry, and the
  anomaly is recorded.

Nothing is discarded either way. A complete invocation is never un-completed by later
arrivals.

**Unstitched events are never deduplicated.** Two identical log lines carrying only a
correlation key are two events, because that is usually what they are.

A retried call should carry a fresh invocation ID. Reusing one produces a cardinality
anomaly that the engine can flag but not untangle.

## 8. Output document

```json
{
  "@timestamp": "2026-08-20T12:00:00.000Z",
  "flow": {
    "id": "abc",
    "rule_id": "application-flow",
    "finalization_reason": "invocations_complete",
    "started_at": "...", "ended_at": "...",
    "duration_ms": 50,
    "event_count": 6,
    "entry_count": 4,
    "duplicate_count": 1,
    "duplicates": [{"type": "http.response", "count": 1}],
    "incomplete_invocations": 1
  },
  "fields": {"status": 500, "customer_id": "c-42"},
  "events": [
    {"group": {"service": "web", "invocation_id": "inv-1"}, "complete": true,
     "started_at": "...", "ended_at": "...", "duration_ms": 41,
     "request":  {"type": "http.request",  "timestamp": "...", "event": {}},
     "response": {"type": "http.response", "timestamp": "...", "event": {}}},

    {"group": {"service": "apm", "invocation_id": "inv-2"}, "complete": false,
     "request": {"type": "http.request", "timestamp": "...", "event": {}}},

    {"type": "log.message", "timestamp": "...", "event": {}}
  ],
  "anomalies": []
}
```

- `finalization_reason` is one of `invocations_complete`, `terminal_event`, `timeout`,
  `limit_exceeded`, or `rule_unavailable`. The last value means a recovered flow's exact rule
  version was absent after a configuration change; its durable rule snapshot was used to
  preserve and deliver the collected events rather than leak the flow.
- `events[]` holds merged and plain entries in one array, ordered by the arrival time of each
  entry's earliest member.
- `complete: false` marks an invocation that never got everything it needed. It is both the
  signal and the way to tell a merged entry from a plain one — `events.complete: false`
  finds every unanswered call across every flow with one query.
- `incomplete_invocations` is observed absence: *this call to this service never answered*.
  Nobody configures what a complete flow should look like.
- Both flow and invocation `duplicate_count` fields are always present, even at zero.
  `duplicates[]` is an array of
  `{type, count}` rather than an object keyed by type, so event type names never become
  index fields.
- `duration_ms` uses producer timestamps. Within one service the number is sound; a merge
  spanning two machines inherits their clock skew. A negative duration is emitted as-is with
  an anomaly rather than clamped — it is the clearest clock-skew alarm available.
- An incomplete invocation has `duration_ms: -1` and no `ended_at`; its `started_at` remains
  the timestamp of the first member that arrived.

Producer data stays inside bounded object fields. Arbitrary keys are never promoted to
mapped top-level fields.

### Field promotion

Promotion gives a small, configured set of producer values real OpenSearch types when
keyword-only flat-object payload search is insufficient:

```yaml
promote:
  status:      {path: $.payload.status, type: long, from: response}
  customer_id: {path: $.customer.id, type: keyword}
```

Promoted values land in the document's `fields` object. Names always come from configuration,
never producer data, and types are declared as `long`, `double`, `keyword`, `boolean`, or
`date`. Missing values are omitted. Values that cannot be coerced are also omitted and
recorded as anomalies so one malformed value never costs the flow document.

Changing promotion configuration requires regenerating and installing the index template
with `flowstitch print-index-template`. With daily indices, the new mapping applies naturally
to the next index; applying it sooner requires creating a new index or updating the deployment's
template and rollover procedure.

## 9. Delivery

Finished documents go to a durable outbox before anything is sent. Correlation and delivery
are decoupled: an OpenSearch outage grows disk-backed backlog instead of destroying
correlation state.

The document ID is `sha256(rule_id + ":" + correlation_key + ":" + first_event_observed_at)`.
It is deterministic, so re-indexing after an ambiguous success overwrites rather than
duplicating — exactly-once *effect* for the finished document without pretending to
exactly-once transport. The start time is what keeps a second flow for the same key from
overwriting the first.

The sink writes in bulk with retry and backoff. Failures are classified: transient ones stay
in the outbox, permanent ones (mapping conflicts, validation rejections) go to a dead-letter
path so one bad document cannot block the queue. `cluster_block_exception` is always
retryable, including when a manual write block returns HTTP 403. Dead-letter records retain
the OpenSearch error type but never its reason string, which may quote producer data.

### 9.1 Stuck-document alerts

When enabled, FlowStitch writes an engine-owned diagnostic document to a dedicated alert
index when either dead-letter records exist or the oldest outbox record exceeds the configured
age threshold (ADR-0013):

```yaml
alerts:
  enabled: true
  index: flowstitch-alerts-{yyyy}.{MM}.{dd}
  min_interval: 5m
  outbox_age_threshold: 5m
```

An alert contains its timestamp, `dlq` or `outbox_backlog` kind, starting/ongoing/clearing
state, retained and dropped counts, reason types as an array of `{type, count}`, affected
target indices, oldest outbox age, and a one-line summary. It contains no producer fields or
payloads and uses a fixed mapping with `dynamic: false`.

Each condition emits once when it starts, no more than once per `min_interval` while it
persists, and once when it clears. Alert delivery is best-effort. A rejected alert is logged
and discarded; it never enters the outbox or dead-letter store and therefore cannot produce
an alert loop.

### 9.2 Dead-letter inspection and replay

Administration uses the existing HTTP listener and is absent unless
`server.admin_token_env` names an environment variable containing a non-empty bearer token.
The token value is read only from the environment. Every registered admin route requires it,
and authentication comparisons are constant-time (ADR-0015).

`GET /v1/admin/dlq` returns a summary and paged record metadata. Metadata includes output ID,
target index, rejection type, byte size, creation and rejection timestamps, attempts and replay
count, but never the document body. `GET /v1/admin/dlq/{output_id}` is the only operation that
returns a body, for one explicitly named record.

`POST /v1/admin/dlq/replay` selects by output ID, reason type, index and rejection age, with a
mandatory bounded limit applied by the server. Broad filters default to a dry run. A real replay
atomically moves each match back to the outbox under its original output ID and index, resets its
delivery attempts, makes it immediately ready and increments a persisted replay counter. The
counter survives another permanent rejection. Replaying cannot repair the rejection's cause;
operators fix the mapping or producer first, then replay.

### 9.3 Durable storage

FlowStitch stores open flows, their expiry index, the outbox and dead-letter records in
Pebble v2. Upgrading from a build that used Pebble v1 may raise the store's on-disk format
major version. That change is one-way: an older FlowStitch build may then be unable to open
the store.

Before upgrading, let open flows finalize, stop FlowStitch, replace the binary or image, and
then start it again (ADR-0014). If the remaining state is disposable, discard the state
directory instead. Do not attempt a rolling downgrade after Pebble v2 has opened the store.

## 10. Acknowledgement and backpressure

The ingress processes a batch and returns. The response follows the flow-state write.

What is ruled out is *deferring* the acknowledgement — it never waits for a flow to finish,
and never waits for OpenSearch. Both would hold a forwarder's connection open for the length
of a flow.

Under overload the progression is: the sink slows, the outbox grows, warning metrics fire,
and past the high-water mark the ingress starts returning retryable failures so the forwarder
buffers instead. Queues are bounded at every boundary; overload is never hidden in memory.

Malformed input and events matching no rule are quarantined, not retried forever, and the
forwarder is told not to retry them.

## 11. Limits

Per rule: `max_events`, `max_event_bytes`, `max_flow_bytes`. Globally: maximum open flows,
state bytes, outbox depth.

A flow reaching a limit finalizes with reason `limit_exceeded`, and the next event carrying
that key opens a fresh flow. A runaway correlation key becomes several bounded documents
that share an ID — never one unbounded document, and never a silent drop.

## 12. Metrics

Metric labels come only from configuration or fixed engine values, never from producer
documents (ADR-0007). The metric set is intentionally small:

| Metric | Question it answers |
|---|---|
| `flowstitch_events_received_total{rule}` | Is data arriving, and for which rules? |
| `flowstitch_events_rejected_total{reason}` | Is something being turned away, and why? |
| `flowstitch_events_duplicate_total{rule}` | Is a forwarder replaying? |
| `flowstitch_flows_open{rule}` | Is correlation state building up? |
| `flowstitch_flows_finalized_total{rule,reason}` | How do flows end, and is the timeout share rising? |
| `flowstitch_flow_age_seconds{rule}` | How long do flows live versus their configured timeout? |
| `flowstitch_incomplete_invocations_total{rule}` | Are observed calls going unanswered? |
| `flowstitch_state_bytes` | Is durable state approaching the point where acknowledgements must stop? |
| `flowstitch_outbox_pending` / `flowstitch_outbox_oldest_seconds` | Is delivery keeping up? |
| `flowstitch_dlq_records` / `flowstitch_dlq_dropped_total` | Are permanent rejections accumulating or overflowing retention? |
| `flowstitch_sink_requests_total{sink,result}` | Is the sink accepting documents? |
| `flowstitch_sink_retry_total{sink,reason}` | Why are sink requests being retried? |
| `flowstitch_ingest_latency_seconds` / `flowstitch_finalize_latency_seconds` | Where is correlation-path time being spent? |
| `flowstitch_config_reloads_total{result}` | Are reload attempts succeeding or failing? |
| `flowstitch_config_loaded_timestamp_seconds` | When did the active configuration last take effect? |
| `flowstitch_alerts_emitted_total{kind}` | Are stuck-document diagnostics being emitted, and for which engine-owned condition? |

The key operating signal is a rising share of
`flowstitch_flows_finalized_total{reason="timeout"}` together with rising
`flowstitch_incomplete_invocations_total`: that pairing means the configured timeout is
probably too short.

## 13. Failure behaviour

| Situation | Result |
|---|---|
| Crash with open flows | Flows survive in the durable store. Those past their deadline finalize on startup, before ingestion resumes. |
| Crash mid-finalization | The flow is still open. It finalizes again; the deterministic ID overwrites. |
| OpenSearch accepted but the ack was lost | Retry reuses the same ID. Still one document. |
| OpenSearch outage | Outbox grows on disk. Backlog age and depth are exposed as metrics. |
| Corrupt state store | Fail closed. Never start with a silently empty store. |
| Disk full | Stop acknowledging before durability can no longer be honoured; readiness fails. |
| Invalid configuration on SIGHUP | Keep the running configuration, count and log the failed reload, and continue serving. |
| Alert document rejected | Log and discard it. Never retry or dead-letter an alert. |

Health distinguishes *alive* from *safe to accept events*. A degraded sink is not
unreadiness while output can still be buffered; exhausted capacity is.

## 14. Deliberately not included

- Tracing. Trace IDs can be correlation keys, but tracing instrumentation is never required.
- A message broker. One may come later for partitioned scale-out; nothing needs it now.
- A completion expression language. Stitching expresses completion better.
- Producer-supplied event IDs. The stitch slot is the identity.
- Reopening finished flows, or correction documents.
- A user interface.
- Distributed cluster mode.
