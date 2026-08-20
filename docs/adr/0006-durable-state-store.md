# ADR-0006: Pebble as the durable state store

- **Status:** accepted
- **Date:** 2026-08-21

## Context

Correlation state must survive a restart. Delivery, capacity limits and recovery all assume
a durable store.

Two load points, given by the architect:

- **Operating load: ~300 events/s.** With a 30s timeout that is roughly 9,000 events and a couple of
  thousand flows in flight. Every candidate engine handles this without tuning.
- **Design ceiling: 50k events/s**, which must be reachable by changing the engine rather
  than the architecture. At a 30s timeout that is ~1.5M events and a few hundred thousand
  open flows.

Event sizes are mixed: mostly small metadata lines, with occasional large payload dumps.

## Decision

### 1. Pebble

An LSM engine, pure Go, single dependency, no cgo — the static binary stays static.

It is chosen over the simpler option specifically because of the 50k/s requirement.
"Switch engines later" reads like replacing one adapter, and structurally it is, but it also
means re-proving crash behaviour, re-tuning, and either migrating open flows or draining and
cutting over. An engine that covers both load points satisfies the readiness requirement by
never being exercised.

At 300/s Pebble runs on defaults. Its tuning surface only becomes work at the load that
would have forced the migration anyway.

### 2. Key layout

Four families, prefix-separated so every scan is bounded:

```text
f/<rule>/<correlation-key>            open flow, encoded
x/<expires-at-millis-BE>/<rule>/<key> expiry index, empty value
o/<output-id>                         outbox record
d/<output-id>                         dead letter
```

Expiry timestamps are big-endian fixed-width so Pebble's byte order is time order and
"which flows are due" is a bounded forward scan, not a table scan.

**The expiry index must be rewritten whenever a deadline moves.** The settle window shortens
`expires_at` on a flow that is already stored, so saving a flow deletes its previous index
entry and writes the new one in the same batch. A stale index entry means a flow finalizes
at the wrong moment or twice.

### 3. Encoding is versioned, and unknown versions fail closed

The aggregate's serialized form needs a version marker because a restart after a code change
reads flows written by the previous binary.

A stored value is a one-byte format version followed by a JSON body. JSON because a flow
holds arbitrary producer documents as `map[string]any` — a format requiring concrete types
would fight the entire event model. Pebble compresses blocks, which absorbs most of the size
cost.

Reading an unrecognised version is a hard failure, not a skip. Silently discarding flows the
current binary cannot parse would lose data exactly when someone is mid-rollback.

### 4. Writes are synced

Events are acknowledged after their flow-state write, so that write has to survive a crash;
an unsynced batch would make the acknowledgement a promise the process cannot keep.

Batches sync by default. At 300/s that costs nothing. At 50k/s concurrent writers share WAL
syncs through group commit, which is Pebble's normal operating mode. A configuration switch
can relax it, and the documentation must say plainly what is being traded away.

### 5. The port is proven by a conformance suite, not by inspection

The real deliverable behind "easy to switch engines" is a test suite that any
`application.StateStore` must pass: transactional behaviour, expiry ordering, outbox
idempotency, recovery with flows already overdue, and behaviour at the boundaries.

Both the memory store and Pebble run it. A future engine is a new adapter plus a green
suite — a measurement rather than an argument. Without it, "the port is clean" is a claim
nobody can check.

### 6. Mixed event sizes make `max_event_bytes` load-bearing

Occasional large values in an LSM engine drive write amplification through compaction. The
per-event byte limit already in the rule schema is the mitigation, and its default should be
chosen deliberately rather than inherited from an example file.

If payload dumps routinely exceed it, the answer is truncation at the ingress or storing
large bodies outside the flow — not a larger limit.

## Consequences

- Open flows survive restarts. Flows already past their deadline finalize on startup before
  ingestion resumes, so timeouts stay correct across a crash.
- Pebble's indexed batches are atomic, so finalization's two writes land together. The code
  must still keep them ordered — outbox before delete — because that ordering is what lets a
  future engine without multi-key atomicity be correct too.
- A corrupt or unopenable store fails startup. Never start with a silently empty store.
- Compaction competes with ingestion for disk. At 50k/s that becomes a tuning exercise; at
  300/s it is invisible.
- Recovery time grows with the number of open flows, and belongs in the conformance suite as
  a measured property rather than an assumption.

## Alternatives considered

- **bbolt** — memory-mapped B+tree, no tuning, extremely stable, and ideal at 300/s. Rejected
  on the single-writer limit: it is a hard ceiling rather than a gradual slowdown, so the
  50k/s requirement would definitely mean replacing it.
- **SQLite** — SQL for admin queries and debugging stuck flows is a genuine operational
  advantage. Rejected on throughput headroom, and because cgo complicates static builds while
  the pure-Go driver gives up speed.
- **Postgres or Redis** — externalises state and enables multi-node ownership, at the cost of
  the single-binary deployment. Revisit only if high availability becomes a requirement.
