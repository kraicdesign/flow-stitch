# ADR-0002: Hexagonal layout with a single bounded context

- **Status:** accepted
- **Date:** 2026-08-20
## Context

One proposed Go-idiomatic layout used technical slices under `internal/` (`ingest`,
`correlation`, `state`, `sinks`…), while the logical design described separate components
and required the core to stay transport- and sink-independent. Those shapes are not the
same thing: technical slices do not, on their own, say where the boundary between policy
and I/O runs.

The decisions that will hurt most if they leak are the input, correlation, durability and
output contracts. They must be expressible without reference to HTTP,
OpenSearch or the storage engine, because all three are expected to change: the ingress
may gain protocols beyond HTTP, sinks may multiply, and the durable store may change.

## Decision

The repository uses a hexagonal (ports and adapters) layout with one bounded context:

- `internal/domain/` — pure. Event envelope, rules, the flow aggregate, projection,
  outbox records. No I/O, no clock, no framework types.
- `internal/application/` — use cases (`ingest`, `expire`, `deliver`) and the driven
  ports they declare in `ports.go`.
- `internal/adapters/` — HTTP ingress, config, rule registry, state stores, sinks,
  quarantine. Each implements a port and depends inward only.
- `cmd/flowstitch/` — the composition root, the single place that knows which adapter
  implements which port.

Correlation and delivery are *not* split into separate bounded contexts. They share the
flow lifecycle and the finalization transaction, and separating them would mean
coordinating an aggregate across a context boundary for no current benefit.

## Consequences

- The storage decision stays deferrable: `application.StateStore` and `application.Tx`
  are the contract, and the memory store already proves the port is implementable.
- Domain tests need no fakes for HTTP or OpenSearch. Correlation acceptance examples run
  against the aggregate directly.
- Producer quirks are absorbed at the edge. `flow_id` arriving as a top-level key
  instead of an envelope field is handled in `httpapi`, not in the domain.
- Cost: more indirection than idiomatic Go usually carries, and a port change touches
  several files.
- Cost: `internal/application/ports.go` becomes a chokepoint that every adapter reads.
  If it grows past comfortable review size, that is the signal to reconsider splitting
  contexts — not before.

## Alternatives considered

- **Technical slices** — most idiomatic Go and least ceremony, but this
  leaves the policy/I·O boundary implicit at exactly the moment the four contracts are
  being fixed.
- **Modular DDD by subdomain** (`correlation/`, `delivery/`, `ingest/`, each with its own
  domain and adapters) — better if correlation and delivery ever become separate
  deployables. Premature for the current requirements.
