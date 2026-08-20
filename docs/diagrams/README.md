# Diagrams

Standalone Mermaid sources. GoLand renders `.mmd` files directly with its bundled Mermaid
support (Preview tab); GitHub and VS Code render them too.

Kept as separate files rather than fenced blocks inside a document so the IDE opens them as
diagrams rather than as text.

| File | What it shows |
|---|---|
| [event-journey.mmd](event-journey.mmd) | One event from Fluent Bit to OpenSearch, including the pass-through branch and the independent delivery loop |
| [flow-decisions.mmd](flow-decisions.mmd) | Every decision made when an event joins a flow: stitch role, invocation slot, duplicate, anomaly, close |
| [flow-lifecycle.mmd](flow-lifecycle.mmd) | A flow's states, all four ways it can close, and what happens to its document afterwards |
| [state-layout.mmd](state-layout.mmd) | What lives on disk, what lives in memory, and what a restart costs |

These are a map, not a contract. Behaviour is defined in [the specification](../specification.md)
and the reasoning in [the ADRs](../adr/README.md) — where a diagram disagrees with either,
the diagram is the thing that is wrong.

Two details worth reading off them:

The `202` is returned once the ingest transaction commits. It never waits for a flow to close
or for OpenSearch to accept anything.

The pass-through buffer is deliberately not on disk. Correlated flows are a small fraction of
traffic and worth a durable write each; ordinary log lines at full volume are not. Durability
for that path lives in the forwarder's own disk buffer, reached through backpressure when the
buffer fills.
