# Architecture decision records

One file per decision, numbered, immutable once accepted. A decision that turns out to
be wrong gets a new ADR that supersedes the old one — the old file stays, because the
reasoning that led there is part of the history.

Use [0000-template.md](0000-template.md).

## Accepted

| ADR | Decision |
|---|---|
| [0001](0001-implementation-language.md) | Go is the implementation language |
| [0002](0002-hexagonal-architecture.md) | Hexagonal layout with a single bounded context |
| [0003](0003-correlation-contract.md) | The correlation contract: time-boxed flows, no tombstones, two-write finalization, acknowledgement that is never deferred |
| [0004](0004-event-model-and-path-language.md) | Producer logs are not reshaped; every value is a configured path |
| [0005](0005-stitching-and-flow-closing.md) | Stitch rules merge events sharing an identity; flows close when no invocation is waiting |
| [0006](0006-durable-state-store.md) | Pebble as the durable store, with a conformance suite that makes the engine swappable |
| [0007](0007-metrics-and-label-cardinality.md) | Metric labels come only from config or fixed enums, never from producer data |
| [0008](0008-opensearch-index-shape.md) | Plain-object entries, flat-object payloads, daily indices, bulk delivery with classified failures |
| [0009](0009-field-promotion.md) | Named fields lifted from events into `fields` with declared types; template generated from config |
| [0010](0010-passthrough.md) | Unmatched events are forwarded verbatim through a memory buffer, never the durable store |
| [0011](0011-configuration-reload.md) | SIGHUP reloads rules; open flows finish under the version they started with |
| [0012](0012-dead-letter-retention.md) | The dead-letter store is capped by count, dropping oldest |
| [0013](0013-self-reported-alerts.md) | Stuck documents are reported into their own OpenSearch index, rate-limited and loop-proof |
| [0014](0014-no-backward-compatibility-by-default.md) | Break formats freely; raise risks instead of writing migrations |
| [0015](0015-admin-surface.md) | Authenticated admin HTTP surface, absent unless a token is configured |

## Not yet decided

Each open question becomes an ADR when it is settled. Code that depends on one carries a
`TODO(contracts)` comment naming the question.

| Question | Blocks |
|---|---|
| Is single-node durable deployment enough for the first real environment? | scale-out design |
| Which queries and dashboards must the output schema optimize for? | additional field promotion and whether a summary block is needed |
| What exact acknowledgement contract should the Fluent Bit batch endpoint use? | partial-batch endpoint semantics |
| At what disk and state thresholds must ingestion stop? | capacity checks |
| What throughput and latency target defines release success? | performance acceptance criteria |

Redaction is out of scope by decision. Stripping secrets or personal data from logs is not
this service's job. Producer payloads reach OpenSearch as written, so redaction belongs at
the producer or in the forwarder.
