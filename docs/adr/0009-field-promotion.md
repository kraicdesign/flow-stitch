# ADR-0009: Field promotion

- **Status:** accepted
- **Date:** 2026-08-21
- **Relates to:** [0008](0008-opensearch-index-shape.md), which made payloads `flat_object`

## Context

ADR-0008 maps producer payloads as `flat_object` so no producer can push the index past its
field limit. The cost is that flat-object content is keyword-only: `status >= 500`,
`duration_ms > 200`, date math and numeric aggregations are all unavailable inside payloads.

Those are ordinary questions to ask of logs. What is missing is a way to name a small number
of fields that matter and give them real types — without reopening the door that `flat_object`
closed.

A fixed `summary` would encode deployment-specific query needs in the engine. Promotion is
the mechanism for naming those needs in configuration instead.

## Decision

### 1. Promotion is declared per rule

```yaml
promote:
  status:      { path: $.payload.status, type: long,    from: response }
  customer_id: { path: $.customer.id,    type: keyword }
  region:      { path: $.attributes.region, type: keyword }
```

Field **names come from configuration**, never from producer documents. This is the same rule
that governs metric labels (ADR-0007, section 1): producer *values* are safe, producer-controlled
*names* are not. Promotion cannot cause a mapping explosion because the set of names is fixed
before ingestion starts.

### 2. Promoted fields land in `fields`, separate from engine metadata

```json
"flow":   { "duration_ms": 50, "finalization_reason": "invocations_complete" },
"fields": { "status": 500, "customer_id": "c-42" },
"events": [ … ]
```

`flow.*` is ours and stable across deployments. `fields.*` is the operator's and changes with
their config. Merging them would make it impossible to tell, a year later, which fields are
guaranteed and which exist because someone once added a line of YAML.

### 3. Flow level only

One value per field per document.

Entry-level promotion was considered and rejected for now: entries are mapped as plain
objects (ADR-0008, section 1), so a promoted field on an entry could not be reliably combined with
that entry's other fields in a query. It would offer precision the index cannot honour.

Every promoted field being single-valued means every filter on `fields.*` is exact. That is
worth more than per-hop detail that queries silently get wrong.

`from` selects which event supplies the value: a stitch role name, `first`, or `last`.
Default is the first event in arrival order whose path resolves.

### 4. Types are declared, and bad values never cost a document

`type` is written down: `long`, `double`, `keyword`, `boolean`, `date`.

Inference was rejected. If one service logs `500` and another logs `"500"`, an inferred field
is a number in one document and a string in the next, and OpenSearch resolves that by
refusing to index the second — silently, and only under real traffic.

A value that cannot be coerced to its declared type is **omitted and recorded as an anomaly**
on the document. The document still lands. Losing a whole flow because one field was
malformed would be the worst possible trade for a tool whose purpose is not losing logs.

### 5. Two rules writing to one index may not disagree about a type

Configuration validation rejects the same promoted name declared with different types by
rules that share an output index. Caught at boot, where it costs nothing, rather than as
rejected documents in production.

### 6. The index template is generated from the configuration

`flowstitch print-index-template` reads the rules and prints the template, including a
correctly typed mapping for every promoted field.

With `dynamic: false` in force, a promoted field with no mapping is stored, returned, and
silently unsearchable — the failure that looks exactly like working. Generating the template
from the same configuration that drives promotion makes that state unreachable.

It prints; it does not apply. Cluster configuration belongs to whoever runs the cluster.

## Consequences

- Typed queries and aggregations become possible on the handful of fields that matter, while
  everything else stays in bounded flat-object payloads.
- Adding a promoted field is a config change plus re-running the template command. On daily
  indices the new mapping takes effect the next day, or immediately on a new index.
- Index templates are generated from the same configuration that drives promotion.
- `fields` answers part of the open question about which queries the schema should serve — by
  letting each deployment answer it for itself rather than guessing centrally.
- Promotion reads the same compiled paths as everything else, so no new path semantics.

## Alternatives considered

- **Entry-level promotion** — richer, and undermined by plain-object mapping until entries
  become `nested`. Revisit together with that decision.
- **Inferred types** — less configuration, at the price of cross-service type conflicts that
  surface as unindexed documents.
- **Promote into the top level of the document** — shortest field paths, and it mixes engine
  guarantees with operator configuration permanently.
- **A fixed built-in summary** (`services`, `outcome`) — no configuration at all, and wrong
  for anyone whose logs are not HTTP.
