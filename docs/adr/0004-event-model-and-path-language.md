# ADR-0004: Event model and path language

- **Status:** accepted
- **Date:** 2026-08-20
## Context

A canonical envelope with fixed field names — `event_id`, `event_type`,
`flow_id`, `source`, `timestamp` — that producers were expected to adopt. Real producers
do not: one service emits the event name as `event`, another as `type`; the timestamp is
`datetime` here and `@timestamp` there; correlation IDs live at any depth.

Requiring producers to rename fields makes adoption a migration project for every team that
wants in. Meanwhile every value the engine needs was already becoming a configurable path —
first `flow_id`, then the stitch key. The envelope was the last thing resisting the pattern.

## Decision

### 1. FlowStitch does not reshape producer logs

An event is the raw producer document, plus `observed_at` from our clock. Nothing is
renamed, moved, or folded into a synthetic `attributes` map. The document is preserved
verbatim into the output.

### 2. Every value the engine needs is a configured path

Each rule declares where to find what it needs in the logs it owns:

```yaml
extract:
  event_type: $.event        # the producer's field, whatever it is called
  timestamp:  $.datetime
correlation:
  key: $.flow_id
```

Extraction is per-rule, not global, because the rule is the thing that knows the shape of
its own logs. Different services can emit entirely different shapes.

### 3. Rule selection is by resolved correlation key

The first rule whose `correlation.key` path resolves to a non-empty value claims the event.
Config validation rejects two enabled rules sharing a key path, so selection is
deterministic and needs no separate match block. Event types remain available as optional
narrowing, not as the selector.

### 4. Nothing about an event is mandatory

`event_id` is not required — it is not used at all. Identity comes from the stitch slot an
event occupies (ADR-0005, section 2), which needs no producer-supplied ID.

`event_type` is not required either. An event whose type path resolves to nothing cannot
fill a stitch role, so it becomes a plain entry in the output and never gates flow closing.
Same rule as an event missing a `group_by` field — no special case.

The only requirement is that the correlation key resolves. Without it there is no flow to
join, and the event is quarantined.

`event_id` is not mandatory, and no content-hash fallback is needed.

### 5. Path language

A path resolves to **at most one value**. That invariant is what keeps correlation
deterministic, and it is why the language has no wildcards, no recursive descent and no
filters.

```yaml
$.context.invocation_id     # keys, any depth
$.attempts[0].status        # index
$.attempts[-1].status       # index from the end
$.payload["http.status"]    # quoted: any field name, dots included
$.payload["0"]              # quoted digits are a key, not an index
```

Grammar:

- `$.` prefix marks a path. A bare string in `roles`, `requires` or `terminal_events` is a
  literal value compared against the event type — never a path.
- Segments split on `.`.
- `[n]` and `[-n]`, bare digits, are array indices; negative counts from the end.
- `["..."]` is a literal key. `\"` and `\\` escape inside it, so no field name is
  unreachable.

Resolution rules:

- A path that does not resolve yields no value. It is not an error — the caller decides
  whether absence is fatal, and only the correlation key treats it that way.
- Non-string scalars coerce deterministically: integral numbers format without a decimal
  point or exponent (`12345`, never `12345.000000` or `1.2345e+04` — JSON numbers decode as
  float64, so this needs care), booleans as `true`/`false`.
- Objects and arrays resolve to no value rather than being stringified.

### 6. Object arrays are a producer problem

Indexing handles `$.attempts[-1].status`. It does not handle finding the element of
`{"headers":[{"name":"X-Id","value":"abc"}]}` where `name == "X-Id"` — that is a filter, and
filters are where a small path language turns into a query engine.

If correlation IDs arrive inside object arrays, the fix belongs at the producer or in an
ingress transformation, not in the rule config.

## Consequences

- Adoption is a config change, not a producer migration. A service already emitting
  structured logs with a correlation ID needs no code change.
- The `event_type` / `event_id` / `attributes` fields disappear from the domain event, and
  with them the normalizer that folded unknown top-level keys into `attributes`.
- The domain holds `map[string]any` rather than a typed struct. Less compile-time safety,
  but honest: the data genuinely is arbitrary, and pretending otherwise moved the problem
  into the adapter without solving it.
- Two rules that own differently-shaped logs coexist without either producer changing.
- Bounded mapping discipline becomes more important, not less: arbitrary producer fields
  now reach the output document directly and must stay under bounded object or flattened
  mappings, never promoted to top-level mapped fields by default.

## Alternatives considered

- **Full JSONPath (RFC 9535)** — familiar and standardised, but most of it is multi-value
  by design, so we would ship a large evaluator and then forbid two thirds of it.
- **JSON Pointer (RFC 6901)** — genuinely elegant here: single-valued by construction,
  native indices, and literal dots need no escaping because `/` is the separator. Rejected
  on familiarity — `/service` next to `http.request` in the same YAML reads as a mistake,
  and everyone who has used Kibana will type `$.` by reflex.
- **Paths as YAML lists** (`[context, invocation_id]`) — no syntax to parse at all, since
  YAML already solves quoting. Rejected as too verbose for the common case; kept in mind as
  a fallback if bracket quoting proves fiddly.
- **A required canonical envelope** — simplest engine, at the cost of
  making every producer team rename fields before they can use the tool.
