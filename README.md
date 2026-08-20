# FlowStitch

**Stitch related events into complete, durable flow documents.**

FlowStitch is a lightweight, stateful event-correlation engine. It accepts independent
structured events, groups the ones that belong to the same logical flow, waits until that
flow is complete or its timeout expires, and emits **one** enriched document to OpenSearch.

> **Version 0.1.0.** The correlation core, durable state, delivery and packaging are complete
> and tested; see [Known gaps](#known-gaps) for what is deliberately not there yet.

## The problem

A distributed operation produces several logs that are technically independent but
semantically one thing:

```text
PC service ──request──> Web service ──request──> APM service
PC service <─response── Web service <─response── APM service
```

A traditional pipeline indexes each arrow separately. Operators then see four or more
documents describing a single user action, and have to reconstruct the flow at query time.
A missing response is not represented at all — it is merely absent.

FlowStitch moves that reconstruction into the ingestion path. Producers keep emitting simple
events; FlowStitch holds temporary correlation state, detects missing and duplicate events
within an open flow, and indexes the completed — or explicitly timed-out — flow as one
queryable document.

```text
Applications ──> Fluent Bit ──> FlowStitch ──> OpenSearch
                                    │
                            durable state + outbox
```

The core idea is broader than HTTP: the engine correlates **arbitrary event types** using
**configuration-defined rules**. Request/response is the first use case, not the domain model.

## What it does

- **Correlates** events sharing a configured key into one flow, over a bounded window.
- **Stitches** events describing the same call — a request and its response — into a single
  entry with its own duration, so a multi-hop operation shows where the time went.
- **Closes** a flow when nothing is still waiting, when a terminal event arrives, when a limit
  is reached, or when the timeout expires. Every reason is visible in the document.
- **Reports absence**: a call that never got a response is counted and marked, rather than
  being silently missing.
- **Survives restarts** — open flows and undelivered documents live in an embedded Pebble
  store, and flows past their deadline finalize before ingestion resumes.
- **Delivers** through the OpenSearch Bulk API with deterministic document IDs, classified
  failures, backoff, and a capped dead-letter store.
- **Passes through** events matching no rule, verbatim, so FlowStitch can sit inline without
  anything disappearing from the pipeline.
- **Promotes** named fields into typed, queryable document fields, and generates the matching
  index template from the same configuration.
- **Reloads** rules on `SIGHUP` without dropping open flows.
- **Reports itself**: Prometheus metrics, health endpoints, and diagnostic documents written
  into OpenSearch when documents get stuck.
- **Recovers** dead-lettered documents through an authenticated admin API — see the
  [stuck-document runbook](docs/runbooks/stuck-documents.md).

## What it is not

- Not a replacement for OpenTelemetry tracing. A trace ID can serve as a correlation key, but
  tracing instrumentation is never required.
- Not a message broker, and it does not need one to run.
- Not a general stream-processing platform. Running Flink or Kafka Streams to merge a few
  related records is disproportionate; FlowStitch is the small stateful primitive in between.
- Not a log collector. Fluent Bit stays responsible for collection, parsing and routing.
- Not a redaction layer. Producer payloads reach OpenSearch as written.

## Known gaps

Honest about what is missing, since some of it matters in production:

- **Quarantined input is logged, not durably stored.** Ingestion tells the forwarder not to
  retry a rejected event, and then only writes a log line.
- **Byte-based capacity limits are not enforced.** Record counts are; `max_state_bytes`
  expresses intent but the threshold contract is undecided. Size the filesystem and alert on
  `flowstitch_state_bytes` meanwhile.
- **No performance targets and no benchmarks.** Nothing defines "fast enough" yet, so nothing
  measures it.
- **Byte-threshold and torn-storage-write tests are deliberately absent** — see
  [`test/e2e`](test/e2e/README.md) for the coverage map and the boundaries of what FlowStitch
  can meaningfully test.
- **Partial-batch acknowledgement semantics are undecided.** A retried batch can produce a
  second document for a flow that has already closed.

Deliberately absent: distributed operation, a user interface, and automatic correction of
rejected documents.

## Design principles

These are load-bearing. Changes that break one of them need an ADR.

1. **Generic events, specific configuration.** HTTP semantics never enter the core.
2. **Acknowledge promptly, never deferred.** The response follows the state write; it never
   waits for a flow to close or for OpenSearch to accept anything.
3. **The state store is truth**; memory is cache and acceleration.
4. **Finalization is explicit.** `invocations_complete`, `terminal_event`, `timeout`,
   `limit_exceeded` and `rule_unavailable` are visible states.
5. **Absence is data.** An unanswered call becomes a counted, queryable field, not a gap.
6. **Idempotency everywhere practical.** Slot-based deduplication plus deterministic sink
   document IDs make retries safe.
7. **Bound everything** — flows, queues, bytes, retries, retention.
8. **Preserve raw evidence while projecting a stable document shape.**
9. **Keep the core transport- and sink-independent.**
10. **Stay lightweight** until real requirements justify distributed complexity.

## Documentation

| Document | What it covers |
|---|---|
| [Specification](docs/specification.md) | What the system is and how it behaves. The current description of everything. |
| [Decision records](docs/adr/README.md) | Why each choice was made, and the alternatives that lost. |
| [Diagrams](docs/diagrams/README.md) | One event end to end, the flow lifecycle, where state lives. |
| [Stuck-document runbook](docs/runbooks/stuck-documents.md) | What to do when OpenSearch refuses documents. |
| [Compose walkthrough](deploy/compose/README.md) | Clone to a stitched document in Dashboards. |

## Getting started

Requires Go 1.25 or newer.

```bash
make validate
```

That is the full gate — gofmt, vet, build, race-enabled tests, and validation of the shipped
example configuration. Run it before every commit.

Start the service:

```bash
make run
```

Then:

```bash
curl -s localhost:8080/health/ready
```

```bash
curl -s localhost:8080/metrics | grep flowstitch_
```

Send an event (answers `202` with its correlation disposition):

```bash
curl -sXPOST localhost:8080/v1/events -d '{"event":"http.request","flow_id":"abc","service":"pc","datetime":"2026-08-20T12:00:00.000Z","context":{"invocation_id":"inv-1"}}'
```

Validate a configuration file without starting:

```bash
./bin/flowstitch -validate -config config/flowstitch.example.yaml
```

## Configuration

Rules are declarative — a new flow type never requires code. See
[config/flowstitch.example.yaml](config/flowstitch.example.yaml) for the annotated version.

```yaml
alerts:
  enabled: true
  index: flowstitch-alerts-{yyyy}.{MM}.{dd}
  min_interval: 5m
  outbox_age_threshold: 5m

rules:
  - id: application-flow
    enabled: true
    extract:
      event_type: $.event
      timestamp: $.datetime
    correlation:
      key: $.flow_id
    stitch:
      - id: api-invocation
        group_by: [$.service, $.context.invocation_id]
        roles:
          request: http.request
          response: [http.response, http.error]
        requires: [request, response]
    promote:
      # Lift only values that need typed range queries or aggregations.
      status: {path: $.payload.status, type: long, from: response}
    lifecycle:
      timeout: 30s
      close_when: all_invocations_complete
    limits:
      max_events: 64
      max_flow_bytes: 1048576
    output:
      index: application-flows-{yyyy}.{MM}.{dd}
      timestamp: first_event.timestamp
```

Configuration is fully validated at boot: an unknown key, an ambiguous rule, or a malformed
path fails the process before it accepts a single event.

Each `group_by` path becomes an `events.group` field using its last segment. Colliding names
gain numeric suffixes in list order (`id`, `id2`). Reordering `group_by` therefore renames
mapped fields; append new paths rather than reordering existing ones unless you intend a
mapping change.

## Index templates

Generate and install a template for each configured output index before sending production
traffic, and regenerate whenever promotion configuration changes:

```bash
./bin/flowstitch print-index-template -config config/flowstitch.example.yaml \
  -index 'application-flows-{yyyy}.{MM}.{dd}' | \
curl -XPUT localhost:9200/_index_template/flowstitch-application-flows \
  -H 'Content-Type: application/json' \
  --data-binary @-
```

Without `-index`, the command prints one template per distinct output index, plus the alert
template when alerts are enabled. Alert documents carry no producer data and use
`dynamic: false`, so a mapping conflict in a flow index cannot stop its own diagnostic from
being indexed.

**Installing a corrected template does not change an index that already exists** — its
mappings were fixed at creation. Delete the affected index and let it be recreated where the
data is expendable, or roll over to a new one. With daily indices the corrected template
applies naturally to the next day.

The example retention policy is separate, and its age is a deployment decision, so edit it
before installing:

```bash
curl -XPUT localhost:9200/_plugins/_ism/policies/flowstitch-retention \
  -H 'Content-Type: application/json' \
  --data-binary @deploy/opensearch/ism-retention.json
```

The `events` array uses ordinary object mapping, not `nested`. A query combining two facts
about one entry can therefore match those facts across *different* entries; use flow-level
fields for exact flow-wide questions (ADR-0008).

## Container

Build locally, or pull once images are published:

```bash
make docker-build
```

Release images use `:X.Y.Z`, `:latest` names the newest release, and every build carries a
full commit-SHA tag so a running container traces back to source. Prefer a release or SHA tag
over `:latest` where deployments must be reproducible.

### Always mount the state directory

A named volume lets open flows and outbox records survive container replacement (ADR-0006):

```bash
docker volume create flowstitch-state
docker run --name flowstitch -p 8080:8080 --stop-timeout 20 \
  --read-only --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  --log-driver local --log-opt max-size=10m --log-opt max-file=3 \
  -v flowstitch-state:/var/lib/flowstitch \
  -v "$PWD/config/flowstitch.container.yaml:/etc/flowstitch/flowstitch.yaml:ro" \
  -e FLOWSTITCH_OPENSEARCH_USERNAME \
  -e FLOWSTITCH_OPENSEARCH_PASSWORD \
  kraicdesign/flowstitch:latest
```

Omit the mount and the image creates an *anonymous* volume: it survives stop and start, but
recreating the container attaches a new empty one and orphans the old one on disk, still
holding those flows.

`docker volume prune` deletes an unused named volume whose container is stopped. A bind mount
is immune to pruning and makes capacity and growth visible to ordinary host tools. The image
runs as `10001:10001`, so prepare the directory first:

```bash
sudo mkdir -p /srv/flowstitch && sudo chown 10001:10001 /srv/flowstitch
```

A volume created by an older image may carry a different owner. Stop FlowStitch, then either
discard the transient state or change the volume's ownership — the image never rewrites
permissions itself.

### Runtime notes

Mount the configuration read-only, keep `state.driver: pebble` with
`state.path: /var/lib/flowstitch`, and pass credentials through the environment rather than
the file. The shipped container configuration addresses OpenSearch as `opensearch` to match
the Compose network.

The **stop timeout must exceed `server.shutdown_grace`** — the commands above allow 20 seconds
for the 15-second grace. The healthcheck start period must exceed worst-case Pebble open and
overdue-flow recovery, not merely normal startup.

Reload configuration without interrupting ingestion (ADR-0011):

```bash
docker kill -s HUP flowstitch
```

An invalid reload leaves the running configuration in place. Open flows finish under the rules
they started with; new flows use the reloaded ones. Listen address and state settings are
boot-only, and changed values are logged as ignored.

### Sizing

Memory: at least `pass-through buffer_size × average encoded event bytes`, plus Pebble's block
cache and process overhead. Measure representative events before choosing a production limit.

Disk during an OpenSearch outage: roughly `documents/second × average document bytes × outage
seconds`, plus open-flow and Pebble overhead. `limits.max_outbox_records` stops ingestion and
fails readiness when its bound is reached.

The state directory holds transient in-flight data, normally minutes of it. It needs
deliberate capacity and monitoring rather than backup — producers and OpenSearch are the
durable endpoints.

## Releasing

Inspect readiness without changing the repository, or run the complete release:

```bash
make release-status 0.1.0
make release 0.1.0
```

Both `0.1.0` and `v0.1.0` produce the annotated Git tag `v0.1.0`. The release command first
requires a clean `main` branch that is not behind or divergent from `origin/main`, checks that
the tag is unused, and considers an existing GitHub CI conclusion when the `gh` CLI can read
one. It then runs `make validate`, `make test-e2e`, and `make docker-build` before creating or
pushing anything. A missing remote `main` is treated as the first-release case; otherwise only
a remote branch behind the verified local commit is advanced. Finally, the command pushes the
tag.

The tag starts the release workflow. With registry secrets configured, that workflow builds
`linux/amd64` and `linux/arm64` images and pushes `:0.1.0`, `:latest`, and the full commit-SHA
tag. Without the secrets, the Git tag remains published but no image is pushed.

Before choosing a version, manually confirm that the repository, package, domain, and
trademark name-collision check is complete; that Docker Hub secrets are configured when an
image is expected; and that the version is final. Release tags are intended to be permanent,
and the command has no force or replacement path.

## Architecture

The layout is hexagonal — a pure domain, an application layer that owns the use cases and
declares its ports, and adapters that implement them. Correlation policy never learns about
HTTP, OpenSearch or the chosen database.

```text
cmd/flowstitch/              composition root — the only place that wires ports to adapters
internal/
  domain/                    pure, no I/O
    event/                     raw producer documents plus observed time
    path/                      compiled single-value document paths
    rule/                      extraction, correlation, stitching and lifecycle rules
    flow/                      the correlation aggregate and its lifecycle
    projection/                finalized flow -> output document, deterministic IDs
    outbox/                    finalized document awaiting delivery
  application/               use cases and the ports they depend on
    ports.go                   StateStore, Tx, Sink, RuleRegistry, Quarantine, Capacity, Clock
    finalize.go                project, enqueue, then delete an open flow
    ingest/                    accept and correlate events
    expire/                    finalize flows past their persisted deadline
    deliver/                   drain the outbox into the sink
    admin/                     inspect and replay dead-letter records
  adapters/                  everything replaceable
    httpapi/                   ingress, health, metrics and authenticated administration
    config/                    strict YAML loading and path compilation
    rules/                     stable, ordered rule registry
    state/memory/              non-durable store for tests — not a deployment option
    state/pebble/              durable flow, expiry, outbox and dead-letter storage
    sink/opensearch/           bulk delivery with deterministic _id
    quarantine/                capture for records that must not be retried forever
  observability/             metrics, health, structured logging
config/                      example configuration
docs/                        specification, decision records, diagrams, runbooks
test/e2e/                    delivery scenarios, and the crash coverage still to be written
```

[The specification](docs/specification.md) defines current behaviour. The ADRs hold the
reasoning, and code comments cite them where a decision needs explaining.

## Open questions

Undecided contracts and what they block are listed in
[the ADR index](docs/adr/README.md). Code depending on one is marked:

```bash
grep -rn "TODO(contracts)" internal/
```

## Development

| Target | What it does |
|---|---|
| `make validate` | The full gate: fmt-check, vet, build, tests, config validation |
| `make test` | Race-enabled unit tests |
| `make test-e2e` | Build and run real-process lifecycle and recovery tests |
| `make lint` | golangci-lint, if installed |
| `make cover` | Coverage summary |
| `make run` | Build and run with the example configuration |

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
