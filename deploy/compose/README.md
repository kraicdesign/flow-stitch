# FlowStitch Compose demo

This stack is for a local demonstration. OpenSearch security is deliberately disabled; do
not expose these ports or copy that setting into production.

## Start

From this directory, build FlowStitch locally and start all three required services:

```bash
docker compose up --build -d
docker compose ps
curl -fsS http://localhost:8080/health/ready
```

The Compose named volume `flowstitch-state` holds Pebble state and the durable outbox. Keep
that volume when recreating the container. `docker compose down` keeps it; `down -v` deletes
it and every open flow. `docker volume prune` also deletes the named volume whenever no
container is using it, including while FlowStitch is deliberately stopped.

Outside Compose, omitting the state mount gives the container an anonymous volume. It survives
container stop/start, but recreation attaches a new empty volume and leaves the old volume
orphaned on disk with its in-flight flows.

The service runs with a read-only root filesystem, a small `/tmp` tmpfs, UID:GID
`10001:10001`, a 20-second stop timeout, and capped local logs. The stop timeout must remain
above `server.shutdown_grace`. The image healthcheck allows two minutes for recovery; raise
that start period above the deployment's worst-case Pebble open and overdue-flow sweep time.

For a host-visible state directory immune to Docker volume pruning, replace the
`flowstitch-state:/var/lib/flowstitch` Compose mount with
`/srv/flowstitch:/var/lib/flowstitch`, after preparing it for the pinned container user:

```bash
sudo mkdir -p /srv/flowstitch
sudo chown 10001:10001 /srv/flowstitch
docker run -v /srv/flowstitch:/var/lib/flowstitch \
  --read-only --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  --log-driver local --log-opt max-size=10m --log-opt max-file=3 \
  --stop-timeout 20 kraicdesign/flowstitch:latest
```

A root-owned directory not writable by UID 10001 fails loudly at startup. The read-only root
prevents writes outside the state volume; the image declaration keeps the state path writable
even when Docker supplies an anonymous volume.

For the disposable demo, a volume from an older image may carry an obsolete Pebble format or
the former dynamic UID/GID. Reset it instead of diagnosing or migrating it (ADR-0014). From
the repository root:

```bash
docker compose -f deploy/compose/docker-compose.yml down
docker volume rm compose_flowstitch-state
```

This permanently deletes the demo's open flows, outbox, and dead-letter records.

Copy `.env.example` to `.env` only when credentials are needed; `.env` is ignored by Git.
The example file intentionally contains empty values.

The 512 MiB Compose memory limit is a demo starting point. Production memory should cover
`passthrough.buffer_size × average encoded event bytes`, Pebble's block cache, and Go/process
overhead. Size state disk for roughly `finished documents/second × average document bytes ×
maximum OpenSearch outage seconds`, plus open flows and Pebble overhead. The outbox record
limit is enforced through readiness and ingestion backpressure. State-byte enforcement is not
implemented — the disk and state thresholds that stop ingestion are still an open question in
the ADR index — so provision the filesystem to the configured budget and alert on
`flowstitch_state_bytes`.

This directory contains transient in-flight data, normally minutes of flows plus any current
delivery outage. It needs sizing and monitoring, not backup.

Fluent Bit is an optional example input, not a dependency. Its profile is off by default:

```bash
docker compose --profile input up -d fluent-bit
```

Any client that can POST JSON can send events directly, as below.

## Install the index template

Generate the template from the exact mounted configuration and install it into OpenSearch:

```bash
docker compose exec -T flowstitch flowstitch print-index-template \
  -config /etc/flowstitch/flowstitch.yaml \
  -index 'application-flows-{yyyy}.{MM}.{dd}' | \
curl -fsS -X PUT http://localhost:9200/_index_template/flowstitch-application-flows \
  -H 'Content-Type: application/json' --data-binary @-
```

OpenSearch returns `{"acknowledged":true}`.

Templates apply only when an index is created. Reinstalling this template does not repair an
existing demo index with an old mapping. The demo data is expendable, so delete the affected
index and let the next delivered document recreate it from the corrected template:

```bash
curl -fsS -X DELETE 'http://localhost:9200/application-flows-*'
```

In production, delete an affected index only when its data is expendable. Otherwise install
the corrected template and roll over to a new index; daily indices pick it up on the next day.

## Stitch a flow

Post both halves of one invocation:

```bash
curl -sS -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"event":"http.request","flow_id":"compose-demo","service":"web","datetime":"2026-08-21T12:00:00.000Z","context":{"invocation_id":"inv-1"}}'
```

```json
{"disposition":"correlated","reason":""}
```

```bash
curl -sS -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"event":"http.response","flow_id":"compose-demo","service":"web","datetime":"2026-08-21T12:00:00.050Z","context":{"invocation_id":"inv-1"},"payload":{"status":200}}'
```

```json
{"disposition":"finalized","reason":"invocations_complete"}
```

Delivery is asynchronous. After a second, verify the document directly:

```bash
curl -fsS 'http://localhost:9200/application-flows-*/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{"query":{"term":{"flow.id":"compose-demo"}}}'
```

Then open <http://localhost:5601>. In **Discover**, create an index pattern (data view)
`application-flows-*`, choose `@timestamp` as its time field, widen the time range to include
the sample timestamps, and search for `flow.id: "compose-demo"`. The result is the single
stitched document containing request and response entries.

## Prove state survives recreation

Post only the request using a fresh flow ID, recreate FlowStitch, and then post its response:

```bash
curl -sS -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"event":"http.request","flow_id":"restart-demo","service":"web","datetime":"2026-08-21T12:01:00.000Z","context":{"invocation_id":"inv-2"}}'
docker compose stop flowstitch
docker compose rm -f flowstitch
docker compose up -d flowstitch
curl -fsS http://localhost:8080/health/ready
curl -sS -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"event":"http.response","flow_id":"restart-demo","service":"web","datetime":"2026-08-21T12:01:00.050Z","context":{"invocation_id":"inv-2"},"payload":{"status":200}}'
```

Query for `flow.id: "restart-demo"` as above. The finalized result proves the named volume,
not the container filesystem, retained the open flow.

Stop the demo without deleting its data:

```bash
docker compose --profile input down
```
