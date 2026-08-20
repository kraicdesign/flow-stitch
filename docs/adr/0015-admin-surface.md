# ADR-0015: Administrative surface and its authentication

- **Status:** accepted
- **Date:** 2026-08-21
- **Relates to:** [0012](0012-dead-letter-retention.md), [0013](0013-self-reported-alerts.md)

## Context

Alerts announce that documents are stuck and why. Recovery requires replaying a dead-lettered
document by putting it back in the outbox so normal delivery retries it.

The obvious shape is a CLI subcommand: `flowstitch dlq replay`. **It cannot work.** Pebble
takes an exclusive lock on the store, so a second process cannot open it while the service is
running. A CLI would require stopping the service to recover documents, during an incident,
which is precisely when stopping it is worst.

That leaves the running process as the only thing that can reach the data, which means an
administrative interface, which means authentication — because dead-lettered documents contain
producer payloads.

## Decision

### 1. Administration is HTTP, on the running process

`/v1/admin/…` on the existing listener. Not a second port: one listener is one thing to
configure, expose and get wrong.

### 2. Disabled unless a token is configured

```yaml
server:
  admin_token_env: FLOWSTITCH_ADMIN_TOKEN
```

With no token configured, the admin routes are **not registered at all** — not present rather
than present-and-refusing. An operator who never configured administration has no
administrative attack surface, and cannot accidentally acquire one by upgrading.

Where a token is configured, every admin request carries `Authorization: Bearer <token>`, the
comparison is constant-time, and failures are logged without echoing the value presented.

The token comes from the environment, never the config file, for the same reason as the
OpenSearch credentials.

### 3. Metadata is not payload

Listing dead letters returns metadata: output ID, index, rejection type, sizes, timestamps,
attempt counts. It does **not** return document bodies.

Fetching one body is a separate, explicit request for a single record. That way the common
operation — seeing what is stuck and why — never moves producer payloads across the network,
and the rare operation of inspecting one document is deliberate.

Bodies are never written to logs, and rejection *reasons* from OpenSearch stay excluded
because they quote offending values.

### 4. Replay is idempotent by construction

A replayed record returns to the outbox with its **original deterministic document ID**, so a
document that was in fact already indexed overwrites itself rather than duplicating.

Attempts reset, the next attempt is immediate, and a replay counter on the record survives
so repeated failures are visible. A document that has been replayed four times and returned
four times is telling you something the first replay did not.

### 5. Replay does not fix the cause

A mapping conflict rejects the same document again. The runbook order is: read the alert, find
the reason, correct the template or the producer, roll or recreate the index, *then* replay.

The documentation must say this plainly, because "replay does nothing" is otherwise a
reasonable conclusion to draw from replaying into an unchanged index.

## Consequences

- Dead-lettered documents become recoverable without stopping the service.
- FlowStitch gains an authenticated surface it did not have. The default of "absent unless
  configured" keeps that from being a cost for deployments that never use it.
- The same surface is the natural home for future flow-inspection and statistics endpoints,
  and for a reload trigger if one is ever wanted from a pipeline.
- Bearer tokens are a modest mechanism. Anything stronger — mTLS, an identity provider — can
  come later without changing the endpoints.

## Alternatives considered

- **A CLI subcommand** — no authentication, no network surface, and blocked by the store lock
  unless the service is stopped.
- **A second admin-only listener** — a cleaner separation for network policy, at the cost of
  another port to configure, expose correctly, and forget about.
- **Automatic periodic replay** — no operator action at all, and it retries into an unchanged
  index forever, turning a visible problem into a quiet loop.
