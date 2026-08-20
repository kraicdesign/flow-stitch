# ADR-0011: Configuration reload on SIGHUP

- **Status:** accepted
- **Date:** 2026-08-21

## Context

Rules change for a new producer, a corrected path or a longer timeout. Applying them must not
restart the process or drop every open flow that has not yet finalized. A flow already records
the rule version it opened under, and the registry keeps versions.

## Decision

### 1. SIGHUP reloads the configuration

Standard for daemons, works with `docker kill -s HUP` and systemd, and needs no
authentication: sending a signal already requires access to the process. No new endpoint on
the port that accepts events.

### 2. A bad configuration never takes down a running service

The new file is fully validated first — paths compiled, rules checked, cross-rule conflicts
resolved. On any error the reload is abandoned, the running configuration stays in force, and
the error is logged naming the file and the problem.

This is the opposite of startup behaviour, and deliberately so. At boot, failing fast on a bad
config is right because nothing is running yet. At reload, a typo must not stop a service that
is correlating live traffic.

### 3. Open flows finish under the rules they started with

A reload publishes a new rule version. Flows already open keep resolving their pinned version
until they finalize, so completion semantics never change halfway through a flow.

Old versions are retained while any open flow references them, and reclaimed once none do.
Without reclamation, every reload would leak a rule set for the lifetime of the process.

### 4. Some settings are boot-only, and the reload says so

Reloadable: rules, stitch definitions, promotion, lifecycle, limits, pass-through settings,
self-reported alert settings, log level.

Boot-only: listen address, state driver and path, sync policy.

A reload that changes a boot-only setting applies everything else and **logs plainly which
values were ignored**. Silently accepting a change that did not take effect is how someone
spends an afternoon debugging a listen address that was never applied.

### 5. Reloads are observable

`flowstitch_config_reloads_total{result}` and
`flowstitch_config_loaded_timestamp_seconds`, recording when the active configuration took
effect. The version is deliberately not a label: every content hash would create an
unbounded label value, while the timestamp answers the operator's question without that
cardinality cost (ADR-0007). A failed reload must be visible as more than a log line, because
the symptom otherwise is a service quietly running yesterday's rules.

## Consequences

- Rule changes no longer cost open flows.
- The rule registry needs reference counting or an equivalent sweep, which is new state to get
  right — a version reclaimed too early breaks a flow mid-life.
- Two validation paths exist with deliberately different failure behaviour, and tests must
  cover both.
- A running process can outlive several configurations, so logs and metrics need the version
  to make sense of what rules were in force.

## Alternatives considered

- **Admin HTTP endpoint** — convenient from a deploy pipeline and can return validation errors
  directly. Needs authentication and puts a config-changing operation on the ingest port.
  Worth adding later; the signal comes first because it needs neither.
- **Watching the config file** — nothing to remember, and it reloads on half-written files,
  editor temp files and partially-synced volume mounts.
- **Restart only** — simplest, and pays for every rule change with the open flows.
