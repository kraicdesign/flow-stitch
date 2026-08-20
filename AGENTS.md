# Agent instructions for FlowStitch

FlowStitch is a stateful event-correlation engine: it groups independent events that
share a correlation key into one flow, waits for completion or timeout, and emits a
single document to OpenSearch.

## Read this first

1. `docs/specification.md` — what the system is and how it behaves. Current; nothing else
   describes the whole system.
2. `docs/adr/` — the decisions behind it, with the reasoning and the alternatives that lost.
3. `README.md` — layout map and current capabilities.

The specification says *what*. The ADRs say *why*. **Code comments cite ADRs**, never
specification section numbers — a decision record explains itself, a section number does
not, and the numbers move.

## Verification gate

```bash
make validate
```

gofmt, `go vet`, build, `go test -race ./...`, and validation of the shipped example
config. A task is not complete until this passes. Run it yourself; do not report a task
done on the strength of a build alone.

## Architecture rules

The layout is hexagonal. Dependencies point inward, always:

```text
adapters ──> application ──> domain
```

- `internal/domain/` is pure. No I/O, no clock, no `context.Context`, no framework or
  transport types. The caller passes `now` in. A domain package importing an adapter is
  a bug, not a shortcut.
- `internal/application/` owns use cases and declares its driven ports in `ports.go`.
  Ports are defined here, by the consumer — never in the adapter that implements them.
- `internal/adapters/` implements ports. Producer quirks, wire formats and vendor
  clients stop here and never leak inward.
- `cmd/flowstitch/main.go` is the only place that knows which adapter fills which port.

## Conventions

- **Cite decisions, not documents.** Non-obvious behaviour gets an `(ADR-000N)` reference
  rather than a restatement of the reasoning. If no ADR covers it, that is a sign the
  decision was never actually made — say so instead of inventing one.
- `TODO(contracts)` marks work blocked on an unanswered question in `docs/adr/README.md`.
  **Never answer one by guessing.** If a task cannot proceed without an answer, stop, leave
  the task file where it is, and say which question blocks you.
- **Errors are values with meaning.** Ingestion distinguishes retryable from permanent,
  because that is what a forwarder acts on. Do not collapse the two.
- **No backward compatibility by default** (ADR-0014). Break persisted formats, config keys,
  document fields and dependency majors freely — do not write migrations, dual-format readers
  or deprecated aliases. If a change risks unrecoverable data, a deployment that cannot start,
  or an irreversible on-disk change, **say so in your final response and let the maintainer
  decide**. Naming the risk is the job; building compatibility on your own initiative is not.
- **Absence is data.** Missing invocation members and duplicates become counters,
  anomalies and fields — never silent drops.
- **Comments explain why.** The what is already in the code.

## Testing

- Unit tests sit next to the code. Table-driven where the cases are homogeneous.
- Domain tests use no fakes: construct events and rules directly.
- Test names state the behaviour, and failures print `got` and `want`.
- Cite the ADR a test defends when it defends one, e.g. `// ADR-0005: arrival order
  must not change the result.`
- Multi-process, crash and restart scenarios belong in `test/e2e/` — see its README for
  the acceptance list.

## Scope

Do what the task file says and stop there. If you find an unrelated problem, note it in
your final response instead of fixing it. Do not commit or push unless the task says to.

## Task lifecycle

Task files live directly under `.codex/`. Follow the lifecycle in `~/.codex/AGENTS.md`:
when the objective is met **and `make validate` passes**, move the task file to
`.codex/done/` with its filename preserved, and say so in your final response. Never move
an incomplete or blocked task.
