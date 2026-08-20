# End-to-end tests

The default gate remains the fast unit and integration suite:

```bash
make validate
```

Tests in this directory build and start the real binary, use a temporary Pebble store and
a stub OpenSearch, and exercise operating-system signals. Run them separately:

```bash
make test-e2e
```

Every process test owns its ports and state directory, polls observable conditions instead
of sleeping for fixed intervals, prints the child process output on failure, and registers
cleanup before starting the child.

## Coverage map

| Scenario | Coverage |
|---|---|
| Request/response arrival order | Integration: `internal/application/ingest` |
| Duplicate suppression | Integration: `internal/application/ingest` |
| Representative multi-hop merge | Domain and integration: `internal/domain/flow` and `internal/application/ingest` |
| Full HTTP-to-Bulk lifecycle | End-to-end: `TestFullLifecycleProducesOneMergedDocument` |
| Timeout with an incomplete invocation | End-to-end: `TestTimeoutClosingReportsMissingResponse` |
| Unmatched-event pass-through, verbatim and outside the outbox | Unit/integration plus end-to-end: `TestPassThroughIsVerbatimAndNeverEntersOutbox` |
| Open flows survive `SIGKILL`; Pebble unlocks and startup recovery delivers them | End-to-end: `TestSIGKILLRestartRecoversOpenFlows` |
| Graceful `SIGTERM` with work in flight exits within the configured grace | End-to-end: `TestSIGTERMWithOpenFlowExitsWithinGrace` |
| `SIGHUP` applies a changed rule to new flows while open flows retain the old rule | Integration plus end-to-end: `TestSIGHUPAppliesNewRuleOnlyToNewFlows` |
| Crash between outbox enqueue and open-flow deletion | End-to-end/store boundary: `TestCrashBetweenFinalizationWritesIsIdempotent` constructs the reachable state, reopens it, and proves re-finalization delivers one deterministic output ID |
| Ambiguous-success retry does not duplicate a sink document | Unit: `internal/adapters/sink/opensearch` and integration: `internal/application/deliver` |
| OpenSearch outage retains finalized flows | Integration: `internal/application/ingest` and `internal/application/deliver` |
| Record-count capacity and per-flow limits | Unit/integration: composition-root capacity checks and `internal/application/ingest` |
| Configuration errors fail before ingestion | Unit/integration: `internal/adapters/config` and composition-root tests |

The finalization crash point is reached by constructing the durable state: save the open
flow, enqueue its deterministic outbox record in a later commit, and deliberately omit the
delete. Timing a kill cannot select the interval reliably, and no test-only failure hook is
shipped in the production binary.

## Deliberate exclusions

- Disk-full and byte-threshold behaviour cannot be covered yet. Byte-based enforcement is
  not implemented because the threshold contract remains an open question. Record-count
  limits are implemented and tested.
- Nothing here attempts a genuinely torn Pebble write. Atomic storage-engine writes are
  Pebble's guarantee; testing that would require manually corrupting its files rather than
  exercising FlowStitch behaviour.
