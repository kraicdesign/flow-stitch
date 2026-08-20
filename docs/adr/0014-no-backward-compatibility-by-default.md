# ADR-0014: No backward compatibility by default

- **Status:** accepted
- **Date:** 2026-08-21

## Context

FlowStitch is pre-release and has no installed base. Compatibility work — dual-format
decoders, migration paths, deprecated fields kept alive, adapters that accept both the old and
the new shape — is expensive to write, harder to test, and permanent once added. Paying that
cost for users who do not exist yet buys nothing.

The question keeps arriving in disguise: a codec version bump, a renamed configuration key, a
changed document field, a dependency major version. Each time, the default answer should be
the same and it should not need re-deciding.

## Decision

**Break freely. Do not build compatibility shims.**

Specifically, none of the following require a migration path, a fallback, or a dual-format
reader:

- persisted flow state — a codec version bump may simply refuse older records;
- configuration keys — removed keys fail the load rather than being tolerated;
- output document fields — renamed or removed without an alias;
- index templates — regenerate and reinstall;
- dependency major versions, including on-disk formats they introduce.

**Where a change carries a real risk, raise it and ask — do not implement compatibility on
your own initiative.** A risk means data that cannot be recovered, a deployment that cannot
start, or an irreversible on-disk change. Naming it is the job; deciding what to do about it
belongs to whoever owns the deployment.

The practical upgrade procedure follows from this: **drain or discard state, then upgrade.**
Let open flows finalize, stop the service, replace the binary or image, start it again. Where
that is not acceptable, that is the moment to ask, not the moment to write a migration.

## Consequences

- Less code, and no long-lived branches for shapes nobody runs any more.
- A running deployment cannot always be upgraded in place. That has to be documented wherever
  operators look, not discovered.
- Rolling back to a previous build may fail if the newer one changed an on-disk format. That
  is acceptable now and stops being acceptable at 1.0.
- Once there is a real installed base, this ADR gets superseded rather than quietly ignored.
  The successor should say what compatibility *is* promised, and from which version.

## Alternatives considered

- **Maintain compatibility from the start** — friendlier to a user base that does not exist,
  at the cost of carrying every past decision forward.
- **Decide case by case** — what happens without this ADR, and it produces inconsistency: one
  migration written because it seemed easy, another skipped because it did not.
