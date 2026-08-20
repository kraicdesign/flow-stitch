# ADR-0012: Dead-letter retention

- **Status:** accepted
- **Date:** 2026-08-21

## Context

A document OpenSearch refuses permanently — a mapping conflict, an unparseable field, a
document too large — is moved to a dead-letter store rather than retried forever, so it cannot
block the queue behind it.

Nothing removes those records. A persistent mapping conflict would fill the disk, and a full
disk stops acknowledgement for *everything*: correlation, delivery and pass-through alike.

## Decision

**Cap the dead-letter store by record count. On overflow, drop the oldest. Expose both the
depth and a counter of what was dropped.**

Bounded disk beats perfect retention. Losing the oldest rejected documents is a small,
contained loss; losing the node to a full disk stops the entire pipeline, including the flows
that were working fine.

Oldest-first because the newest rejections are the ones someone is most likely to be
investigating, and because a repeating mapping conflict produces near-identical records — the
hundredth copy teaches nothing the first did not.

`flowstitch_dlq_records` and `flowstitch_dlq_dropped_total` make both states visible. A
non-zero drop count means rejections are outpacing anyone's attention, which is the signal
that the cap is doing real work rather than sitting idle.

## Consequences

- Disk use is bounded by configuration rather than by hope.
- Dead-lettered documents are diagnostic, not archival. Anything that must not be lost has to
  be fixed at the mapping or the producer, not stored here indefinitely.
- The cap needs a sensible default, since it is the kind of setting nobody tunes until it has
  already caused a problem.

## Alternatives considered

- **Age-based retention** — predictable, and bounded only if the rejection rate stays
  reasonable. A persistent conflict can still fill the disk inside the window.
- **Keep forever with loud alerting** — nothing is ever lost, and it guarantees a full disk
  whenever nobody is watching.
