# ADR-0001: Go is the implementation language

- **Status:** accepted
- **Date:** 2026-08-20
## Context

The implementation language is a choice rather than an external constraint. The workload
is a long-lived stateful daemon: it holds
open correlation state in memory over an embedded transactional store, serves an HTTP
batch ingress, runs background timeout and delivery loops, and must deploy as a small
sidecar next to a log forwarder.

## Decision

FlowStitch is written in Go.

## Consequences

- Single static binary and small container image, which makes sidecar deployment practical.
- A fixed number of hash-partitioned workers with serialized mutation per correlation key
  maps directly onto goroutines and channels.
- Embedded transactional stores are available natively (bbolt, Pebble, Badger), so real
  candidates can be benchmarked without cgo.
- Prometheus and OpenSearch clients are first-class.
- Cost: no sum types or generics-friendly pattern matching for the rule expression
  language, so constrained predicates would have to be enforced by hand.
- Cost: the team's other services are PHP, so this repository does not share their
  tooling, CI images or review habits.

## Alternatives considered

- **PHP 8.5 / Symfony** — matches the surrounding stack and the team's DDD experience,
  but a stateful daemon with in-memory shards, a timing wheel and embedded transactional
  storage fights the request-scoped process model. It would require Swoole or RoadRunner
  to be viable, which trades the familiarity advantage away.
- **Java 25 / Kotlin** — strongest transactional-storage and concurrency ecosystem
  (virtual threads, RocksDB), but the JVM footprint undermines the lightweight identity.
- **Rust** — best fit for bounded memory and predictable latency, rejected on delivery
  speed and the absence of Rust experience on the team.
