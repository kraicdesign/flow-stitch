// Package health implements liveness and readiness semantics.
//
// The distinction that matters: a degraded sink is not unreadiness, because
// finalized documents are protected by the outbox and correlation continues.
// Exhausted durable capacity *is* unreadiness, because FlowStitch can no
// longer honour its acceptance promise.
package health

import (
	"context"
	"sync"
)

// State is the result of one check.
type State struct {
	Healthy bool
	Detail  string
}

// Check reports the state of one subsystem.
type Check func(ctx context.Context) State

// Registry aggregates named checks.
type Registry struct {
	mu    sync.RWMutex
	live  map[string]Check
	ready map[string]Check
}

// New builds an empty registry.
func New() *Registry {
	return &Registry{
		live:  make(map[string]Check),
		ready: make(map[string]Check),
	}
}

// AddLiveness registers a check that decides whether the process is alive.
// Liveness must stay cheap and must not depend on downstream systems, or a
// sink outage will get the process killed.
func (r *Registry) AddLiveness(name string, c Check) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.live[name] = c
}

// AddReadiness registers a check that decides whether it is safe to accept
// events: state store healthy, capacity available, config loaded.
func (r *Registry) AddReadiness(name string, c Check) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ready[name] = c
}

// Live evaluates all liveness checks.
func (r *Registry) Live(ctx context.Context) (bool, map[string]State) {
	return evaluate(ctx, r.snapshot(r.live))
}

// Ready evaluates all readiness checks.
func (r *Registry) Ready(ctx context.Context) (bool, map[string]State) {
	return evaluate(ctx, r.snapshot(r.ready))
}

func (r *Registry) snapshot(from map[string]Check) map[string]Check {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]Check, len(from))
	for name, c := range from {
		out[name] = c
	}
	return out
}

func evaluate(ctx context.Context, checks map[string]Check) (bool, map[string]State) {
	results := make(map[string]State, len(checks))
	healthy := true
	for name, c := range checks {
		state := c(ctx)
		results[name] = state
		if !state.Healthy {
			healthy = false
		}
	}
	return healthy, results
}

// Healthy is a check that always passes.
func Healthy(_ context.Context) State { return State{Healthy: true} }
