// Package rules implements the ordered, versioned rule registry.
package rules

import (
	"sync"
	"sync/atomic"

	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

// Registry publishes current rules while retaining versions pinned by open flows.
type Registry struct {
	mu       sync.Mutex
	snapshot atomic.Pointer[snapshot]
	open     map[versionKey]int
}
type snapshot struct {
	ordered  []rule.Rule
	versions map[versionKey]rule.Rule
}
type versionKey struct {
	id      rule.ID
	version rule.Version
}

// NewRegistry publishes an initial rule snapshot.
func NewRegistry(rs []rule.Rule) *Registry {
	registry := &Registry{open: make(map[versionKey]int)}
	registry.Publish(rs)
	return registry
}

// Publish atomically replaces the current rules and reclaims unused old versions.
func (r *Registry) Publish(rs []rule.Rule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := &snapshot{ordered: append([]rule.Rule(nil), rs...), versions: make(map[versionKey]rule.Rule)}
	if current := r.snapshot.Load(); current != nil {
		for key, value := range current.versions {
			if r.open[key] > 0 {
				next.versions[key] = value
			}
		}
	}
	for _, configured := range rs {
		next.versions[versionKey{configured.ID, configured.Version}] = configured
	}
	r.snapshot.Store(next)
}

// MatchAndRetain returns the first matching current rule and provisionally
// retains its version, atomically with respect to publication (ADR-0011).
func (r *Registry) MatchAndRetain(e event.Event) (rule.Rule, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.snapshot.Load()
	if current == nil {
		return rule.Rule{}, false
	}
	for _, configured := range current.ordered {
		if !configured.Enabled {
			continue
		}
		if value, ok := configured.Key.Resolve(e.Doc); ok && value != "" {
			r.open[versionKey{configured.ID, configured.Version}]++
			return configured, true
		}
	}
	return rule.Rule{}, false
}

// SeedOpenFlows initializes version references recovered from durable state.
func (r *Registry) SeedOpenFlows(counts map[rule.Reference]int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for reference, count := range counts {
		r.open[versionKey{reference.ID, reference.Version}] = count
	}
}

// Release drops one flow reference and reclaims a non-current version at zero.
func (r *Registry) Release(reference rule.Reference) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := versionKey{reference.ID, reference.Version}
	if r.open[key] > 1 {
		r.open[key]--
		return
	}
	delete(r.open, key)
	current := r.snapshot.Load()
	for _, configured := range current.ordered {
		if configured.ID == reference.ID && configured.Version == reference.Version {
			return
		}
	}
	next := &snapshot{ordered: append([]rule.Rule(nil), current.ordered...), versions: make(map[versionKey]rule.Rule, len(current.versions))}
	for existing, configured := range current.versions {
		if existing != key {
			next.versions[existing] = configured
		}
	}
	r.snapshot.Store(next)
}

// Get resolves an exact rule version retained for an open flow.
func (r *Registry) Get(id rule.ID, version rule.Version) (rule.Rule, bool) {
	current := r.snapshot.Load()
	if current == nil {
		return rule.Rule{}, false
	}
	configured, ok := current.versions[versionKey{id, version}]
	return configured, ok
}
