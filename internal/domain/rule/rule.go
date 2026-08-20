// Package rule models immutable correlation and stitching configuration.
package rule

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/domain/path"
)

// ID is the configuration-owned stable name of a correlation rule.
type ID string

// Version identifies one immutable normalized rule definition.
type Version string

// CloseCondition selects a non-terminal lifecycle completion policy.
type CloseCondition string

// Reference identifies one immutable rule definition.
type Reference struct {
	ID      ID
	Version Version
}

// CloseAllInvocationsComplete closes after at least one complete invocation and none incomplete.
const CloseAllInvocationsComplete CloseCondition = "all_invocations_complete"

// Rule is an immutable, compiled correlation definition.
type Rule struct {
	ID        ID
	Version   Version
	Enabled   bool
	Extract   Extract
	Key       path.Path
	Stitch    []Stitch
	Promote   []Promotion
	Lifecycle Lifecycle
	Limits    Limits
	Output    Output
}

// PromotionType declares the output mapping type for a promoted field.
type PromotionType string

const (
	// PromotionLong maps a promoted integral value as an OpenSearch long.
	PromotionLong PromotionType = "long"
	// PromotionDouble maps a promoted numeric value as an OpenSearch double.
	PromotionDouble PromotionType = "double"
	// PromotionKeyword maps a promoted scalar as an exact-match string.
	PromotionKeyword PromotionType = "keyword"
	// PromotionBoolean maps a promoted value as an OpenSearch boolean.
	PromotionBoolean PromotionType = "boolean"
	// PromotionDate maps a promoted timestamp as an OpenSearch date.
	PromotionDate PromotionType = "date"
)

// Promotion lifts one selected event value into a typed flow field.
type Promotion struct {
	Name string
	Path path.Path
	Type PromotionType
	From string
}

// Extract holds compiled selectors for producer event metadata.
type Extract struct {
	EventType path.Path
	Timestamp path.Path
}

// Stitch maps producer event types into named invocation roles.
type Stitch struct {
	ID       string
	GroupBy  []path.Path
	Roles    []Role
	Requires []string
}

// GroupFields derives stable output field names from group_by paths. Repeated
// final segments are disambiguated in configuration order (ADR-0005, section 4).
func (s Stitch) GroupFields() []string {
	fields := make([]string, 0, len(s.GroupBy))
	used := make(map[string]struct{}, len(s.GroupBy))
	for _, groupPath := range s.GroupBy {
		base := groupPath.LastSegment()
		name := base
		for suffix := 2; ; suffix++ {
			if _, exists := used[name]; !exists {
				break
			}
			name = base + strconv.Itoa(suffix)
		}
		used[name] = struct{}{}
		fields = append(fields, name)
	}
	return fields
}

// Role names one invocation slot and the producer types that can fill it.
type Role struct {
	Name  string
	Types []string
}

// Lifecycle defines the total timeout and early-closing conditions.
type Lifecycle struct {
	Timeout        time.Duration
	CloseWhen      CloseCondition
	Settle         time.Duration
	TerminalEvents []string
}

// Limits bounds the evidence retained by one open flow.
type Limits struct {
	MaxEvents     int
	MaxEventBytes int64
	MaxFlowBytes  int64
}

// Output configures the correlated document destination and timestamp.
type Output struct {
	Index           string
	TimestampSource TimestampSource
}

// TimestampSource selects which lifecycle time becomes the document timestamp.
type TimestampSource string

const (
	// TimestampFirstEvent selects the earliest producer timestamp.
	TimestampFirstEvent TimestampSource = "first_event.timestamp"
	// TimestampLastEvent selects the latest producer timestamp.
	TimestampLastEvent TimestampSource = "last_event.timestamp"
	// TimestampFinalized selects the engine's finalization time.
	TimestampFinalized TimestampSource = "finalized_at"
)

var (
	// ErrMissingID reports a rule without its stable configuration identity.
	ErrMissingID = errors.New("rule: id is required")
	// ErrNoCorrelationKey reports a rule that cannot identify a flow.
	ErrNoCorrelationKey = errors.New("rule: correlation.key is required")
	// ErrNoTimeout reports a rule without a bounded lifetime.
	ErrNoTimeout = errors.New("rule: lifecycle.timeout is required")
	// ErrNoIndex reports a rule without an output destination.
	ErrNoIndex = errors.New("rule: output.index is required")
)

// Validate rejects incoherent rules before ingestion starts (ADR-0004; ADR-0005).
func (r Rule) Validate() error {
	if r.ID == "" {
		return ErrMissingID
	}
	if r.Key.String() == "" {
		return fmt.Errorf("rule %q: %w", r.ID, ErrNoCorrelationKey)
	}
	if r.Lifecycle.Timeout <= 0 {
		return fmt.Errorf("rule %q: %w", r.ID, ErrNoTimeout)
	}
	if r.Output.Index == "" {
		return fmt.Errorf("rule %q: %w", r.ID, ErrNoIndex)
	}

	claimed := make(map[string]string)
	declaredRoles := make(map[string]struct{})
	for _, stitch := range r.Stitch {
		roles := make(map[string]struct{}, len(stitch.Roles))
		for _, role := range stitch.Roles {
			roles[role.Name] = struct{}{}
			declaredRoles[role.Name] = struct{}{}
			for _, typ := range role.Types {
				if owner, exists := claimed[typ]; exists {
					return fmt.Errorf("rule %q: event type %q is claimed by stitch rules %q and %q", r.ID, typ, owner, stitch.ID)
				}
				claimed[typ] = stitch.ID
			}
		}
		for _, required := range stitch.Requires {
			if _, exists := roles[required]; !exists {
				return fmt.Errorf("rule %q: stitch %q requires undeclared role %q", r.ID, stitch.ID, required)
			}
		}
	}
	for _, promoted := range r.Promote {
		switch promoted.Type {
		case PromotionLong, PromotionDouble, PromotionKeyword, PromotionBoolean, PromotionDate:
		default:
			return fmt.Errorf("rule %q: promoted field %q has unknown type %q", r.ID, promoted.Name, promoted.Type)
		}
		if promoted.From == "" || promoted.From == "first" || promoted.From == "last" {
			continue
		}
		if _, ok := declaredRoles[promoted.From]; !ok {
			return fmt.Errorf("rule %q: promoted field %q selects undeclared role %q", r.ID, promoted.Name, promoted.From)
		}
	}
	return nil
}

// IsTerminal reports whether an event type immediately closes this rule's flow.
func (r Rule) IsTerminal(eventType string) bool {
	for _, terminal := range r.Lifecycle.TerminalEvents {
		if terminal == eventType {
			return true
		}
	}
	return false
}

// ContentVersion hashes the normalized rule definition. Collection members
// whose order has no semantic meaning are sorted before hashing (ADR-0011).
func ContentVersion(r Rule) (Version, error) {
	normalized := r
	normalized.Version = ""
	normalized.Stitch = append([]Stitch(nil), r.Stitch...)
	for i := range normalized.Stitch {
		normalized.Stitch[i].GroupBy = append([]path.Path(nil), r.Stitch[i].GroupBy...)
		normalized.Stitch[i].Roles = append([]Role(nil), r.Stitch[i].Roles...)
		for j := range normalized.Stitch[i].Roles {
			normalized.Stitch[i].Roles[j].Types = append([]string(nil), normalized.Stitch[i].Roles[j].Types...)
			sort.Strings(normalized.Stitch[i].Roles[j].Types)
		}
		sort.Slice(normalized.Stitch[i].Roles, func(a, b int) bool { return normalized.Stitch[i].Roles[a].Name < normalized.Stitch[i].Roles[b].Name })
		normalized.Stitch[i].Requires = append([]string(nil), r.Stitch[i].Requires...)
		sort.Strings(normalized.Stitch[i].Requires)
	}
	normalized.Promote = append([]Promotion(nil), r.Promote...)
	sort.Slice(normalized.Promote, func(i, j int) bool { return normalized.Promote[i].Name < normalized.Promote[j].Name })
	normalized.Lifecycle.TerminalEvents = append([]string(nil), r.Lifecycle.TerminalEvents...)
	sort.Strings(normalized.Lifecycle.TerminalEvents)
	raw, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("rule %q: hash definition: %w", r.ID, err)
	}
	sum := sha256.Sum256(raw)
	return Version(hex.EncodeToString(sum[:])), nil
}
