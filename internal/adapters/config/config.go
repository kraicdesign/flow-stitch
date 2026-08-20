// Package config owns the strict YAML wire format and builds domain rules.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/domain/indexname"
	"github.com/kraicdesign/flow-stitch/internal/domain/path"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
	"gopkg.in/yaml.v3"
)

// Config is the fully decoded and defaulted service configuration.
type Config struct {
	Server        Server        `yaml:"server"`
	State         State         `yaml:"state"`
	Passthrough   Passthrough   `yaml:"passthrough"`
	Alerts        Alerts        `yaml:"alerts"`
	Rules         []Rule        `yaml:"rules"`
	Sinks         Sinks         `yaml:"sinks"`
	Observability Observability `yaml:"observability"`
	Limits        Limits        `yaml:"limits"`
}

// Server configures the shared ingress and administrative HTTP listener.
type Server struct {
	Address        string        `yaml:"address"`
	ReadTimeout    time.Duration `yaml:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout"`
	ShutdownGrace  time.Duration `yaml:"shutdown_grace"`
	MaxRequestSize int64         `yaml:"max_request_size"`
	AdminTokenEnv  string        `yaml:"admin_token_env"`
}

// State selects the state-store adapter and its durability policy.
type State struct {
	Driver     string `yaml:"driver"`
	Path       string `yaml:"path"`
	SyncWrites *bool  `yaml:"sync_writes"`
}

// Passthrough configures delivery of events not claimed by a correlation rule.
type Passthrough struct {
	Enabled       bool          `yaml:"enabled"`
	Index         string        `yaml:"index"`
	Timestamp     string        `yaml:"timestamp"`
	BufferSize    int           `yaml:"buffer_size"`
	BatchSize     int           `yaml:"batch_size"`
	FlushInterval time.Duration `yaml:"flush_interval"`
}

// Alerts configures self-reported stuck-document diagnostics.
type Alerts struct {
	Enabled            bool          `yaml:"enabled"`
	Index              string        `yaml:"index"`
	MinInterval        time.Duration `yaml:"min_interval"`
	OutboxAgeThreshold time.Duration `yaml:"outbox_age_threshold"`
}

// Rule is the YAML representation of one correlation rule.
type Rule struct {
	ID          string               `yaml:"id"`
	Enabled     bool                 `yaml:"enabled"`
	Extract     Extract              `yaml:"extract"`
	Correlation Correlation          `yaml:"correlation"`
	Stitch      []Stitch             `yaml:"stitch"`
	Promote     map[string]Promotion `yaml:"promote"`
	Lifecycle   Lifecycle            `yaml:"lifecycle"`
	Limits      RuleLimits           `yaml:"limits"`
	Output      Output               `yaml:"output"`
}

// Promotion describes one typed output field lifted from a matched event.
type Promotion struct {
	Path string `yaml:"path"`
	Type string `yaml:"type"`
	From string `yaml:"from"`
}

// Extract selects the producer fields used for event type and time.
type Extract struct {
	EventType string `yaml:"event_type"`
	Timestamp string `yaml:"timestamp"`
}

// Correlation selects the field that identifies a flow.
type Correlation struct {
	Key string `yaml:"key"`
}

// Stitch describes how related event roles merge into one invocation.
type Stitch struct {
	ID       string    `yaml:"id"`
	GroupBy  []string  `yaml:"group_by"`
	Roles    yaml.Node `yaml:"roles"`
	Requires []string  `yaml:"requires"`
}

// Lifecycle bounds a flow and defines its completion conditions.
type Lifecycle struct {
	Timeout        time.Duration `yaml:"timeout"`
	CloseWhen      string        `yaml:"close_when"`
	Settle         time.Duration `yaml:"settle"`
	TerminalEvents []string      `yaml:"terminal_events"`
}

// RuleLimits bounds one flow's event count and encoded size.
type RuleLimits struct {
	MaxEvents     int   `yaml:"max_events"`
	MaxEventBytes int64 `yaml:"max_event_bytes"`
	MaxFlowBytes  int64 `yaml:"max_flow_bytes"`
}

// Output selects the destination index and document timestamp source.
type Output struct {
	Index     string `yaml:"index"`
	Timestamp string `yaml:"timestamp"`
}

// Sinks groups configuration for downstream delivery adapters.
type Sinks struct {
	OpenSearch OpenSearch `yaml:"opensearch"`
}

// OpenSearch configures bulk delivery to an OpenSearch cluster.
type OpenSearch struct {
	Addresses     []string      `yaml:"addresses"`
	UsernameEnv   string        `yaml:"username_env"`
	PasswordEnv   string        `yaml:"password_env"`
	TLSSkipVerify bool          `yaml:"tls_skip_verify"`
	BulkSize      int           `yaml:"bulk_size"`
	FlushInterval time.Duration `yaml:"flush_interval"`
	MaxRetries    int           `yaml:"max_retries"`
}

// Observability configures metrics and internal logging.
type Observability struct {
	MetricsPath string `yaml:"metrics_path"`
	LogLevel    string `yaml:"log_level"`
	LogFormat   string `yaml:"log_format"`
}

// Limits bounds process-wide state, backlog, and input size.
type Limits struct {
	MaxOpenFlows     int   `yaml:"max_open_flows"`
	MaxStateBytes    int64 `yaml:"max_state_bytes"`
	MaxOutboxRecords int   `yaml:"max_outbox_records"`
	MaxDLQRecords    int   `yaml:"max_dlq_records"`
	MaxEventBytes    int64 `yaml:"max_event_bytes"`
}

// Load decodes, defaults, and fully validates one YAML configuration file.
func Load(filename string) (*Config, error) {
	raw, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", filename, err)
	}
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", filename, err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ErrNoRules reports a configuration that cannot claim any correlated event.
var ErrNoRules = errors.New("config: at least one rule is required")

// Validate rejects invalid settings and conflicts between rules and indices.
func (c *Config) Validate() error {
	if len(c.Rules) == 0 {
		return ErrNoRules
	}
	if c.Limits.MaxDLQRecords < 1 {
		return errors.New("config: limits.max_dlq_records must be greater than zero")
	}
	if c.Passthrough.Enabled {
		if c.Passthrough.Index == "" {
			return errors.New("config: passthrough.index is required when passthrough is enabled")
		}
		if err := indexname.Validate(c.Passthrough.Index); err != nil {
			return fmt.Errorf("config: passthrough.index: %w", err)
		}
		if c.Passthrough.Timestamp != "" {
			if _, err := path.Compile(c.Passthrough.Timestamp); err != nil {
				return fmt.Errorf("config: passthrough.timestamp: %w", err)
			}
		}
		if c.Passthrough.BufferSize < 1 || c.Passthrough.BatchSize < 1 || c.Passthrough.BatchSize > c.Passthrough.BufferSize || c.Passthrough.FlushInterval <= 0 {
			return errors.New("config: passthrough buffer_size, batch_size and flush_interval must be positive, and batch_size must not exceed buffer_size")
		}
	}
	if c.Alerts.Enabled {
		if c.Alerts.Index == "" {
			return errors.New("config: alerts.index is required when alerts are enabled")
		}
		if err := indexname.Validate(c.Alerts.Index); err != nil {
			return fmt.Errorf("config: alerts.index: %w", err)
		}
		if c.Alerts.MinInterval <= 0 || c.Alerts.OutboxAgeThreshold <= 0 {
			return errors.New("config: alerts.min_interval and alerts.outbox_age_threshold must be positive")
		}
		if c.Passthrough.Enabled && c.Alerts.Index == c.Passthrough.Index {
			return errors.New("config: alerts.index must differ from passthrough.index")
		}
	}
	switch c.State.Driver {
	case "memory":
	case "pebble":
		if c.State.Path == "" {
			return errors.New("config: state.path is required when state.driver is \"pebble\"")
		}
	default:
		return fmt.Errorf("config: unsupported state.driver %q (want \"memory\" or \"pebble\")", c.State.Driver)
	}
	seenIDs := make(map[string]struct{}, len(c.Rules))
	keyOwners := make(map[string]string)
	type promotionOwner struct {
		ruleID   string
		typeName rule.PromotionType
	}
	promotedByIndex := make(map[string]map[string]promotionOwner)
	for _, raw := range c.Rules {
		if err := indexname.Validate(raw.Output.Index); err != nil {
			return fmt.Errorf("config: rule %q: output.index: %w", raw.ID, err)
		}
		if c.Alerts.Enabled && raw.Output.Index == c.Alerts.Index {
			return fmt.Errorf("config: rule %q output.index must differ from alerts.index", raw.ID)
		}
		if _, exists := seenIDs[raw.ID]; exists {
			return fmt.Errorf("config: duplicate rule id %q", raw.ID)
		}
		seenIDs[raw.ID] = struct{}{}
		domainRule, err := raw.toDomain("")
		if err != nil {
			return err
		}
		if err := domainRule.Validate(); err != nil {
			return fmt.Errorf("config: %w", err)
		}
		owners := promotedByIndex[raw.Output.Index]
		if owners == nil {
			owners = make(map[string]promotionOwner)
			promotedByIndex[raw.Output.Index] = owners
		}
		for _, promoted := range domainRule.Promote {
			if owner, exists := owners[promoted.Name]; exists && owner.typeName != promoted.Type {
				return fmt.Errorf("config: rules %q and %q promote field %q to different types %q and %q for output index %q", owner.ruleID, raw.ID, promoted.Name, owner.typeName, promoted.Type, raw.Output.Index)
			}
			owners[promoted.Name] = promotionOwner{ruleID: raw.ID, typeName: promoted.Type}
		}
		if raw.Enabled {
			key := domainRule.Key.Canonical()
			if owner, exists := keyOwners[key]; exists {
				return fmt.Errorf("config: correlation key path %q is shared by enabled rules %q and %q", key, owner, raw.ID)
			}
			keyOwners[key] = raw.ID
		}
	}
	return nil
}

// DomainRules compiles configured wire rules into immutable domain rules.
func (c *Config) DomainRules() ([]rule.Rule, error) {
	out := make([]rule.Rule, 0, len(c.Rules))
	for _, raw := range c.Rules {
		converted, err := raw.toDomain("")
		if err != nil {
			return nil, err
		}
		version, err := rule.ContentVersion(converted)
		if err != nil {
			return nil, err
		}
		converted.Version = version
		out = append(out, converted)
	}
	return out, nil
}

// PassthroughTimestamp compiles the optional timestamp selector.
func (c *Config) PassthroughTimestamp() (path.Path, error) {
	if c.Passthrough.Timestamp == "" {
		return path.Path{}, nil
	}
	return path.Compile(c.Passthrough.Timestamp)
}

func (r Rule) toDomain(version rule.Version) (rule.Rule, error) {
	compile := func(field, expression string, optional bool) (path.Path, error) {
		if optional && expression == "" {
			return path.Path{}, nil
		}
		compiled, err := path.Compile(expression)
		if err != nil {
			return path.Path{}, fmt.Errorf("config: rule %q: %s: %w", r.ID, field, err)
		}
		return compiled, nil
	}
	eventType, err := compile("extract.event_type", r.Extract.EventType, true)
	if err != nil {
		return rule.Rule{}, err
	}
	timestamp, err := compile("extract.timestamp", r.Extract.Timestamp, true)
	if err != nil {
		return rule.Rule{}, err
	}
	key, err := compile("correlation.key", r.Correlation.Key, false)
	if err != nil {
		return rule.Rule{}, err
	}
	stitches := make([]rule.Stitch, 0, len(r.Stitch))
	for _, rawStitch := range r.Stitch {
		stitch := rule.Stitch{ID: rawStitch.ID, Requires: rawStitch.Requires}
		for i, expression := range rawStitch.GroupBy {
			compiled, err := compile(fmt.Sprintf("stitch[%q].group_by[%d]", rawStitch.ID, i), expression, false)
			if err != nil {
				return rule.Rule{}, err
			}
			stitch.GroupBy = append(stitch.GroupBy, compiled)
		}
		roles, err := decodeRoles(rawStitch.Roles)
		if err != nil {
			return rule.Rule{}, fmt.Errorf("config: rule %q: stitch %q roles: %w", r.ID, rawStitch.ID, err)
		}
		stitch.Roles = roles
		if len(stitch.Requires) == 0 {
			for _, role := range roles {
				stitch.Requires = append(stitch.Requires, role.Name)
			}
		}
		stitches = append(stitches, stitch)
	}
	promotedNames := make([]string, 0, len(r.Promote))
	for name := range r.Promote {
		promotedNames = append(promotedNames, name)
	}
	sort.Strings(promotedNames)
	promotions := make([]rule.Promotion, 0, len(promotedNames))
	for _, name := range promotedNames {
		rawPromotion := r.Promote[name]
		compiled, err := compile(fmt.Sprintf("promote[%q].path", name), rawPromotion.Path, false)
		if err != nil {
			return rule.Rule{}, err
		}
		promotions = append(promotions, rule.Promotion{Name: name, Path: compiled, Type: rule.PromotionType(rawPromotion.Type), From: rawPromotion.From})
	}
	return rule.Rule{ID: rule.ID(r.ID), Version: version, Enabled: r.Enabled, Extract: rule.Extract{EventType: eventType, Timestamp: timestamp}, Key: key, Stitch: stitches,
		Promote:   promotions,
		Lifecycle: rule.Lifecycle{Timeout: r.Lifecycle.Timeout, CloseWhen: rule.CloseCondition(r.Lifecycle.CloseWhen), Settle: r.Lifecycle.Settle, TerminalEvents: r.Lifecycle.TerminalEvents},
		Limits:    rule.Limits{MaxEvents: r.Limits.MaxEvents, MaxEventBytes: r.Limits.MaxEventBytes, MaxFlowBytes: r.Limits.MaxFlowBytes},
		Output:    rule.Output{Index: r.Output.Index, TimestampSource: rule.TimestampSource(r.Output.Timestamp)}}, nil
}

func decodeRoles(node yaml.Node) ([]rule.Role, error) {
	if node.Kind == 0 {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, errors.New("must be a mapping")
	}
	roles := make([]rule.Role, 0, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		name := node.Content[i].Value
		value := node.Content[i+1]
		var types []string
		switch value.Kind {
		case yaml.ScalarNode:
			types = []string{value.Value}
		case yaml.SequenceNode:
			if err := value.Decode(&types); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("role %q must be a string or list of strings", name)
		}
		roles = append(roles, rule.Role{Name: name, Types: types})
	}
	return roles, nil
}

func (c *Config) applyDefaults() {
	if c.Server.Address == "" {
		c.Server.Address = ":8080"
	}
	if c.Server.ShutdownGrace == 0 {
		c.Server.ShutdownGrace = 15 * time.Second
	}
	if c.Server.MaxRequestSize == 0 {
		c.Server.MaxRequestSize = 4 << 20
	}
	if c.State.Driver == "" {
		c.State.Driver = "memory"
	}
	if c.State.SyncWrites == nil {
		enabled := true
		c.State.SyncWrites = &enabled
	}
	if c.Passthrough.BufferSize == 0 {
		c.Passthrough.BufferSize = 10000
	}
	if c.Passthrough.BatchSize == 0 {
		c.Passthrough.BatchSize = 500
	}
	if c.Passthrough.FlushInterval == 0 {
		c.Passthrough.FlushInterval = time.Second
	}
	if c.Alerts.MinInterval == 0 {
		c.Alerts.MinInterval = 5 * time.Minute
	}
	if c.Alerts.OutboxAgeThreshold == 0 {
		c.Alerts.OutboxAgeThreshold = 5 * time.Minute
	}
	if c.Observability.MetricsPath == "" {
		c.Observability.MetricsPath = "/metrics"
	}
	if c.Observability.LogLevel == "" {
		c.Observability.LogLevel = "info"
	}
	if c.Observability.LogFormat == "" {
		c.Observability.LogFormat = "json"
	}
	if c.Limits.MaxDLQRecords == 0 {
		c.Limits.MaxDLQRecords = 10000
	}
}

// SyncWritesEnabled reports whether state commits are acknowledged only after
// the WAL is synced. It defaults to true because it protects the ingestion
// acknowledgement guarantee (ADR-0006).
func (s State) SyncWritesEnabled() bool { return s.SyncWrites == nil || *s.SyncWrites }
