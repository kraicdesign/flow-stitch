package config_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/adapters/config"
)

func TestLoadExampleConfigAndCompileEveryPath(t *testing.T) {
	filename := filepath.Join("..", "..", "..", "config", "flowstitch.example.yaml")
	cfg, err := config.Load(filename)
	if err != nil {
		t.Fatalf("Load(%s) = %v, want nil", filename, err)
	}
	rules, err := cfg.DomainRules()
	if err != nil {
		t.Fatalf("DomainRules() = %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("len(rules) = %d, want 2", len(rules))
	}
	for _, configured := range rules {
		if configured.Key.String() == "" {
			t.Errorf("rule %q correlation path was not compiled", configured.ID)
		}
		for _, stitch := range configured.Stitch {
			for _, group := range stitch.GroupBy {
				if group.String() == "" {
					t.Errorf("rule %q group_by path was not compiled", configured.ID)
				}
			}
		}
	}
	application := rules[0]
	if got, want := application.Stitch[0].Requires, []string{"request", "response"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("default requires = %v, want role order %v", got, want)
	}
	if len(application.Promote) != 1 || application.Promote[0].Name != "status" || application.Promote[0].Path.String() != "$.payload.status" {
		t.Fatalf("example promotions = %+v, want compiled status", application.Promote)
	}
}

func TestRuleVersionIsStableAcrossYAMLMapOrderAndWhitespace(t *testing.T) {
	first := "rules:\n" + indent(ruleYAML("one", "$.flow_id", "    stitch:\n      - id: call\n        group_by: [$.service]\n        roles:\n          request: [start, begin]\n          response: finish\n"), 2)
	second := "rules:\n" + indent(ruleYAML("one", "$.flow_id", "    stitch:\n      - id: call\n        group_by: [ $.service ]\n        roles:\n          response: finish\n          request: [begin, start]\n"), 2)
	load := func(raw string) string {
		filename := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(filename, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := config.Load(filename)
		if err != nil {
			t.Fatal(err)
		}
		rules, err := cfg.DomainRules()
		if err != nil {
			t.Fatal(err)
		}
		return string(rules[0].Version)
	}
	if got, want := load(second), load(first); got != want || got == "" {
		t.Fatalf("versions = %q and %q, want identical non-empty hashes", got, want)
	}
}

func TestValidateRejectsEmptyConfig(t *testing.T) {
	var cfg config.Config
	if err := cfg.Validate(); !errors.Is(err, config.ErrNoRules) {
		t.Fatalf("Validate() = %v, want %v", err, config.ErrNoRules)
	}
}

func TestDuplicateEnabledCorrelationPathsAreRejected(t *testing.T) {
	yaml := ruleYAML("one", "$.flow_id", "") + ruleYAML("two", "$.flow_id", "")
	err := loadYAML(t, "rules:\n"+indent(yaml, 2))
	if err == nil || !strings.Contains(err.Error(), "shared by enabled rules") {
		t.Fatalf("Load() = %v, want duplicate correlation path error", err)
	}
}

func TestRequiresMustNameDeclaredRole(t *testing.T) {
	stitch := "    stitch:\n      - id: call\n        group_by: [$.invocation_id]\n        roles:\n          request: http.request\n        requires: [response]\n"
	err := loadYAML(t, "rules:\n"+indent(ruleYAML("one", "$.flow_id", stitch), 2))
	if err == nil || !strings.Contains(err.Error(), "undeclared role") {
		t.Fatalf("Load() = %v, want undeclared role error", err)
	}
}

func TestStitchRulesCannotClaimSameEventType(t *testing.T) {
	stitch := "    stitch:\n      - id: first\n        group_by: [$.one]\n        roles:\n          request: http.request\n      - id: second\n        group_by: [$.two]\n        roles:\n          other: http.request\n"
	err := loadYAML(t, "rules:\n"+indent(ruleYAML("one", "$.flow_id", stitch), 2))
	if err == nil || !strings.Contains(err.Error(), "claimed by stitch rules") {
		t.Fatalf("Load() = %v, want overlapping stitch type error", err)
	}
}

func TestRemovedKeysFailStrictLoad(t *testing.T) {
	for _, removed := range []string{"start_on: [http.request]", "complete_when: {expression: x}", "late_event_policy: quarantine"} {
		t.Run(strings.Split(removed, ":")[0], func(t *testing.T) {
			extra := "    lifecycle:\n      timeout: 10s\n      " + removed + "\n"
			err := loadYAML(t, "rules:\n"+indent(ruleYAMLWithoutLifecycle("one", "$.flow_id", extra), 2))
			if err == nil {
				t.Fatalf("Load() accepted removed key %q", removed)
			}
		})
	}
}

func TestMalformedPathFailsLoad(t *testing.T) {
	err := loadYAML(t, "rules:\n"+indent(ruleYAML("one", "flow_id", ""), 2))
	if err == nil || !strings.Contains(err.Error(), "flow_id") {
		t.Fatalf("Load() = %v, want malformed path naming expression", err)
	}
}

func TestUnknownIndexPlaceholderFailsLoad(t *testing.T) {
	yaml := strings.Replace(ruleYAML("one", "$.flow_id", ""), "index: flows", "index: flows-{hour}", 1)
	err := loadYAML(t, "rules:\n"+indent(yaml, 2))
	if err == nil || !strings.Contains(err.Error(), "unknown placeholder") {
		t.Fatalf("Load() = %v, want unknown placeholder error", err)
	}
}

func TestPromotionRejectsUnknownTypeAndUndeclaredRole(t *testing.T) {
	for _, test := range []struct {
		name, promotion string
		want            []string
	}{
		{"unknown type", "    promote:\n      status: {path: $.payload.status, type: integer}\n", []string{"status", "integer"}},
		{"undeclared role", "    promote:\n      status: {path: $.payload.status, type: long, from: missing}\n", []string{"status", "missing"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := loadYAML(t, "rules:\n"+indent(ruleYAML("one", "$.flow_id", test.promotion), 2))
			if err == nil {
				t.Fatal("Load() = nil, want promotion validation error")
			}
			for _, want := range test.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("Load() = %v, want error naming %q", err, want)
				}
			}
		})
	}
}

func TestPromotionRejectsConflictingTypesForSharedIndex(t *testing.T) {
	one := ruleYAML("one", "$.flow_one", "    promote:\n      status: {path: $.payload.status, type: long}\n")
	two := ruleYAML("two", "$.flow_two", "    promote:\n      status: {path: $.payload.status, type: keyword}\n")
	err := loadYAML(t, "rules:\n"+indent(one+two, 2))
	if err == nil || !strings.Contains(err.Error(), "one") || !strings.Contains(err.Error(), "two") || !strings.Contains(err.Error(), "status") {
		t.Fatalf("Load() = %v, want both rules and field", err)
	}
}

func TestPebbleRequiresPath(t *testing.T) {
	yaml := "state:\n  driver: pebble\nrules:\n" + indent(ruleYAML("one", "$.flow_id", ""), 2)
	err := loadYAML(t, yaml)
	if err == nil || !strings.Contains(err.Error(), "state.path is required") {
		t.Fatalf("Load() = %v, want missing Pebble path error", err)
	}
}

func TestStateSyncWritesDefaultsTrueAndCanBeDisabled(t *testing.T) {
	base := "state:\n  driver: memory\n%srules:\n" + indent(ruleYAML("one", "$.flow_id", ""), 2)
	for _, test := range []struct {
		name, setting string
		want          bool
	}{{"default", "", true}, {"disabled", "  sync_writes: false\n", false}} {
		t.Run(test.name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(filename, []byte(fmt.Sprintf(base, test.setting)), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(filename)
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.State.SyncWritesEnabled(); got != test.want {
				t.Fatalf("SyncWritesEnabled() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestDeadLetterLimitDefaultsAndRejectsInvalidValue(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "config.yaml")
	raw := "state:\n  driver: memory\nrules:\n" + indent(ruleYAML("one", "$.flow_id", ""), 2)
	if err := os.WriteFile(filename, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(filename)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Limits.MaxDLQRecords != 10000 {
		t.Fatalf("max_dlq_records = %d, want default 10000", cfg.Limits.MaxDLQRecords)
	}
	invalid := "limits:\n  max_dlq_records: -1\n" + raw
	if err := os.WriteFile(filename, []byte(invalid), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(filename); err == nil || !strings.Contains(err.Error(), "max_dlq_records") {
		t.Fatalf("Load() = %v, want max_dlq_records validation error", err)
	}
}

func TestAdminConfigurationNamesEnvironmentVariableAndRejectsInlineToken(t *testing.T) {
	base := "server:\n  admin_token_env: FLOWSTITCH_ADMIN_TOKEN\nrules:\n" + indent(ruleYAML("one", "$.flow_id", ""), 2)
	filename := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(filename, []byte(base), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(filename)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.AdminTokenEnv != "FLOWSTITCH_ADMIN_TOKEN" {
		t.Fatalf("admin token env = %q", cfg.Server.AdminTokenEnv)
	}
	inline := strings.Replace(base, "  admin_token_env: FLOWSTITCH_ADMIN_TOKEN", "  admin_token: plaintext-secret", 1)
	if err := os.WriteFile(filename, []byte(inline), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(filename); err == nil || !strings.Contains(err.Error(), "admin_token") {
		t.Fatalf("Load() = %v, want inline token rejection", err)
	}
}

func TestPassthroughDefaultsCompileAndValidation(t *testing.T) {
	base := "passthrough:\n  enabled: true\n  index: logs-{yyyy}.{MM}.{dd}\n  timestamp: $.datetime\nstate:\n  driver: memory\nrules:\n" + indent(ruleYAML("one", "$.flow_id", ""), 2)
	filename := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(filename, []byte(base), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(filename)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Passthrough.BufferSize != 10000 || cfg.Passthrough.BatchSize != 500 || cfg.Passthrough.FlushInterval != time.Second {
		t.Fatalf("passthrough defaults = %+v", cfg.Passthrough)
	}
	if compiled, err := cfg.PassthroughTimestamp(); err != nil || compiled.String() != "$.datetime" {
		t.Fatalf("PassthroughTimestamp() = %q, %v", compiled.String(), err)
	}
	for _, invalid := range []string{
		strings.Replace(base, "  index: logs-{yyyy}.{MM}.{dd}\n", "", 1),
		strings.Replace(base, "$.datetime", "datetime", 1),
		strings.Replace(base, "logs-{yyyy}.{MM}.{dd}", "logs-{hour}", 1),
	} {
		if err := os.WriteFile(filename, []byte(invalid), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := config.Load(filename); err == nil {
			t.Fatal("Load() accepted invalid pass-through config")
		}
	}
}

func TestAlertsDefaultsAndValidation(t *testing.T) {
	base := "alerts:\n  enabled: true\n  index: flowstitch-alerts-{yyyy}.{MM}.{dd}\nrules:\n" + indent(ruleYAML("one", "$.flow_id", ""), 2)
	filename := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(filename, []byte(base), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(filename)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Alerts.MinInterval != 5*time.Minute || cfg.Alerts.OutboxAgeThreshold != 5*time.Minute {
		t.Fatalf("alert defaults = %+v", cfg.Alerts)
	}
	for _, invalid := range []string{
		strings.Replace(base, "  index: flowstitch-alerts-{yyyy}.{MM}.{dd}\n", "", 1),
		strings.Replace(base, "flowstitch-alerts-{yyyy}.{MM}.{dd}", "flows", 1),
		strings.Replace(base, "  enabled: true\n", "  enabled: true\n  min_interval: -1s\n", 1),
	} {
		if err := os.WriteFile(filename, []byte(invalid), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := config.Load(filename); err == nil {
			t.Fatalf("Load() accepted invalid alerts config:\n%s", invalid)
		}
	}
}

func ruleYAML(id, key, extra string) string {
	base := ruleYAMLWithoutLifecycle(id, key, "    lifecycle:\n      timeout: 10s\n")
	marker := "    lifecycle:\n"
	return strings.Replace(base, marker, extra+marker, 1)
}

func ruleYAMLWithoutLifecycle(id, key, lifecycle string) string {
	return "  - id: " + id + "\n    enabled: true\n    extract:\n      event_type: $.event\n      timestamp: $.datetime\n    correlation:\n      key: " + key + "\n" + lifecycle + "    limits:\n      max_events: 32\n    output:\n      index: flows\n"
}

func indent(value string, spaces int) string {
	prefix := strings.Repeat(" ", spaces)
	return prefix + strings.ReplaceAll(strings.TrimSuffix(value, "\n"), "\n", "\n"+prefix) + "\n"
}

func loadYAML(t *testing.T, value string) error {
	t.Helper()
	filename := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(filename, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := config.Load(filename)
	return err
}
