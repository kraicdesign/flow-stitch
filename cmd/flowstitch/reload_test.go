package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/adapters/config"
	passthroughadapter "github.com/kraicdesign/flow-stitch/internal/adapters/passthrough"
	"github.com/kraicdesign/flow-stitch/internal/adapters/rules"
	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
	"github.com/kraicdesign/flow-stitch/internal/observability/logging"
	"github.com/kraicdesign/flow-stitch/internal/observability/metrics"
)

type reloadClock struct{ now time.Time }

func (c reloadClock) Now() time.Time { return c.now }

func TestValidReloadPublishesRulesAndReloadableSettings(t *testing.T) {
	fixture := newReloadFixture(t, reloadYAML(":8080", true, "$.flow_id", false))
	writeReloadConfig(t, fixture.path, reloadYAML(":9090", false, "$.trace_id", true))
	if err := fixture.reloader.Reload(); err != nil {
		t.Fatal(err)
	}
	configured, ok := fixture.registry.MatchAndRetain(event.Event{Doc: map[string]any{"trace_id": "new"}})
	if !ok || configured.Key.String() != "$.trace_id" {
		t.Fatalf("new rule match = (%q, %v), want $.trace_id", configured.Key.String(), ok)
	}
	fixture.registry.Release(rule.Reference{ID: configured.ID, Version: configured.Version})
	if err := fixture.buffer.Accept(event.Event{Doc: map[string]any{"message": "ordinary"}, Raw: []byte(`{"message":"ordinary"}`)}); err != nil {
		t.Fatalf("reloaded pass-through Accept() = %v", err)
	}
	if got := fixture.buffer.Pending()[0].Index; got != "logs" {
		t.Fatalf("pass-through index = %q, want logs", got)
	}
	fixture.logger.Debug("debug after reload")
	logs := fixture.logs.String()
	for _, want := range []string{"server.address", "state.sync_writes", "debug after reload"} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs do not name %q: %s", want, logs)
		}
	}
	if got := metricValue(t, fixture.metrics, "flowstitch_config_reloads_total", "success"); got != 1 {
		t.Fatalf("success reloads = %v, want 1", got)
	}
	if got := metricValue(t, fixture.metrics, "flowstitch_config_loaded_timestamp_seconds", ""); got != float64(fixture.clock.now.Unix()) {
		t.Fatalf("loaded timestamp = %v, want %d", got, fixture.clock.now.Unix())
	}
}

func TestInvalidReloadKeepsCurrentRulesAndCountsFailure(t *testing.T) {
	fixture := newReloadFixture(t, reloadYAML(":8080", true, "$.flow_id", false))
	writeReloadConfig(t, fixture.path, strings.Replace(reloadYAML(":8080", true, "$.trace_id", false), "timeout: 10s", "timeout: invalid", 1))
	if err := fixture.reloader.Reload(); err == nil {
		t.Fatal("Reload() = nil, want validation error")
	}
	configured, ok := fixture.registry.MatchAndRetain(event.Event{Doc: map[string]any{"flow_id": "still-active"}})
	if !ok || configured.Key.String() != "$.flow_id" {
		t.Fatalf("running rule changed after failed reload: (%q, %v)", configured.Key.String(), ok)
	}
	fixture.registry.Release(rule.Reference{ID: configured.ID, Version: configured.Version})
	if got := metricValue(t, fixture.metrics, "flowstitch_config_reloads_total", "failure"); got != 1 {
		t.Fatalf("failed reloads = %v, want 1", got)
	}
	logs := fixture.logs.String()
	if !strings.Contains(logs, fixture.path) || !strings.Contains(logs, "invalid") {
		t.Fatalf("failure log does not name file and problem: %s", logs)
	}
}

type reloadFixture struct {
	path     string
	reloader *configReloader
	registry *rules.Registry
	buffer   *passthroughadapter.Buffer
	metrics  *metrics.Metrics
	logger   interface{ Debug(string, ...any) }
	logs     *bytes.Buffer
	clock    reloadClock
}

func newReloadFixture(t *testing.T, raw string) reloadFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "flowstitch.yaml")
	writeReloadConfig(t, path, raw)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	domainRules, err := cfg.DomainRules()
	if err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	logger, level := logging.NewReloadable(logs, cfg.Observability.LogLevel, "text")
	m := metrics.New()
	clock := reloadClock{now: time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)}
	buffer := passthroughadapter.New(passthroughadapter.Options{BufferSize: cfg.Passthrough.BufferSize, BatchSize: cfg.Passthrough.BatchSize, FlushInterval: cfg.Passthrough.FlushInterval, Clock: clock, Recorder: m})
	buffer.Reconfigure(passthroughadapter.Options{BufferSize: cfg.Passthrough.BufferSize, BatchSize: cfg.Passthrough.BatchSize, FlushInterval: cfg.Passthrough.FlushInterval, Clock: clock, Recorder: m}, false)
	registry := rules.NewRegistry(domainRules)
	reloader := &configReloader{path: path, boot: cfg, registry: registry, passthrough: buffer, logLevel: level, recorder: m, clock: clock, logger: logger}
	return reloadFixture{path: path, reloader: reloader, registry: registry, buffer: buffer, metrics: m, logger: logger, logs: logs, clock: clock}
}

func metricValue(t *testing.T, m *metrics.Metrics, name, result string) float64 {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if result != "" {
				matched := false
				for _, label := range metric.Label {
					matched = label.GetName() == "result" && label.GetValue() == result
				}
				if !matched {
					continue
				}
			}
			if metric.Counter != nil {
				return metric.Counter.GetValue()
			}
			return metric.Gauge.GetValue()
		}
	}
	return 0
}

func reloadYAML(address string, syncWrites bool, key string, passthrough bool) string {
	return fmt.Sprintf("server:\n  address: %s\nstate:\n  driver: memory\n  sync_writes: %t\nobservability:\n  log_level: %s\npassthrough:\n  enabled: %t\n  index: logs\nrules:\n  - id: reload-rule\n    enabled: true\n    correlation:\n      key: %s\n    lifecycle:\n      timeout: 10s\n    output:\n      index: flows\n", address, syncWrites, map[bool]string{true: "debug", false: "info"}[passthrough], passthrough, key)
}

func writeReloadConfig(t *testing.T, path, raw string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
}

var _ application.Clock = reloadClock{}
