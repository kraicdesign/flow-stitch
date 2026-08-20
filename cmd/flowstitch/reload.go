package main

import (
	"fmt"
	"log/slog"

	"github.com/kraicdesign/flow-stitch/internal/adapters/config"
	passthroughadapter "github.com/kraicdesign/flow-stitch/internal/adapters/passthrough"
	"github.com/kraicdesign/flow-stitch/internal/adapters/rules"
	"github.com/kraicdesign/flow-stitch/internal/application"
	alertservice "github.com/kraicdesign/flow-stitch/internal/application/alerts"
	"github.com/kraicdesign/flow-stitch/internal/observability/logging"
	"github.com/kraicdesign/flow-stitch/internal/observability/metrics"
)

type configReloader struct {
	path        string
	boot        *config.Config
	registry    *rules.Registry
	passthrough *passthroughadapter.Buffer
	logLevel    *logging.LevelController
	recorder    *metrics.Metrics
	clock       application.Clock
	logger      *slog.Logger
	alerts      *alertservice.Service
}

func (r *configReloader) Reload() error {
	next, err := config.Load(r.path)
	if err != nil {
		r.recorder.ConfigReload("failure")
		r.logger.Error("configuration reload failed", slog.String("config", r.path), slog.String("error", err.Error()))
		return err
	}
	domainRules, err := next.DomainRules()
	if err != nil {
		r.recorder.ConfigReload("failure")
		r.logger.Error("configuration reload failed", slog.String("config", r.path), slog.String("error", err.Error()))
		return err
	}
	timestamp, err := next.PassthroughTimestamp()
	if err != nil {
		r.recorder.ConfigReload("failure")
		r.logger.Error("configuration reload failed", slog.String("config", r.path), slog.String("error", err.Error()))
		return err
	}

	r.logIgnoredBootChanges(next)
	r.registry.Publish(domainRules)
	r.passthrough.Reconfigure(passthroughadapter.Options{
		Index: next.Passthrough.Index, Timestamp: timestamp,
		BufferSize: next.Passthrough.BufferSize, BatchSize: next.Passthrough.BatchSize,
		FlushInterval: next.Passthrough.FlushInterval, Clock: r.clock, Recorder: r.recorder,
	}, next.Passthrough.Enabled)
	r.logLevel.Set(next.Observability.LogLevel)
	if r.alerts != nil {
		r.alerts.Reconfigure(alertservice.Options{
			Enabled: next.Alerts.Enabled, Index: next.Alerts.Index, MinInterval: next.Alerts.MinInterval,
			OutboxAgeThreshold: next.Alerts.OutboxAgeThreshold,
		})
	}
	loadedAt := r.clock.Now()
	r.recorder.ConfigReload("success")
	r.recorder.ConfigLoaded(loadedAt)
	r.logger.Info("configuration reloaded", slog.String("config", r.path), slog.Int("rules", len(domainRules)))
	return nil
}

func (r *configReloader) logIgnoredBootChanges(next *config.Config) {
	ignored := func(name string, oldValue, newValue any) {
		if fmt.Sprint(oldValue) == fmt.Sprint(newValue) {
			return
		}
		r.logger.Warn("boot-only configuration change ignored", slog.String("setting", name), slog.Any("running", oldValue), slog.Any("requested", newValue))
	}
	ignored("server.address", r.boot.Server.Address, next.Server.Address)
	ignored("server.admin_token_env", r.boot.Server.AdminTokenEnv, next.Server.AdminTokenEnv)
	ignored("state.driver", r.boot.State.Driver, next.State.Driver)
	ignored("state.path", r.boot.State.Path, next.State.Path)
	ignored("state.sync_writes", r.boot.State.SyncWritesEnabled(), next.State.SyncWritesEnabled())
}
