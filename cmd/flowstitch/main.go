// Command flowstitch runs the correlation engine as a single process.
//
// This file is the composition root: the only place that knows which adapter
// implements which port. Everything below it is wired through interfaces so
// the correlation core stays independent of transport, storage and sink
// (ADR-0002).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/adapters/config"
	"github.com/kraicdesign/flow-stitch/internal/adapters/httpapi"
	passthroughadapter "github.com/kraicdesign/flow-stitch/internal/adapters/passthrough"
	"github.com/kraicdesign/flow-stitch/internal/adapters/quarantine"
	"github.com/kraicdesign/flow-stitch/internal/adapters/rules"
	"github.com/kraicdesign/flow-stitch/internal/adapters/sink/opensearch"
	"github.com/kraicdesign/flow-stitch/internal/adapters/state/memory"
	pebblestate "github.com/kraicdesign/flow-stitch/internal/adapters/state/pebble"
	"github.com/kraicdesign/flow-stitch/internal/application"
	adminservice "github.com/kraicdesign/flow-stitch/internal/application/admin"
	alertservice "github.com/kraicdesign/flow-stitch/internal/application/alerts"
	"github.com/kraicdesign/flow-stitch/internal/application/deliver"
	"github.com/kraicdesign/flow-stitch/internal/application/expire"
	"github.com/kraicdesign/flow-stitch/internal/application/ingest"
	"github.com/kraicdesign/flow-stitch/internal/observability/health"
	"github.com/kraicdesign/flow-stitch/internal/observability/logging"
	"github.com/kraicdesign/flow-stitch/internal/observability/metrics"
)

// version is stamped at build time by the Makefile.
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "flowstitch: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) > 1 && os.Args[1] == "print-index-template" {
		return printIndexTemplate(os.Args[2:], os.Stdout)
	}
	configPath := flag.String("config", "config/flowstitch.example.yaml", "path to the configuration file")
	showVersion := flag.Bool("version", false, "print version and exit")
	validateOnly := flag.Bool("validate", false, "validate configuration and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return nil
	}

	// Configuration is fully validated before anything starts. An unsafe or
	// ambiguous rule must fail here, not at the first event (ADR-0004).
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if *validateOnly {
		fmt.Printf("configuration valid: %d rule(s)\n", len(cfg.Rules))
		return nil
	}

	logger, logLevel := logging.NewReloadable(os.Stdout, cfg.Observability.LogLevel, cfg.Observability.LogFormat)
	logger.Info("starting", slog.String("version", version), slog.String("config", *configPath))

	domainRules, err := cfg.DomainRules()
	if err != nil {
		return err
	}

	m := metrics.New()
	m.ConfigLoaded(time.Now().UTC())
	checks := health.New()
	clock := systemClock{}

	var store application.StateStore
	var capacity application.Capacity
	switch cfg.State.Driver {
	case "memory":
		logger.Warn("using non-durable in-memory state store: accepted events will not survive a restart")
		memoryStore := memory.New(cfg.Limits.MaxDLQRecords)
		store = memoryStore
	case "pebble":
		store, err = pebblestate.Open(cfg.State.Path, cfg.State.SyncWritesEnabled(), cfg.Limits.MaxDLQRecords)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("state driver %q passed validation but cannot be built", cfg.State.Driver)
	}
	capacity = storeCapacity{store: store, maxOpenFlows: cfg.Limits.MaxOpenFlows, maxOutboxRecords: cfg.Limits.MaxOutboxRecords}
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("state store close failed", slog.String("error", err.Error()))
		}
	}()
	openFlows, err := store.OpenFlows(context.Background())
	if err != nil {
		return fmt.Errorf("seed open-flow metrics: %w", err)
	}
	m.SeedOpenFlows(openFlows)
	deadLetters, err := store.DeadLetters(context.Background())
	if err != nil {
		return fmt.Errorf("seed dead-letter metrics: %w", err)
	}
	m.SeedDeadLetters(deadLetters)

	registry := rules.NewRegistry(domainRules)
	registry.SeedOpenFlows(openFlows)
	dlq := quarantine.NewLog(logger)
	timestamp, err := cfg.PassthroughTimestamp()
	if err != nil {
		return err
	}
	passthroughBuffer := passthroughadapter.New(passthroughadapter.Options{
		Index: cfg.Passthrough.Index, Timestamp: timestamp,
		BufferSize: cfg.Passthrough.BufferSize, BatchSize: cfg.Passthrough.BatchSize,
		FlushInterval: cfg.Passthrough.FlushInterval, Clock: clock, Recorder: m,
	})
	passthroughBuffer.Reconfigure(passthroughadapter.Options{
		Index: cfg.Passthrough.Index, Timestamp: timestamp,
		BufferSize: cfg.Passthrough.BufferSize, BatchSize: cfg.Passthrough.BatchSize,
		FlushInterval: cfg.Passthrough.FlushInterval, Clock: clock, Recorder: m,
	}, cfg.Passthrough.Enabled)
	ingestSvc := ingest.New(store, registry, dlq, capacity, clock, m, passthroughBuffer)
	const expiryBatch = 128
	expireSvc := expire.New(store, registry, clock, expiryBatch, m, logger)
	sink := opensearch.New(sinkOptions(cfg))
	deliverSvc := deliver.New(store, sink, dlq, clock, cfg.Sinks.OpenSearch.BulkSize, cfg.Sinks.OpenSearch.MaxRetries, m, passthroughBuffer)
	alertSvc := alertservice.New(alertservice.Options{
		Enabled: cfg.Alerts.Enabled, Index: cfg.Alerts.Index, MinInterval: cfg.Alerts.MinInterval,
		OutboxAgeThreshold: cfg.Alerts.OutboxAgeThreshold,
	}, deadLetters, sink, m, logger)
	deliverSvc.SetStuckReporter(alertSvc)
	adminSvc := adminservice.New(store, clock, m, alertSvc)
	adminToken := ""
	if cfg.Server.AdminTokenEnv != "" {
		adminToken = os.Getenv(cfg.Server.AdminTokenEnv)
	}
	if adminToken == "" {
		logger.Info("administration disabled")
	}

	checks.AddLiveness("process", health.Healthy)
	checks.AddReadiness("capacity", func(ctx context.Context) health.State {
		ok, reason := capacity.AcceptingEvents(ctx)
		return health.State{Healthy: ok, Detail: reason}
	})
	checks.AddReadiness("state", func(_ context.Context) health.State {
		return health.State{Healthy: true, Detail: cfg.State.Driver}
	})

	server := httpapi.New(httpapi.Options{
		Address:        cfg.Server.Address,
		ReadTimeout:    cfg.Server.ReadTimeout,
		WriteTimeout:   cfg.Server.WriteTimeout,
		MaxRequestSize: cfg.Server.MaxRequestSize,
		MetricsPath:    cfg.Observability.MetricsPath,
		AdminToken:     adminToken,
	}, ingestSvc, checks, m, logger, adminSvc)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	reloader := &configReloader{
		path: *configPath, boot: cfg, registry: registry, passthrough: passthroughBuffer,
		logLevel: logLevel, recorder: m, clock: clock, logger: logger, alerts: alertSvc,
	}

	// Flows already past their deadline finalize before normal ingestion
	// resumes. With a durable store this makes timeout behaviour deterministic
	// across restarts.
	for {
		count, err := expireSvc.Sweep(ctx, clock.Now())
		if err != nil {
			return fmt.Errorf("startup expiry sweep: %w", err)
		}
		if count < expiryBatch {
			break
		}
	}

	errs := make(chan error, 3)
	go func() { errs <- server.ListenAndServe() }()
	go func() { errs <- ignoreCancelled(expireSvc.Run(ctx, time.Second)) }()
	go func() { errs <- ignoreCancelled(deliverSvc.Run(ctx, cfg.Sinks.OpenSearch.FlushInterval)) }()

	running := true
	for running {
		select {
		case <-ctx.Done():
			logger.Info("shutdown signal received")
			running = false
		case <-hup:
			_ = reloader.Reload()
		case err := <-errs:
			if err != nil {
				logger.Error("component failed", slog.String("error", err.Error()))
			}
			running = false
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownGrace)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}

func printIndexTemplate(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("print-index-template", flag.ContinueOnError)
	configPath := flags.String("config", "config/flowstitch.example.yaml", "path to the configuration file")
	index := flags.String("index", "", "print only the API-ready template for this configured output index")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	rules, err := cfg.DomainRules()
	if err != nil {
		return err
	}
	var raw []byte
	if *index == "" {
		if cfg.Alerts.Enabled {
			raw, err = opensearch.MarshalTemplates(rules, cfg.Alerts.Index)
		} else {
			raw, err = opensearch.MarshalTemplates(rules)
		}
	} else {
		if cfg.Alerts.Enabled {
			raw, err = opensearch.MarshalTemplate(rules, *index, cfg.Alerts.Index)
		} else {
			raw, err = opensearch.MarshalTemplate(rules, *index)
		}
	}
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(stdout, string(raw)); err != nil {
		return fmt.Errorf("print index template: %w", err)
	}
	return nil
}

func sinkOptions(cfg *config.Config) opensearch.Options {
	return opensearch.Options{
		Addresses:     cfg.Sinks.OpenSearch.Addresses,
		Username:      os.Getenv(cfg.Sinks.OpenSearch.UsernameEnv),
		Password:      os.Getenv(cfg.Sinks.OpenSearch.PasswordEnv),
		TLSSkipVerify: cfg.Sinks.OpenSearch.TLSSkipVerify,
		BulkSize:      cfg.Sinks.OpenSearch.BulkSize,
		FlushInterval: cfg.Sinks.OpenSearch.FlushInterval,
		MaxRetries:    cfg.Sinks.OpenSearch.MaxRetries,
	}
}

func ignoreCancelled(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// systemClock is the production [application.Clock].
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

// storeCapacity is the ingestion high-water check.
type storeCapacity struct {
	store            application.StateStore
	maxOpenFlows     int
	maxOutboxRecords int
}

func (c storeCapacity) AcceptingEvents(ctx context.Context) (bool, string) {
	counts, err := c.store.OpenFlows(ctx)
	if err != nil {
		return false, "open flow count unavailable"
	}
	total := 0
	for _, count := range counts {
		total += count
	}
	if c.maxOpenFlows > 0 && total >= c.maxOpenFlows {
		return false, fmt.Sprintf("open flow limit reached (%d)", c.maxOpenFlows)
	}
	outboxRecords, err := c.store.OutboxRecords(ctx)
	if err != nil {
		return false, "outbox depth unavailable"
	}
	if c.maxOutboxRecords > 0 && outboxRecords >= c.maxOutboxRecords {
		return false, fmt.Sprintf("outbox record limit reached (%d)", c.maxOutboxRecords)
	}
	// TODO(contracts): byte-based enforcement waits on an answer to "at what disk
	// and state thresholds must ingestion stop?" — see the open questions in
	// docs/adr/README.md. Record counts are enforced above; bytes are not.
	return true, ""
}
