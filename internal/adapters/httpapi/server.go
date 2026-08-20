// Package httpapi is the HTTP ingress adapter and operational surface.
//
// It processes each submitted batch and returns after its flow-state writes.
// A response never waits for the flow to finish or for OpenSearch to accept
// the finalized document (ADR-0003, section 4).
package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/application"
	adminservice "github.com/kraicdesign/flow-stitch/internal/application/admin"
	"github.com/kraicdesign/flow-stitch/internal/application/ingest"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
	"github.com/kraicdesign/flow-stitch/internal/observability/health"
	"github.com/kraicdesign/flow-stitch/internal/observability/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Options configures the server.
type Options struct {
	Address        string
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	MaxRequestSize int64
	MetricsPath    string
	AdminToken     string
}

// Server exposes ingestion, health and metrics endpoints.
type Server struct {
	opts    Options
	ingest  *ingest.Service
	health  *health.Registry
	metrics *metrics.Metrics
	logger  *slog.Logger
	http    *http.Server
	admin   *adminservice.Service
}

// New builds the server and its routes.
func New(opts Options, svc *ingest.Service, checks *health.Registry, m *metrics.Metrics, logger *slog.Logger, administrative ...*adminservice.Service) *Server {
	if opts.MetricsPath == "" {
		opts.MetricsPath = "/metrics"
	}
	if opts.MaxRequestSize <= 0 {
		opts.MaxRequestSize = 4 << 20
	}

	s := &Server{opts: opts, ingest: svc, health: checks, metrics: m, logger: logger}
	if len(administrative) > 0 {
		s.admin = administrative[0]
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", s.handleEvent)
	mux.HandleFunc("POST /v1/events/batch", s.handleBatch)
	mux.HandleFunc("GET /health/live", s.handleLive)
	mux.HandleFunc("GET /health/ready", s.handleReady)
	mux.Handle("GET "+opts.MetricsPath, promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
	if opts.AdminToken != "" && s.admin != nil {
		mux.Handle("GET /v1/admin/dlq", s.authenticate(http.HandlerFunc(s.handleDLQList)))
		mux.Handle("GET /v1/admin/dlq/{output_id}", s.authenticate(http.HandlerFunc(s.handleDLQGet)))
		mux.Handle("POST /v1/admin/dlq/replay", s.authenticate(http.HandlerFunc(s.handleDLQReplay)))
	}

	s.http = &http.Server{
		Addr:         opts.Address,
		Handler:      mux,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.WriteTimeout,
	}
	return s
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		presentedHash := sha256.Sum256([]byte(presented))
		expectedHash := sha256.Sum256([]byte(s.opts.AdminToken))
		if !found || subtle.ConstantTimeCompare(presentedHash[:], expectedHash[:]) != 1 {
			s.logger.Warn("admin authentication failed", slog.String("method", r.Method))
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "unauthorized", "")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleDLQList(w http.ResponseWriter, r *http.Request) {
	limit, err := boundedLimit(r.URL.Query().Get("limit"), 50, 500)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid limit", err.Error())
		return
	}
	filter := outbox.DeadLetterFilter{ReasonType: r.URL.Query().Get("reason_type"), Index: r.URL.Query().Get("index"), Limit: limit}
	summary, page, err := s.admin.List(r.Context(), filter, projection.OutputID(r.URL.Query().Get("cursor")))
	if err != nil {
		s.logger.Error("admin dead-letter listing failed", slog.String("error", err.Error()))
		writeError(w, http.StatusInternalServerError, "dead-letter listing failed", "")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"summary": summary, "records": page.Records, "next_cursor": page.NextCursor})
}

func (s *Server) handleDLQGet(w http.ResponseWriter, r *http.Request) {
	record, found, err := s.admin.Get(r.Context(), projection.OutputID(r.PathValue("output_id")))
	if err != nil {
		s.logger.Error("admin dead-letter fetch failed", slog.String("error", err.Error()))
		writeError(w, http.StatusInternalServerError, "dead-letter fetch failed", "")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "dead-letter record not found", "")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"metadata": record.Metadata(), "document": json.RawMessage(record.Document)})
}

type replayRequest struct {
	OutputID   projection.OutputID `json:"output_id"`
	ReasonType string              `json:"reason_type"`
	Index      string              `json:"index"`
	OlderThan  time.Time           `json:"older_than"`
	Limit      int                 `json:"limit"`
	DryRun     *bool               `json:"dry_run"`
}

func (s *Server) handleDLQReplay(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, s.opts.MaxRequestSize)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	var request replayRequest
	if err := dec.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid replay request", err.Error())
		return
	}
	limit := request.Limit
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		writeError(w, http.StatusBadRequest, "invalid limit", "must be between 1 and 1000")
		return
	}
	dryRun := request.OutputID == ""
	if request.DryRun != nil {
		dryRun = *request.DryRun
	}
	filter := outbox.DeadLetterFilter{OutputID: request.OutputID, ReasonType: request.ReasonType, Index: request.Index, OlderThan: request.OlderThan, Limit: limit}
	records, err := s.admin.Replay(r.Context(), filter, dryRun)
	if err != nil {
		s.logger.Error("admin dead-letter replay failed", slog.String("error", err.Error()))
		writeError(w, http.StatusInternalServerError, "dead-letter replay failed", "")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"dry_run": dryRun, "matched": len(records), "records": records})
}

func boundedLimit(raw string, fallback, maximum int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > maximum {
		return 0, errors.New("must be a positive integer within the endpoint maximum")
	}
	return value, nil
}

// ListenAndServe runs until the server is shut down.
func (s *Server) ListenAndServe() error {
	s.logger.Info("http listening", slog.String("address", s.opts.Address))
	if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Handler exposes the complete HTTP surface to in-process end-to-end tests and embedders.
func (s *Server) Handler() http.Handler { return s.http.Handler }

// Shutdown stops accepting connections and drains in-flight requests.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func (s *Server) handleEvent(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, s.opts.MaxRequestSize)

	raw, err := io.ReadAll(body)
	if err != nil {
		s.metrics.EventRejected("malformed json")
		writeError(w, http.StatusBadRequest, "malformed json", err.Error())
		return
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil || doc == nil {
		s.metrics.EventRejected("malformed json")
		writeError(w, http.StatusBadRequest, "malformed json", errorDetail(err))
		return
	}

	result, err := s.ingest.Accept(r.Context(), event.Event{Doc: doc, Raw: raw, ObservedAt: time.Now().UTC()})
	if err != nil {
		s.writeIngestError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"disposition": string(result.Disposition),
		"reason":      result.Reason,
	})
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, s.opts.MaxRequestSize)

	var rawDocs []json.RawMessage
	if err := json.NewDecoder(body).Decode(&rawDocs); err != nil {
		s.metrics.EventRejected("malformed json")
		writeError(w, http.StatusBadRequest, "malformed json", err.Error())
		return
	}

	now := time.Now().UTC()

	// TODO(contracts): partial-batch acknowledgement semantics with Fluent Bit
	// are not decided. Pass-through backpressure is collected so later correlated
	// events in the same batch still reach durable state before the retryable response.
	results := make([]map[string]any, 0, len(rawDocs))
	var passthroughBackpressure error
	for _, raw := range rawDocs {
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil || doc == nil {
			s.metrics.EventRejected("malformed json")
			writeError(w, http.StatusBadRequest, "malformed json", errorDetail(err))
			return
		}
		result, err := s.ingest.Accept(r.Context(), event.Event{Doc: doc, Raw: raw, ObservedAt: now})
		if err != nil {
			if errors.Is(err, application.ErrPassthroughFull) {
				passthroughBackpressure = err
				continue
			}
			s.writeIngestError(w, err)
			return
		}
		results = append(results, map[string]any{
			"disposition": string(result.Disposition),
			"reason":      result.Reason,
		})
	}
	if passthroughBackpressure != nil {
		s.writeIngestError(w, passthroughBackpressure)
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{"results": results})
}

func errorDetail(err error) string {
	if err == nil {
		return "expected a JSON object"
	}
	return err.Error()
}

func (s *Server) writeIngestError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ingest.ErrUnavailable):
		// Retryable: the forwarder must buffer and try again.
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, "not accepting events", err.Error())
	case errors.Is(err, ingest.ErrRejected):
		writeError(w, http.StatusBadRequest, "event rejected", err.Error())
	default:
		s.logger.Error("ingest failed", slog.String("error", err.Error()))
		writeError(w, http.StatusInternalServerError, "ingest failed", "")
	}
}

func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	ok, states := s.health.Live(r.Context())
	writeHealth(w, ok, states)
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ok, states := s.health.Ready(r.Context())
	writeHealth(w, ok, states)
}

func writeHealth(w http.ResponseWriter, ok bool, states map[string]health.State) {
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}

	checks := make(map[string]any, len(states))
	for name, state := range states {
		checks[name] = map[string]any{"healthy": state.Healthy, "detail": state.Detail}
	}
	writeJSON(w, status, map[string]any{"healthy": ok, "checks": checks})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message, detail string) {
	writeJSON(w, status, map[string]any{"error": message, "detail": detail})
}
