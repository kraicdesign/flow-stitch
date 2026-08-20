package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/adapters/passthrough"
	adapterrules "github.com/kraicdesign/flow-stitch/internal/adapters/rules"
	"github.com/kraicdesign/flow-stitch/internal/adapters/state/memory"
	"github.com/kraicdesign/flow-stitch/internal/application"
	adminservice "github.com/kraicdesign/flow-stitch/internal/application/admin"
	"github.com/kraicdesign/flow-stitch/internal/application/ingest"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/path"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
	"github.com/kraicdesign/flow-stitch/internal/observability/health"
	"github.com/kraicdesign/flow-stitch/internal/observability/metrics"
)

type httpClock struct{ now time.Time }

func (c httpClock) Now() time.Time { return c.now }

type httpCapacity struct{}

func (httpCapacity) AcceptingEvents(context.Context) (bool, string) { return true, "" }

type httpQuarantine struct{}

func (httpQuarantine) CaptureEvent(context.Context, event.Event, string) error    { return nil }
func (httpQuarantine) CaptureRecord(context.Context, outbox.Record, string) error { return nil }

func TestPostEventReturnsAcceptedDispositionAndFinalizesPair(t *testing.T) {
	compile := func(expression string) path.Path {
		compiled, err := path.Compile(expression)
		if err != nil {
			t.Fatal(err)
		}
		return compiled
	}
	configured := rule.Rule{ID: "application-flow", Version: "1", Enabled: true,
		Extract: rule.Extract{EventType: compile("$.event"), Timestamp: compile("$.datetime")}, Key: compile("$.flow_id"),
		Stitch:    []rule.Stitch{{ID: "call", GroupBy: []path.Path{compile("$.invocation_id")}, Roles: []rule.Role{{Name: "request", Types: []string{"request"}}, {Name: "response", Types: []string{"response"}}}, Requires: []string{"request", "response"}}},
		Lifecycle: rule.Lifecycle{Timeout: time.Second, CloseWhen: rule.CloseAllInvocationsComplete}, Limits: rule.Limits{MaxEvents: 10}, Output: rule.Output{Index: "flows"}}
	store := memory.New()
	registry := adapterrules.NewRegistry([]rule.Rule{configured})
	m := metrics.New()
	service := ingest.New(store, registry, httpQuarantine{}, httpCapacity{}, httpClock{now: time.Date(2026, 8, 20, 12, 0, 1, 0, time.UTC)}, m)
	server := New(Options{}, service, health.New(), m, slog.New(slog.NewTextHandler(io.Discard, nil)))

	post := func(body string) map[string]any {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(body))
		response := httptest.NewRecorder()
		server.http.Handler.ServeHTTP(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202; body=%s", response.Code, response.Body)
		}
		var result map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	request := post(`{"flow_id":"one","event":"request","invocation_id":"i-1","datetime":"2026-08-20T12:00:00Z"}`)
	if request["disposition"] != string(ingest.Correlated) {
		t.Fatalf("request response = %v", request)
	}
	response := post(`{"flow_id":"one","event":"response","invocation_id":"i-1","datetime":"2026-08-20T12:00:00.010Z"}`)
	if response["disposition"] != string(ingest.Finalized) {
		t.Fatalf("response response = %v", response)
	}
	if got := store.PendingRecords(); got != 1 {
		t.Fatalf("PendingRecords = %d, want 1", got)
	}
}

func TestFullPassthroughBufferReturns503WithRetryAfter(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	store := memory.New()
	m := metrics.New()
	buffer := passthrough.New(passthrough.Options{Index: "logs", BufferSize: 1, BatchSize: 1, FlushInterval: time.Second, Clock: httpClock{now}, Recorder: m})
	service := ingest.New(store, adapterrules.NewRegistry(nil), httpQuarantine{}, httpCapacity{}, httpClock{now}, m, buffer)
	server := New(Options{}, service, health.New(), m, slog.New(slog.NewTextHandler(io.Discard, nil)))

	post := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(`{"message":"ordinary"}`))
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		return response
	}
	if response := post(); response.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202", response.Code)
	}
	response := post()
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") == "" {
		t.Fatalf("full status = %d Retry-After=%q, want 503 with header", response.Code, response.Header().Get("Retry-After"))
	}
}

func TestAdminRoutesAreAbsentWithoutToken(t *testing.T) {
	m := metrics.New()
	server := New(Options{}, nil, health.New(), m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, path := range []string{"/v1/admin/dlq", "/v1/admin/dlq/id", "/v1/admin/dlq/replay"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("GET %s status = %d, want 404", path, response.Code)
		}
	}
}

func TestAdminAuthenticationListingFetchAndReplay(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	store := memory.New()
	for i, id := range []projection.OutputID{"one", "two"} {
		record := outbox.Record{OutputID: id, Index: "flows", Document: []byte(`{"known":"payload-secret"}`), CreatedAt: now.Add(time.Duration(i) * time.Second)}
		if err := store.WithTx(context.Background(), func(tx application.Tx) error {
			if err := tx.EnqueueOutbox(context.Background(), record); err != nil {
				return err
			}
			_, err := tx.ResolveOutbox(context.Background(), []outbox.Result{{OutputID: id, Disposition: outbox.Permanent, Attempts: 2, RejectionType: "mapper_parsing_exception", DeadLetteredAt: now.Add(time.Minute)}})
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	m := metrics.New()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	admin := adminservice.New(store, httpClock{now: now.Add(2 * time.Minute)}, m, nil)
	server := New(Options{AdminToken: "correct-token"}, nil, health.New(), m, logger, admin)
	do := func(method, path, token, body string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(method, path, strings.NewReader(body))
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		return response
	}
	if response := do(http.MethodGet, "/v1/admin/dlq", "", ""); response.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d, want 401", response.Code)
	}
	if response := do(http.MethodGet, "/v1/admin/dlq", "presented-secret", ""); response.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token status = %d, want 401", response.Code)
	}
	if strings.Contains(logs.String(), "presented-secret") {
		t.Fatalf("authentication log leaked presented token: %s", logs.String())
	}

	listing := do(http.MethodGet, "/v1/admin/dlq?limit=1", "correct-token", "")
	if listing.Code != http.StatusOK || strings.Contains(listing.Body.String(), "payload-secret") || !strings.Contains(listing.Body.String(), `"output_id":"one"`) {
		t.Fatalf("listing status=%d body=%s, want metadata without payload", listing.Code, listing.Body)
	}
	fetched := do(http.MethodGet, "/v1/admin/dlq/one", "correct-token", "")
	if fetched.Code != http.StatusOK || !strings.Contains(fetched.Body.String(), "payload-secret") {
		t.Fatalf("fetch status=%d body=%s, want document", fetched.Code, fetched.Body)
	}

	dry := do(http.MethodPost, "/v1/admin/dlq/replay", "correct-token", `{"reason_type":"mapper_parsing_exception","limit":1}`)
	if dry.Code != http.StatusOK || !strings.Contains(dry.Body.String(), `"dry_run":true`) {
		t.Fatalf("dry-run status=%d body=%s", dry.Code, dry.Body)
	}
	if count, _ := store.DeadLetterRecords(context.Background()); count != 2 {
		t.Fatalf("dead letters after dry run = %d, want 2", count)
	}
	replayed := do(http.MethodPost, "/v1/admin/dlq/replay", "correct-token", `{"reason_type":"mapper_parsing_exception","limit":1,"dry_run":false}`)
	if replayed.Code != http.StatusOK || !strings.Contains(replayed.Body.String(), `"matched":1`) {
		t.Fatalf("replay status=%d body=%s", replayed.Code, replayed.Body)
	}
	if count, _ := store.DeadLetterRecords(context.Background()); count != 1 {
		t.Fatalf("dead letters after bounded replay = %d, want 1", count)
	}
}

var _ application.Clock = httpClock{}
