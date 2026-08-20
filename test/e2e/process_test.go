//go:build e2e

package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	pebblestate "github.com/kraicdesign/flow-stitch/internal/adapters/state/pebble"
	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/application/deliver"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/path"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

const conditionTimeout = 8 * time.Second

type lockedBuffer struct {
	sync.Mutex
	bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.Write(p)
}

func (b *lockedBuffer) String() string {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.String()
}

type bulkDocument struct {
	Action   map[string]map[string]string
	Document map[string]any
}

type stubSink struct {
	server *httptest.Server
	mu     sync.Mutex
	docs   []bulkDocument
	err    error
}

func newStubSink(t *testing.T) *stubSink {
	t.Helper()
	s := &stubSink{}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scanner := bufio.NewScanner(r.Body)
		var received []bulkDocument
		var ids []string
		for scanner.Scan() {
			var action map[string]map[string]string
			if err := json.Unmarshal(scanner.Bytes(), &action); err != nil {
				s.recordError(fmt.Errorf("decode bulk action: %w", err))
				http.Error(w, "bad action", http.StatusBadRequest)
				return
			}
			if !scanner.Scan() {
				s.recordError(errors.New("bulk action has no document"))
				http.Error(w, "missing document", http.StatusBadRequest)
				return
			}
			var document map[string]any
			if err := json.Unmarshal(scanner.Bytes(), &document); err != nil {
				s.recordError(fmt.Errorf("decode bulk document: %w", err))
				http.Error(w, "bad document", http.StatusBadRequest)
				return
			}
			received = append(received, bulkDocument{Action: action, Document: document})
			ids = append(ids, action["index"]["_id"])
		}
		if err := scanner.Err(); err != nil {
			s.recordError(fmt.Errorf("scan bulk request: %w", err))
			http.Error(w, "read failure", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.docs = append(s.docs, received...)
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, bulkResponse(ids))
	}))
	t.Cleanup(s.server.Close)
	return s
}

func bulkResponse(ids []string) string {
	items := make([]map[string]map[string]any, len(ids))
	for i, id := range ids {
		items[i] = map[string]map[string]any{"index": {"_id": id, "status": 201}}
	}
	raw, _ := json.Marshal(map[string]any{"errors": false, "items": items})
	return string(raw)
}

func (s *stubSink) recordError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

func (s *stubSink) documents() ([]bulkDocument, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]bulkDocument(nil), s.docs...), s.err
}

type processHarness struct {
	t          *testing.T
	binary     string
	configPath string
	statePath  string
	address    string
	baseURL    string
	sink       *stubSink
	logs       lockedBuffer
	cmd        *exec.Cmd
	done       chan error
}

func newProcessHarness(t *testing.T, timeout time.Duration, outputIndex string, passthrough bool) *processHarness {
	t.Helper()
	t.Parallel()
	directory := t.TempDir()
	address := unusedAddress(t)
	h := &processHarness{
		t: t, binary: e2eBinary(t), configPath: filepath.Join(directory, "flowstitch.yaml"),
		statePath: filepath.Join(directory, "state"), address: address,
		baseURL: "http://" + address, sink: newStubSink(t),
	}
	h.writeConfig(timeout, outputIndex, "http.request", "http.response", passthrough)
	t.Cleanup(func() {
		h.stop(syscall.SIGKILL, 2*time.Second, false)
		if t.Failed() {
			t.Logf("process output:\n%s", h.logs.String())
		}
	})
	return h
}

func e2eBinary(t *testing.T) string {
	t.Helper()
	name := os.Getenv("FLOWSTITCH_E2E_BINARY")
	if name == "" {
		t.Fatal("FLOWSTITCH_E2E_BINARY is unset; run `make test-e2e`")
	}
	abs, err := filepath.Abs(name)
	if err != nil {
		t.Fatal(err)
	}
	return abs
}

func unusedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func (h *processHarness) writeConfig(timeout time.Duration, outputIndex, requestType, responseType string, passthrough bool) {
	h.t.Helper()
	raw := fmt.Sprintf(`server:
  address: %q
  read_timeout: 2s
  write_timeout: 2s
  shutdown_grace: 2s
  max_request_size: 1048576
state:
  driver: pebble
  path: %q
  sync_writes: true
passthrough:
  enabled: %t
  index: passthrough-logs
  timestamp: $.datetime
  buffer_size: 32
  batch_size: 8
  flush_interval: 20ms
alerts:
  enabled: false
rules:
  - id: application-flow
    enabled: true
    extract:
      event_type: $.event
      timestamp: $.datetime
    correlation:
      key: $.flow_id
    stitch:
      - id: call
        group_by: [$.service, $.invocation_id]
        roles:
          request: %s
          response: %s
        requires: [request, response]
    lifecycle:
      timeout: %s
      close_when: all_invocations_complete
    limits:
      max_events: 32
      max_event_bytes: 65536
      max_flow_bytes: 1048576
    output:
      index: %s
      timestamp: first_event.timestamp
sinks:
  opensearch:
    addresses: [%q]
    bulk_size: 8
    flush_interval: 20ms
    max_retries: 3
observability:
  metrics_path: /metrics
  log_level: debug
  log_format: json
limits:
  max_open_flows: 100
  max_outbox_records: 100
  max_dlq_records: 100
  max_event_bytes: 65536
`, h.address, h.statePath, passthrough, requestType, responseType, timeout, outputIndex, h.sink.server.URL)
	if err := os.WriteFile(h.configPath, []byte(raw), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

func (h *processHarness) start() {
	h.t.Helper()
	h.cmd = exec.Command(h.binary, "-config", h.configPath)
	h.cmd.Stdout = &h.logs
	h.cmd.Stderr = &h.logs
	if err := h.cmd.Start(); err != nil {
		h.fail("start process: %v", err)
	}
	h.done = make(chan error, 1)
	go func() { h.done <- h.cmd.Wait() }()
	h.poll("process readiness", conditionTimeout, func() (bool, string) {
		select {
		case err := <-h.done:
			h.done <- err
			return false, fmt.Sprintf("process exited: %v", err)
		default:
		}
		response, err := http.Get(h.baseURL + "/health/ready")
		if err != nil {
			return false, err.Error()
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK, response.Status
	})
}

func (h *processHarness) stop(signal syscall.Signal, timeout time.Duration, requireZero bool) {
	if h.cmd == nil || h.cmd.Process == nil || h.done == nil {
		return
	}
	select {
	case <-h.done:
		h.done = nil
		return
	default:
	}
	if err := h.cmd.Process.Signal(signal); err != nil && !errors.Is(err, os.ErrProcessDone) {
		if requireZero {
			h.fail("signal process: %v", err)
		}
		return
	}
	select {
	case err := <-h.done:
		h.done = nil
		if requireZero && err != nil {
			h.fail("process exit = %v, want zero", err)
		}
	case <-time.After(timeout):
		_ = h.cmd.Process.Kill()
		<-h.done
		h.done = nil
		if requireZero {
			h.fail("process did not exit within %s", timeout)
		}
	}
}

func (h *processHarness) post(document string) map[string]any {
	h.t.Helper()
	response, err := http.Post(h.baseURL+"/v1/events", "application/json", bytes.NewBufferString(document))
	if err != nil {
		h.fail("POST event: %v", err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusAccepted {
		h.fail("POST status = %d, body = %s", response.StatusCode, raw)
	}
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		h.fail("decode POST response %q: %v", raw, err)
	}
	return result
}

func (h *processHarness) waitDocuments(count int) []bulkDocument {
	h.t.Helper()
	var documents []bulkDocument
	h.poll(fmt.Sprintf("%d sink documents", count), conditionTimeout, func() (bool, string) {
		var err error
		documents, err = h.sink.documents()
		if err != nil {
			return false, err.Error()
		}
		return len(documents) >= count, fmt.Sprintf("got %d", len(documents))
	})
	return documents
}

func (h *processHarness) poll(name string, timeout time.Duration, condition func() (bool, string)) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	last := "condition not evaluated"
	for time.Now().Before(deadline) {
		if ok, observed := condition(); ok {
			return
		} else {
			last = observed
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.fail("timed out waiting for %s (%s)", name, last)
}

func (h *processHarness) fail(format string, args ...any) {
	h.t.Helper()
	h.t.Fatalf(format+"\nprocess output:\n%s", append(args, h.logs.String())...)
}

func TestFullLifecycleProducesOneMergedDocument(t *testing.T) {
	h := newProcessHarness(t, time.Minute, "flows-v1", true)
	h.start()
	h.post(requestEvent("complete", "http.request"))
	h.post(responseEvent("complete", "http.response"))
	documents := h.waitDocuments(1)
	if len(documents) != 1 {
		h.fail("sink documents = %d, want exactly 1", len(documents))
	}
	entries := documents[0].Document["events"].([]any)
	entry := entries[0].(map[string]any)
	if len(entries) != 1 || entry["request"] == nil || entry["response"] == nil || entry["duration_ms"] != float64(50) {
		h.fail("events = %#v, want one merged entry with 50ms duration", entries)
	}
}

func TestTimeoutClosingReportsMissingResponse(t *testing.T) {
	h := newProcessHarness(t, 100*time.Millisecond, "timeout-flows", true)
	h.start()
	h.post(requestEvent("timeout", "http.request"))
	document := h.waitDocuments(1)[0].Document
	metadata := document["flow"].(map[string]any)
	if metadata["finalization_reason"] != "timeout" || metadata["incomplete_invocations"] != float64(1) {
		h.fail("flow metadata = %#v, want timeout with one incomplete invocation", metadata)
	}
}

func TestPassThroughIsVerbatimAndNeverEntersOutbox(t *testing.T) {
	h := newProcessHarness(t, time.Minute, "flows", true)
	h.start()
	raw := `{"event":"unmatched","datetime":"2026-08-22T10:00:00Z","message":"untouched","nested":{"value":7}}`
	result := h.post(raw)
	if result["disposition"] != "passed_through" {
		h.fail("POST result = %#v, want passed_through", result)
	}
	delivered := h.waitDocuments(1)[0]
	if delivered.Action["index"]["_index"] != "passthrough-logs" || delivered.Action["index"]["_id"] != "" {
		h.fail("pass-through action = %#v", delivered.Action)
	}
	var want map[string]any
	_ = json.Unmarshal([]byte(raw), &want)
	if !mapsEqual(delivered.Document, want) {
		h.fail("pass-through document = %#v, want %#v", delivered.Document, want)
	}
	h.stop(syscall.SIGTERM, 3*time.Second, true)
	store, err := pebblestate.Open(h.statePath, true)
	if err != nil {
		h.fail("reopen state: %v", err)
	}
	defer store.Close()
	if count, err := store.OutboxRecords(context.Background()); err != nil || count != 0 {
		h.fail("outbox records = %d, %v; want 0", count, err)
	}
}

func TestSIGKILLRestartRecoversOpenFlows(t *testing.T) {
	h := newProcessHarness(t, 150*time.Millisecond, "recovered-flows", true)
	h.start()
	h.post(requestEvent("restart-a", "http.request"))
	h.post(requestEvent("restart-b", "http.request"))
	h.stop(syscall.SIGKILL, 3*time.Second, false)
	h.start()
	documents := h.waitDocuments(2)
	ids := map[string]bool{}
	for _, document := range documents {
		metadata := document.Document["flow"].(map[string]any)
		ids[metadata["id"].(string)] = metadata["finalization_reason"] == "timeout"
	}
	if !ids["restart-a"] || !ids["restart-b"] {
		h.fail("recovered documents = %#v", documents)
	}
}

func TestSIGTERMWithOpenFlowExitsWithinGrace(t *testing.T) {
	h := newProcessHarness(t, time.Hour, "flows", true)
	h.start()
	h.post(requestEvent("shutdown", "http.request"))
	started := time.Now()
	h.stop(syscall.SIGTERM, 3*time.Second, true)
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		h.fail("graceful shutdown took %s", elapsed)
	}
}

func TestSIGHUPAppliesNewRuleOnlyToNewFlows(t *testing.T) {
	h := newProcessHarness(t, time.Minute, "flows-v1", true)
	h.start()
	h.post(requestEvent("old", "http.request"))
	h.writeConfig(time.Minute, "flows-v2", "rpc.start", "rpc.finish", true)
	if err := h.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		h.fail("send SIGHUP: %v", err)
	}
	h.poll("successful reload", conditionTimeout, func() (bool, string) {
		return bytes.Contains([]byte(h.logs.String()), []byte(`"msg":"configuration reloaded"`)), h.logs.String()
	})
	h.post(responseEvent("old", "http.response"))
	h.post(requestEvent("new", "rpc.start"))
	h.post(responseEvent("new", "rpc.finish"))
	documents := h.waitDocuments(2)
	indices := map[string]string{}
	for _, document := range documents {
		metadata := document.Document["flow"].(map[string]any)
		indices[metadata["id"].(string)] = document.Action["index"]["_index"]
	}
	if indices["old"] != "flows-v1" || indices["new"] != "flows-v2" {
		h.fail("indices by flow = %#v, want old=v1 and new=v2", indices)
	}
}

func requestEvent(id, eventType string) string {
	return fmt.Sprintf(`{"flow_id":%q,"event":%q,"service":"web","invocation_id":"inv-1","datetime":"2026-08-22T10:00:00.000Z"}`, id, eventType)
}

func responseEvent(id, eventType string) string {
	return fmt.Sprintf(`{"flow_id":%q,"event":%q,"service":"web","invocation_id":"inv-1","datetime":"2026-08-22T10:00:00.050Z"}`, id, eventType)
}

func mapsEqual(got, want map[string]any) bool {
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	return bytes.Equal(gotJSON, wantJSON)
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type recordingSink struct {
	records []outbox.Record
}

func (s *recordingSink) Name() string { return "recording" }
func (s *recordingSink) Deliver(_ context.Context, records []outbox.Record) ([]outbox.Result, error) {
	s.records = append(s.records, records...)
	results := make([]outbox.Result, len(records))
	for i := range records {
		results[i] = outbox.Result{OutputID: records[i].OutputID, Disposition: outbox.Delivered}
	}
	return results, nil
}

type quarantine struct{}

func (quarantine) CaptureEvent(context.Context, event.Event, string) error    { return nil }
func (quarantine) CaptureRecord(context.Context, outbox.Record, string) error { return nil }

func TestCrashBetweenFinalizationWritesIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	configured := testRule(t)
	now := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	producer := event.Event{ObservedAt: now, Doc: map[string]any{"flow_id": "crash", "event": "http.request", "service": "web", "invocation_id": "inv-1", "datetime": "2026-08-22T10:00:00Z"}}
	current := flow.Open(flow.Key{RuleID: configured.ID, CorrelationKey: "crash"}, configured, producer)
	current.Apply(producer, configured, now)
	directory := t.TempDir()
	first, err := pebblestate.Open(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.WithTx(ctx, func(tx application.Tx) error { return tx.SaveFlow(ctx, current) }); err != nil {
		t.Fatal(err)
	}
	wantID := projection.NewOutputID(configured.ID, "crash", now)
	snapshot, err := projection.Project(current.Finalize(flow.ReasonTimeout, now.Add(time.Minute)), configured)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(snapshot)
	if err := first.WithTx(ctx, func(tx application.Tx) error {
		return tx.EnqueueOutbox(ctx, outbox.Record{OutputID: wantID, Index: "flows", Document: raw, CreatedAt: now.Add(time.Minute)})
	}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := pebblestate.Open(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.WithTx(ctx, func(tx application.Tx) error {
		loaded, found, err := tx.LoadFlow(ctx, current.Key())
		if err != nil || !found {
			t.Fatalf("open flow after crash state = found %v, err %v", found, err)
		}
		records, err := tx.PendingOutbox(ctx, now.Add(2*time.Minute), 10)
		if err != nil || len(records) != 1 || records[0].OutputID != wantID {
			t.Fatalf("outbox after crash state = %#v, err %v", records, err)
		}
		gotID, err := application.Finalize(ctx, tx, loaded, configured, flow.ReasonTimeout, now.Add(time.Minute), application.NoopRecorder{})
		if gotID != wantID || err != nil {
			t.Fatalf("Finalize() = (%q, %v), want (%q, nil)", gotID, err, wantID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sink := &recordingSink{}
	delivery := deliver.New(second, sink, quarantine{}, fixedClock{now.Add(2 * time.Minute)}, 10, 3, application.NoopRecorder{})
	if count, err := delivery.Drain(ctx); err != nil || count != 1 {
		t.Fatalf("Drain() = (%d, %v), want (1, nil)", count, err)
	}
	if len(sink.records) != 1 || sink.records[0].OutputID != wantID {
		t.Fatalf("delivered records = %#v, want one record %q", sink.records, wantID)
	}
}

func testRule(t *testing.T) rule.Rule {
	t.Helper()
	mustPath := func(raw string) path.Path {
		compiled, err := path.Compile(raw)
		if err != nil {
			t.Fatal(err)
		}
		return compiled
	}
	configured := rule.Rule{
		ID: "application-flow", Enabled: true,
		Extract:   rule.Extract{EventType: mustPath("$.event"), Timestamp: mustPath("$.datetime")},
		Key:       mustPath("$.flow_id"),
		Stitch:    []rule.Stitch{{ID: "call", GroupBy: []path.Path{mustPath("$.service"), mustPath("$.invocation_id")}, Roles: []rule.Role{{Name: "request", Types: []string{"http.request"}}, {Name: "response", Types: []string{"http.response"}}}, Requires: []string{"request", "response"}}},
		Lifecycle: rule.Lifecycle{Timeout: time.Minute, CloseWhen: rule.CloseAllInvocationsComplete},
		Limits:    rule.Limits{MaxEvents: 32}, Output: rule.Output{Index: "flows", TimestampSource: rule.TimestampFirstEvent},
	}
	version, err := rule.ContentVersion(configured)
	if err != nil {
		t.Fatal(err)
	}
	configured.Version = version
	return configured
}

var _ application.Clock = fixedClock{}
var _ application.Sink = (*recordingSink)(nil)
