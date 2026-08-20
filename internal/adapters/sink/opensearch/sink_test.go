package opensearch_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kraicdesign/flow-stitch/internal/adapters/sink/opensearch"
	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
)

func TestDeliverBuildsBulkRequestAndParsesSuccess(t *testing.T) {
	var requestBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_bulk" || r.Header.Get("Content-Type") != "application/x-ndjson" {
			t.Errorf("request = %s %s, content-type %q", r.Method, r.URL.Path, r.Header.Get("Content-Type"))
		}
		if username, password, ok := r.BasicAuth(); !ok || username != "user" || password != "pass" {
			t.Errorf("BasicAuth() = (%q, %q, %v)", username, password, ok)
		}
		raw, _ := io.ReadAll(r.Body)
		requestBody = string(raw)
		_, _ = io.WriteString(w, `{"errors":false,"items":[{"index":{"_id":"one","status":201}},{"index":{"_id":"two","status":200}}]}`)
	}))
	defer server.Close()

	records := testRecords()
	sink := opensearch.New(opensearch.Options{Addresses: []string{server.URL}, Username: "user", Password: "pass"})
	results, err := sink.Deliver(context.Background(), records)
	if err != nil {
		t.Fatal(err)
	}
	assertDispositions(t, results, outbox.Delivered, outbox.Delivered)
	lines := strings.Split(strings.TrimSpace(requestBody), "\n")
	if len(lines) != 4 {
		t.Fatalf("bulk lines = %d, want 4: %q", len(lines), requestBody)
	}
	var action map[string]map[string]string
	if err := json.Unmarshal([]byte(lines[0]), &action); err != nil {
		t.Fatal(err)
	}
	if action["index"]["_id"] != "one" || action["index"]["_index"] != "flows-2026.08.21" {
		t.Fatalf("first action = %v", action)
	}
}

func TestDeliverPassthroughOmitsIDAndPreservesDocumentBytes(t *testing.T) {
	var requestBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestBody, _ = io.ReadAll(r.Body)
		_, _ = io.WriteString(w, `{"errors":false,"items":[{"index":{"_id":"assigned","status":201}}]}`)
	}))
	defer server.Close()
	raw := []byte(`{"message":"verbatim","number":1.00}`)
	sink := opensearch.New(opensearch.Options{Addresses: []string{server.URL}})
	results, err := sink.DeliverPassthrough(context.Background(), []application.PassthroughRecord{{Sequence: 7, Index: "logs-2026.08.21", Document: raw}})
	if err != nil || len(results) != 1 || results[0].Disposition != outbox.Delivered {
		t.Fatalf("DeliverPassthrough() = %+v, %v", results, err)
	}
	lines := bytes.Split(bytes.TrimSpace(requestBody), []byte("\n"))
	if len(lines) != 2 || bytes.Contains(lines[0], []byte(`"_id"`)) || !bytes.Equal(lines[1], raw) {
		t.Fatalf("bulk body = %q, want action without _id and byte-identical document", requestBody)
	}
}

func TestDeliverClassifiesFailuresInsideHTTP200(t *testing.T) {
	server := bulkServer(t, `{"errors":true,"items":[{"index":{"_id":"one","status":429,"error":{"type":"es_rejected_execution_exception","reason":"secret payload"}}},{"index":{"_id":"two","status":400,"error":{"type":"mapper_parsing_exception","reason":"secret payload"}}}]}`)
	defer server.Close()
	sink := opensearch.New(opensearch.Options{Addresses: []string{server.URL}})
	results, err := sink.Deliver(context.Background(), testRecords())
	if err != nil {
		t.Fatal(err)
	}
	assertDispositions(t, results, outbox.Retryable, outbox.Permanent)
	if results[1].RejectionType != "mapper_parsing_exception" {
		t.Fatalf("rejection type = %q, want mapper_parsing_exception", results[1].RejectionType)
	}
	for _, result := range results {
		if result.Err == nil || strings.Contains(result.Err.Error(), "secret payload") {
			t.Fatalf("safe classification error = %v", result.Err)
		}
	}
}

func TestDeliverTreatsForbiddenClusterBlockAsRetryable(t *testing.T) {
	server := bulkServer(t, `{"errors":true,"items":[{"index":{"_id":"one","status":403,"error":{"type":"cluster_block_exception","reason":"secret"}}},{"index":{"_id":"two","status":201}}]}`)
	defer server.Close()
	sink := opensearch.New(opensearch.Options{Addresses: []string{server.URL}})
	results, err := sink.Deliver(context.Background(), testRecords())
	if err != nil {
		t.Fatal(err)
	}
	assertDispositions(t, results, outbox.Retryable, outbox.Delivered)
	if results[0].RejectionType != "cluster_block_exception" || strings.Contains(results[0].Err.Error(), "secret") {
		t.Fatalf("cluster block result = %+v", results[0])
	}
}

func TestDeliverAlertUsesNoIDAndReturnsSafeRejection(t *testing.T) {
	var body []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		_, _ = io.WriteString(w, `{"errors":true,"items":[{"index":{"_id":"assigned","status":400,"error":{"type":"mapper_parsing_exception","reason":"secret"}}}]}`)
	}))
	defer server.Close()
	sink := opensearch.New(opensearch.Options{Addresses: []string{server.URL}})
	err := sink.DeliverAlert(context.Background(), application.AlertRecord{Index: "flowstitch-alerts-2026.08.21", Document: []byte(`{"kind":"dlq"}`)})
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("DeliverAlert() = %v, want safe rejection", err)
	}
	lines := bytes.Split(bytes.TrimSpace(body), []byte("\n"))
	if len(lines) != 2 || bytes.Contains(lines[0], []byte(`"_id"`)) {
		t.Fatalf("alert bulk body = %q", body)
	}
}

func TestDeliverTreatsRecordMissingFromResponseAsRetryable(t *testing.T) {
	server := bulkServer(t, `{"errors":false,"items":[{"index":{"_id":"one","status":201}}]}`)
	defer server.Close()
	sink := opensearch.New(opensearch.Options{Addresses: []string{server.URL}})
	results, err := sink.Deliver(context.Background(), testRecords())
	if err != nil {
		t.Fatal(err)
	}
	assertDispositions(t, results, outbox.Delivered, outbox.Retryable)
}

func TestDeliverTreatsUnrecognisedBulkStatusAsRetryable(t *testing.T) {
	server := bulkServer(t, `{"errors":true,"items":[{"index":{"_id":"one"}},{"index":{"_id":"two","status":399}}]}`)
	defer server.Close()
	sink := opensearch.New(opensearch.Options{Addresses: []string{server.URL}})
	results, err := sink.Deliver(context.Background(), testRecords())
	if err != nil {
		t.Fatal(err)
	}
	assertDispositions(t, results, outbox.Retryable, outbox.Retryable)
}

func TestDeliverClassifiesTransportAndRequestFailuresForWholeBatch(t *testing.T) {
	for _, test := range []struct {
		name    string
		address string
	}{{"invalid address", "://bad"}, {"connection", "http://127.0.0.1:1"}} {
		t.Run(test.name, func(t *testing.T) {
			sink := opensearch.New(opensearch.Options{Addresses: []string{test.address}})
			results, err := sink.Deliver(context.Background(), testRecords())
			if err != nil {
				t.Fatal(err)
			}
			assertDispositions(t, results, outbox.Retryable, outbox.Retryable)
		})
	}
}

func bulkServer(t *testing.T, response string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, response)
	}))
}

func testRecords() []outbox.Record {
	return []outbox.Record{
		{OutputID: "one", Index: "flows-2026.08.21", Document: []byte(`{"flow":{"id":"one"}}`)},
		{OutputID: "two", Index: "flows-2026.08.21", Document: []byte(`{"flow":{"id":"two"}}`)},
	}
}

func assertDispositions(t *testing.T, results []outbox.Result, want ...outbox.Disposition) {
	t.Helper()
	if len(results) != len(want) {
		t.Fatalf("len(results) = %d, want %d", len(results), len(want))
	}
	for i := range want {
		if results[i].Disposition != want[i] {
			t.Fatalf("results[%d] = %q, want %q", i, results[i].Disposition, want[i])
		}
	}
}
