// Package opensearch delivers finalized documents to OpenSearch.
package opensearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
	"github.com/kraicdesign/flow-stitch/internal/domain/projection"
)

// Options configures OpenSearch transport and bulk delivery behaviour.
type Options struct {
	Addresses     []string
	Username      string
	Password      string
	TLSSkipVerify bool
	BulkSize      int
	FlushInterval time.Duration
	MaxRetries    int
}

// Sink delivers correlated, pass-through, and diagnostic documents to OpenSearch.
type Sink struct {
	opts   Options
	client *http.Client
	next   atomic.Uint64
}

// New does not dial: an outage at boot must not prevent buffering (ADR-0008, section 5).
func New(opts Options) *Sink {
	if opts.BulkSize <= 0 {
		opts.BulkSize = 64
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = time.Second
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: opts.TLSSkipVerify} //nolint:gosec // explicit operator option
	return &Sink{opts: opts, client: &http.Client{Timeout: 30 * time.Second, Transport: transport}}
}

// Name returns the stable adapter name used by instrumentation.
func (s *Sink) Name() string { return "opensearch" }

// Deliver uses the stored index and deterministic ID and inspects every bulk item (ADR-0008, section 4).
func (s *Sink) Deliver(ctx context.Context, records []outbox.Record) ([]outbox.Result, error) {
	if len(records) == 0 {
		return []outbox.Result{}, nil
	}
	if len(s.opts.Addresses) == 0 {
		return retryAll(records, errors.New("opensearch transport: no address configured")), nil
	}

	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for _, record := range records {
		if err := encoder.Encode(map[string]any{"index": map[string]string{"_id": string(record.OutputID), "_index": record.Index}}); err != nil {
			return nil, fmt.Errorf("opensearch: encode bulk action: %w", err)
		}
		body.Write(record.Document)
		body.WriteByte('\n')
	}

	address := s.opts.Addresses[int(s.next.Add(1)-1)%len(s.opts.Addresses)]
	endpoint, err := bulkURL(address)
	if err != nil {
		return retryAll(records, fmt.Errorf("opensearch transport: %w", err)), nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return retryAll(records, fmt.Errorf("opensearch transport: %w", err)), nil
	}
	request.Header.Set("Content-Type", "application/x-ndjson")
	if s.opts.Username != "" || s.opts.Password != "" {
		request.SetBasicAuth(s.opts.Username, s.opts.Password)
	}

	response, err := s.client.Do(request)
	if err != nil {
		return retryAll(records, transportError(err)), nil
	}
	defer response.Body.Close() //nolint:errcheck // Read-only HTTP response bodies have no close result to act on.
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// The response body may echo producer payloads and is never retained or logged.
		failure := fmt.Errorf("opensearch bulk request status %d", response.StatusCode)
		return classifyAll(records, response.StatusCode, failure), nil
	}

	raw, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return retryAll(records, fmt.Errorf("opensearch transport: read bulk response: %w", err)), nil
	}
	var decoded bulkResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return retryAll(records, errors.New("opensearch transport: malformed bulk response")), nil
	}
	return resultsFor(records, decoded), nil
}

// DeliverPassthrough omits _id so OpenSearch assigns identity (ADR-0010, section 4).
func (s *Sink) DeliverPassthrough(ctx context.Context, records []application.PassthroughRecord) ([]application.PassthroughResult, error) {
	if len(records) == 0 {
		return []application.PassthroughResult{}, nil
	}
	retryAll := func(disposition outbox.Disposition) []application.PassthroughResult {
		results := make([]application.PassthroughResult, len(records))
		for i, record := range records {
			results[i] = application.PassthroughResult{Sequence: record.Sequence, Disposition: disposition}
		}
		return results
	}
	if len(s.opts.Addresses) == 0 {
		return retryAll(outbox.Retryable), nil
	}
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for _, record := range records {
		if err := encoder.Encode(map[string]any{"index": map[string]string{"_index": record.Index}}); err != nil {
			return nil, fmt.Errorf("opensearch: encode pass-through bulk action: %w", err)
		}
		body.Write(record.Document)
		body.WriteByte('\n')
	}
	address := s.opts.Addresses[int(s.next.Add(1)-1)%len(s.opts.Addresses)]
	endpoint, err := bulkURL(address)
	if err != nil {
		return retryAll(outbox.Retryable), nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return retryAll(outbox.Retryable), nil
	}
	request.Header.Set("Content-Type", "application/x-ndjson")
	if s.opts.Username != "" || s.opts.Password != "" {
		request.SetBasicAuth(s.opts.Username, s.opts.Password)
	}
	response, err := s.client.Do(request)
	if err != nil {
		return retryAll(outbox.Retryable), nil
	}
	defer response.Body.Close() //nolint:errcheck // Read-only HTTP response bodies have no close result to act on.
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return retryAll(classifyStatus(response.StatusCode)), nil
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return retryAll(outbox.Retryable), nil
	}
	var decoded bulkResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return retryAll(outbox.Retryable), nil
	}
	results := make([]application.PassthroughResult, len(records))
	for i, record := range records {
		disposition := outbox.Retryable
		if i < len(decoded.Items) {
			for _, item := range decoded.Items[i] {
				disposition = classifyStatus(item.Status)
				break
			}
		}
		results[i] = application.PassthroughResult{Sequence: record.Sequence, Disposition: disposition}
	}
	return results, nil
}

type bulkResponse struct {
	Items []map[string]bulkItem `json:"items"`
}

type bulkItem struct {
	ID     projection.OutputID `json:"_id"`
	Status int                 `json:"status"`
	Error  json.RawMessage     `json:"error"`
}

func resultsFor(records []outbox.Record, response bulkResponse) []outbox.Result {
	items := make(map[projection.OutputID]bulkItem, len(response.Items))
	for _, action := range response.Items {
		for _, item := range action {
			if item.ID != "" {
				items[item.ID] = item
			}
			break
		}
	}
	results := make([]outbox.Result, 0, len(records))
	for _, record := range records {
		item, ok := items[record.OutputID]
		if !ok {
			results = append(results, outbox.Result{OutputID: record.OutputID, Disposition: outbox.Retryable, Err: errors.New("opensearch bulk response omitted record")})
			continue
		}
		rejectionType := failureType(item.Error)
		disposition := classifyItem(item.Status, rejectionType)
		var itemErr error
		if disposition != outbox.Delivered {
			itemErr = itemFailure(item.Status, rejectionType)
		}
		results = append(results, outbox.Result{OutputID: record.OutputID, Disposition: disposition, Err: itemErr, RejectionType: rejectionType})
	}
	return results
}

func failureType(raw json.RawMessage) string {
	var detail struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(raw, &detail)
	return detail.Type
}

func itemFailure(status int, rejectionType string) error {
	if rejectionType == "" {
		return fmt.Errorf("opensearch bulk item status %d", status)
	}
	// Reasons can echo payloads; the classification type is sufficient for the DLQ.
	return fmt.Errorf("opensearch bulk item status %d type %s", status, rejectionType)
}

func classifyItem(status int, rejectionType string) outbox.Disposition {
	if rejectionType == "cluster_block_exception" {
		return outbox.Retryable
	}
	return classifyStatus(status)
}

func classifyStatus(status int) outbox.Disposition {
	if status >= 200 && status < 300 {
		return outbox.Delivered
	}
	if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable || status == http.StatusRequestTimeout || status >= 500 {
		return outbox.Retryable
	}
	if status >= 400 && status < 500 {
		return outbox.Permanent
	}
	return outbox.Retryable
}

func classifyAll(records []outbox.Record, status int, err error) []outbox.Result {
	disposition := classifyStatus(status)
	results := make([]outbox.Result, len(records))
	for i, record := range records {
		results[i] = outbox.Result{OutputID: record.OutputID, Disposition: disposition, Err: err}
	}
	return results
}

func retryAll(records []outbox.Record, err error) []outbox.Result {
	results := make([]outbox.Result, len(records))
	for i, record := range records {
		results[i] = outbox.Result{OutputID: record.OutputID, Disposition: outbox.Retryable, Err: err}
	}
	return results
}

// DeliverAlert writes one engine-owned diagnostic document without assigning
// an ID. Failures are returned to the alert reporter and never retried here.
func (s *Sink) DeliverAlert(ctx context.Context, record application.AlertRecord) error {
	if len(s.opts.Addresses) == 0 {
		return errors.New("opensearch alert: no address configured")
	}
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	if err := encoder.Encode(map[string]any{"index": map[string]string{"_index": record.Index}}); err != nil {
		return fmt.Errorf("opensearch alert: encode bulk action: %w", err)
	}
	body.Write(record.Document)
	body.WriteByte('\n')
	address := s.opts.Addresses[int(s.next.Add(1)-1)%len(s.opts.Addresses)]
	endpoint, err := bulkURL(address)
	if err != nil {
		return fmt.Errorf("opensearch alert: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return fmt.Errorf("opensearch alert: request: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-ndjson")
	if s.opts.Username != "" || s.opts.Password != "" {
		request.SetBasicAuth(s.opts.Username, s.opts.Password)
	}
	response, err := s.client.Do(request)
	if err != nil {
		return transportError(err)
	}
	defer response.Body.Close() //nolint:errcheck // Read-only HTTP response bodies have no close result to act on.
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("opensearch alert bulk status %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return errors.New("opensearch alert: response read failed")
	}
	var decoded bulkResponse
	if err := json.Unmarshal(raw, &decoded); err != nil || len(decoded.Items) == 0 {
		return errors.New("opensearch alert: malformed bulk response")
	}
	for _, item := range decoded.Items[0] {
		if item.Status >= 200 && item.Status < 300 {
			return nil
		}
		return itemFailure(item.Status, failureType(item.Error))
	}
	return errors.New("opensearch alert: bulk response omitted item")
}

func transportError(err error) error {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return errors.New("opensearch transport timeout")
	}
	return errors.New("opensearch transport failure")
}

func bulkURL(address string) (string, error) {
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("invalid address")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/_bulk"
	return parsed.String(), nil
}

var _ application.Sink = (*Sink)(nil)
var _ application.PassthroughSink = (*Sink)(nil)
var _ application.AlertSink = (*Sink)(nil)
