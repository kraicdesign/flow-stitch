package alerts_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/application/alerts"
	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
)

type alertSink struct {
	records []application.AlertRecord
	err     error
}

func (s *alertSink) DeliverAlert(_ context.Context, record application.AlertRecord) error {
	s.records = append(s.records, record)
	return s.err
}

type alertLogger struct{ calls int }

func (l *alertLogger) ErrorContext(context.Context, string, ...any) { l.calls++ }

type alertRecorder struct {
	application.NoopRecorder
	kinds []string
}

func (r *alertRecorder) AlertEmitted(kind string) { r.kinds = append(r.kinds, kind) }

func TestDLQAlertsEnterRepeatAtIntervalAndClear(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	sink := &alertSink{}
	service := alerts.New(alertOptions(), outbox.DeadLetterSummary{
		Records: 1, Reasons: map[string]int{"mapper_parsing_exception": 1}, Indices: map[string]int{"flows": 1},
	}, sink, &alertRecorder{}, &alertLogger{})
	service.Observe(context.Background(), nil, now)
	for i := range 10 {
		service.ApplyDeadLetterChange(application.DeadLetterChange{Changed: true, Records: i + 2,
			Added: []outbox.DeadLetterRef{{Type: "mapper_parsing_exception", Index: "flows"}}})
		service.Observe(context.Background(), nil, now.Add(time.Minute))
	}
	if len(sink.records) != 1 {
		t.Fatalf("alerts within interval = %d, want 1", len(sink.records))
	}
	service.Observe(context.Background(), nil, now.Add(5*time.Minute))
	if len(sink.records) != 2 {
		t.Fatalf("alerts after interval = %d, want 2", len(sink.records))
	}
	removed := make([]outbox.DeadLetterRef, 11)
	for i := range removed {
		removed[i] = outbox.DeadLetterRef{Type: "mapper_parsing_exception", Index: "flows"}
	}
	service.ApplyDeadLetterChange(application.DeadLetterChange{Changed: true, Records: 0, Removed: removed})
	service.Observe(context.Background(), nil, now.Add(6*time.Minute))
	if len(sink.records) != 3 {
		t.Fatalf("alerts after clear = %d, want 3", len(sink.records))
	}
	starting := decodeAlert(t, sink.records[0])
	clearing := decodeAlert(t, sink.records[2])
	if starting.Kind != alerts.KindDLQ || starting.State != "starting" || len(starting.Reasons) != 1 || starting.Reasons[0].Type != "mapper_parsing_exception" {
		t.Fatalf("starting alert = %+v", starting)
	}
	if clearing.State != "clearing" {
		t.Fatalf("clearing state = %q, want clearing", clearing.State)
	}
}

func TestOldOutboxEmitsWithoutDeadLetters(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	sink := &alertSink{}
	service := alerts.New(alertOptions(), outbox.DeadLetterSummary{}, sink, &alertRecorder{}, &alertLogger{})
	service.Observe(context.Background(), []outbox.Record{{Index: "flows", CreatedAt: now.Add(-6 * time.Minute)}}, now)
	if len(sink.records) != 1 {
		t.Fatalf("alerts = %d, want 1", len(sink.records))
	}
	document := decodeAlert(t, sink.records[0])
	if document.Kind != alerts.KindOutboxBacklog || document.Counts.DeadLetterRecords != 0 || document.OldestOutboxAgeSeconds != 360 {
		t.Fatalf("outbox alert = %+v", document)
	}
}

func TestRejectedAlertIsLoggedDiscardedAndDoesNotLoop(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	sink := &alertSink{err: errors.New("alert mapping rejected")}
	logger := &alertLogger{}
	recorder := &alertRecorder{}
	service := alerts.New(alertOptions(), outbox.DeadLetterSummary{Records: 1, Reasons: map[string]int{"bad": 1}, Indices: map[string]int{"flows": 1}}, sink, recorder, logger)
	service.Observe(context.Background(), nil, now)
	service.Observe(context.Background(), nil, now.Add(time.Second))
	if len(sink.records) != 1 || logger.calls != 1 || len(recorder.kinds) != 1 {
		t.Fatalf("rejected alert: attempts=%d logs=%d metrics=%d, want 1 each", len(sink.records), logger.calls, len(recorder.kinds))
	}
}

func TestDisabledAlertsDoNothing(t *testing.T) {
	sink := &alertSink{}
	opts := alertOptions()
	opts.Enabled = false
	service := alerts.New(opts, outbox.DeadLetterSummary{Records: 1}, sink, &alertRecorder{}, &alertLogger{})
	service.Observe(context.Background(), []outbox.Record{{CreatedAt: time.Now().Add(-time.Hour)}}, time.Now())
	if len(sink.records) != 0 {
		t.Fatalf("disabled alerts emitted %d documents", len(sink.records))
	}
}

func alertOptions() alerts.Options {
	return alerts.Options{Enabled: true, Index: "flowstitch-alerts-{yyyy}.{MM}.{dd}", MinInterval: 5 * time.Minute, OutboxAgeThreshold: 5 * time.Minute}
}

func decodeAlert(t *testing.T, record application.AlertRecord) alerts.Document {
	t.Helper()
	var document alerts.Document
	if err := json.Unmarshal(record.Document, &document); err != nil {
		t.Fatal(err)
	}
	return document
}
