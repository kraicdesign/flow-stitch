package passthrough_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/adapters/passthrough"
	"github.com/kraicdesign/flow-stitch/internal/application"
	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/path"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func TestBufferPreservesBytesResolvesTimestampAndFallsBackToNow(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	timestamp, err := path.Compile("$.datetime")
	if err != nil {
		t.Fatal(err)
	}
	buffer := passthrough.New(passthrough.Options{Index: "logs-{yyyy}.{MM}.{dd}", Timestamp: timestamp, BufferSize: 2, BatchSize: 2, FlushInterval: time.Second, Clock: fixedClock{now}, Recorder: application.NoopRecorder{}})
	raw := []byte(`{"message":"unchanged","datetime":"2026-08-20T23:30:00-02:00"}`)
	if err := buffer.Accept(event.Event{Doc: map[string]any{"datetime": "2026-08-20T23:30:00-02:00"}, Raw: raw}); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Accept(event.Event{Doc: map[string]any{"datetime": "not-a-time"}, Raw: []byte(`{"datetime":"not-a-time"}`)}); err != nil {
		t.Fatal(err)
	}
	records := buffer.Pending()
	if len(records) != 2 || !bytes.Equal(records[0].Document, raw) {
		t.Fatalf("records = %+v, want byte-identical first document", records)
	}
	if records[0].Index != "logs-2026.08.21" || records[1].Index != "logs-2026.08.22" {
		t.Fatalf("indices = %q, %q, want timestamp date then now fallback", records[0].Index, records[1].Index)
	}
}

func TestBufferNeverExceedsFixedCapacity(t *testing.T) {
	buffer := passthrough.New(passthrough.Options{Index: "logs", BufferSize: 3, BatchSize: 2, FlushInterval: time.Second, Clock: fixedClock{time.Now()}, Recorder: application.NoopRecorder{}})
	for i := 0; i < 100; i++ {
		err := buffer.Accept(event.Event{Doc: map[string]any{"n": i}})
		if i < 3 && err != nil {
			t.Fatalf("Accept(%d) = %v", i, err)
		}
		if i >= 3 && !errors.Is(err, application.ErrPassthroughFull) {
			t.Fatalf("Accept(%d) = %v, want full", i, err)
		}
		if got := buffer.Depth(); got > 3 {
			t.Fatalf("Depth() = %d, exceeds 3", got)
		}
	}
}
