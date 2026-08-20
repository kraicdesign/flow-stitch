package event_test

import (
	"strings"
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/domain/event"
)

func TestStringDoesNotIncludeProducerDocument(t *testing.T) {
	e := event.Event{Doc: map[string]any{"secret": "do-not-log"}, ObservedAt: time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)}
	got := e.String()
	if strings.Contains(got, "secret") || strings.Contains(got, "do-not-log") {
		t.Fatalf("String() leaked producer document: %q", got)
	}
}
