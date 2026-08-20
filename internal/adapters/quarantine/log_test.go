package quarantine

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/kraicdesign/flow-stitch/internal/domain/outbox"
)

func TestPermanentRecordLogExcludesRejectionReason(t *testing.T) {
	var output bytes.Buffer
	q := NewLog(slog.New(slog.NewTextHandler(&output, nil)))
	if err := q.CaptureRecord(context.Background(), outbox.Record{
		OutputID: "id", Index: "flows", Attempts: 2, RejectionType: "mapper_parsing_exception",
	}, "failed to parse field containing payload-secret"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "payload-secret") || !strings.Contains(output.String(), "mapper_parsing_exception") {
		t.Fatalf("log = %q, want rejection type without reason", output.String())
	}
}
