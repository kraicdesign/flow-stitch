package indexname_test

import (
	"testing"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/domain/indexname"
)

func TestResolveUsesDocumentTimestampInUTC(t *testing.T) {
	documentTime := time.Date(2026, 8, 21, 0, 30, 0, 0, time.FixedZone("east", 2*60*60))
	got := indexname.Resolve("application-flows-{yyyy}.{MM}.{dd}", documentTime, time.Time{})
	if want := "application-flows-2026.08.20"; got != want {
		t.Fatalf("Resolve() = %q, want %q", got, want)
	}
}

func TestResolveFallsBackToFinalizationTime(t *testing.T) {
	finalizedAt := time.Date(2026, 9, 2, 1, 2, 3, 0, time.FixedZone("west", -3*60*60))
	got := indexname.Resolve("application-flows-{yyyy}.{MM}.{dd}", time.Time{}, finalizedAt)
	if want := "application-flows-2026.09.02"; got != want {
		t.Fatalf("Resolve() = %q, want %q", got, want)
	}
}

func TestValidateRejectsUnknownAndMalformedPlaceholders(t *testing.T) {
	for _, pattern := range []string{"flows-{hour}", "flows-{yyyy", "flows-yyyy}"} {
		if err := indexname.Validate(pattern); err == nil {
			t.Fatalf("Validate(%q) = nil, want error", pattern)
		}
	}
	if err := indexname.Validate("literal-index"); err != nil {
		t.Fatalf("Validate(literal) = %v, want nil", err)
	}
}

func TestWildcardPatternCollapsesDatePlaceholders(t *testing.T) {
	got := indexname.WildcardPattern("application-flows-{yyyy}.{MM}.{dd}")
	if want := "application-flows-*"; got != want {
		t.Fatalf("WildcardPattern() = %q, want %q", got, want)
	}
}
