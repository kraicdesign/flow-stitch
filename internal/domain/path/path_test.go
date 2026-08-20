package path_test

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/kraicdesign/flow-stitch/internal/domain/path"
)

func TestResolve(t *testing.T) {
	var doc map[string]any
	if err := json.Unmarshal([]byte(`{
		"context":{"invocation_id":"inv-1"},
		"attempts":[{"status":"first"},{"status":"last"}],
		"payload":{"http.status":200,"0":"quoted","quote\"slash\\":"escaped"},
		"array-key":["indexed"],"integer":12345,"fraction":12.5,"enabled":true,
		"object":{"nested":"value"},"array":[1,2]
	}`), &doc); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name, expression, want string
		ok                     bool
	}{
		{"nested keys", "$.context.invocation_id", "inv-1", true},
		{"positive index", "$.attempts[0].status", "first", true},
		{"negative index", "$.attempts[-1].status", "last", true},
		{"quoted dotted key", `$.payload["http.status"]`, "200", true},
		{"quoted digits are key", `$.payload["0"]`, "quoted", true},
		{"bare digits are index", "$.array-key[0]", "indexed", true},
		{"quoted escapes", `$.payload["quote\"slash\\"]`, "escaped", true},
		{"integral number", "$.integer", "12345", true},
		{"fractional number", "$.fraction", "12.5", true},
		{"boolean", "$.enabled", "true", true},
		{"missing key", "$.missing", "", false},
		{"index out of range", "$.attempts[9].status", "", false},
		{"object", "$.object", "", false},
		{"array", "$.array", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := path.Compile(tc.expression)
			if err != nil {
				t.Fatalf("Compile(%q) = %v", tc.expression, err)
			}
			got, ok := compiled.Resolve(doc)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("Resolve() = (%q, %v), want (%q, %v)", got, ok, tc.want, tc.ok)
			}
			if compiled.String() != tc.expression {
				t.Fatalf("String() = %q, want %q", compiled.String(), tc.expression)
			}
		})
	}
}

func TestCompileRejectsMalformedExpressions(t *testing.T) {
	for _, expression := range []string{"flow_id", "$", "$.", "$.a.", "$.a..b", "$.a[]", "$.a[no]", "$.a[1x]", `$.a["unterminated]`, `$.a["bad\nescape"]`} {
		t.Run(expression, func(t *testing.T) {
			_, err := path.Compile(expression)
			if err == nil {
				t.Fatalf("Compile(%q) = nil error", expression)
			}
			if !strings.Contains(err.Error(), expression) && !strings.Contains(err.Error(), strconv.Quote(expression)) {
				t.Fatalf("error %q does not name expression %q", err, expression)
			}
		})
	}
}

func TestCanonicalNormalizesEquivalentKeySpellings(t *testing.T) {
	for _, expression := range []string{"$.payload.status", `$.payload["status"]`} {
		compiled, err := path.Compile(expression)
		if err != nil {
			t.Fatal(err)
		}
		if got := compiled.Canonical(); got != "$.payload.status" {
			t.Fatalf("Canonical(%q) = %q, want $.payload.status", expression, got)
		}
	}
}
