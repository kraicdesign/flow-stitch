// Package indexname resolves configured output-index patterns.
package indexname

import (
	"fmt"
	"strings"
	"time"
)

var placeholders = map[string]func(time.Time) string{
	"yyyy": func(at time.Time) string { return at.Format("2006") },
	"MM":   func(at time.Time) string { return at.Format("01") },
	"dd":   func(at time.Time) string { return at.Format("02") },
}

// Validate rejects placeholders that FlowStitch cannot resolve.
func Validate(pattern string) error {
	for offset := 0; ; {
		start := strings.IndexByte(pattern[offset:], '{')
		if start < 0 {
			break
		}
		start += offset
		end := strings.IndexByte(pattern[start+1:], '}')
		if end < 0 {
			return fmt.Errorf("index pattern %q has an unterminated placeholder", pattern)
		}
		end += start + 1
		name := pattern[start+1 : end]
		if _, ok := placeholders[name]; !ok {
			return fmt.Errorf("index pattern %q contains unknown placeholder {%s}", pattern, name)
		}
		offset = end + 1
	}
	if strings.Contains(pattern, "}") {
		// Every valid closing brace was consumed with its opening brace above.
		remaining := pattern
		for {
			start := strings.IndexByte(remaining, '{')
			if start < 0 {
				break
			}
			end := strings.IndexByte(remaining[start+1:], '}')
			if end < 0 {
				break
			}
			remaining = remaining[:start] + remaining[start+1+end+1:]
		}
		if strings.Contains(remaining, "}") {
			return fmt.Errorf("index pattern %q has an unmatched closing brace", pattern)
		}
	}
	return nil
}

// Resolve substitutes date placeholders using the document timestamp in UTC.
// A missing producer timestamp uses finalization time (ADR-0008, section 3).
func Resolve(pattern string, documentTimestamp, finalizedAt time.Time) string {
	at := documentTimestamp
	if at.IsZero() {
		at = finalizedAt
	}
	at = at.UTC()
	resolved := pattern
	for name, format := range placeholders {
		resolved = strings.ReplaceAll(resolved, "{"+name+"}", format(at))
	}
	return resolved
}

// WildcardPattern converts date placeholders to the index-template wildcard.
func WildcardPattern(pattern string) string {
	resolved := pattern
	for name := range placeholders {
		resolved = strings.ReplaceAll(resolved, "{"+name+"}", "*")
	}
	for {
		previous := resolved
		for _, separator := range []string{".", "-", "_"} {
			resolved = strings.ReplaceAll(resolved, "*"+separator+"*", "*")
		}
		if resolved == previous {
			return resolved
		}
	}
}
