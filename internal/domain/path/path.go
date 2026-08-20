// Package path compiles and resolves the single-value path language from ADR-0004.
package path

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type segment struct {
	key   string
	index int
	isKey bool
}

// Path is a compiled reference to at most one value inside a producer document.
type Path struct {
	expr     string
	segments []segment
}

// LastSegment returns the field name represented by the path's final segment.
func (p Path) LastSegment() string {
	if len(p.segments) == 0 {
		return ""
	}
	last := p.segments[len(p.segments)-1]
	if last.isKey {
		return last.key
	}
	return strconv.Itoa(last.index)
}

// Compile parses a restricted, single-value document path (ADR-0004, section 5).
func Compile(expr string) (Path, error) {
	fail := func(detail string) (Path, error) {
		return Path{}, fmt.Errorf("path: invalid expression %q: %s", expr, detail)
	}
	if !strings.HasPrefix(expr, "$.") {
		return fail("must start with $.")
	}
	if len(expr) == 2 {
		return fail("missing segment")
	}

	p := Path{expr: expr}
	for i := 2; i < len(expr); {
		start := i
		for i < len(expr) && expr[i] != '.' && expr[i] != '[' {
			i++
		}
		if start == i {
			return fail("empty key segment")
		}
		p.segments = append(p.segments, segment{key: expr[start:i], isKey: true})

		for i < len(expr) && expr[i] == '[' {
			i++
			if i >= len(expr) {
				return fail("unterminated bracket")
			}
			if expr[i] == '"' {
				i++
				var key strings.Builder
				closed := false
				for i < len(expr) {
					switch expr[i] {
					case '"':
						i++
						closed = true
					case '\\':
						i++
						if i >= len(expr) || (expr[i] != '"' && expr[i] != '\\') {
							return fail("quoted keys only support \\\" and \\\\")
						}
						key.WriteByte(expr[i])
						i++
					default:
						key.WriteByte(expr[i])
						i++
					}
					if closed {
						break
					}
				}
				if !closed || i >= len(expr) || expr[i] != ']' {
					return fail("unterminated quoted key")
				}
				i++
				p.segments = append(p.segments, segment{key: key.String(), isKey: true})
			} else {
				start = i
				if expr[i] == '-' {
					i++
				}
				digits := i
				for i < len(expr) && expr[i] >= '0' && expr[i] <= '9' {
					i++
				}
				if digits == i || i >= len(expr) || expr[i] != ']' {
					return fail("array index must be bare digits, optionally negative")
				}
				n, err := strconv.Atoi(expr[start:i])
				if err != nil {
					return fail("array index is out of range")
				}
				i++
				p.segments = append(p.segments, segment{index: n})
			}
		}

		if i == len(expr) {
			break
		}
		if expr[i] != '.' {
			return fail("unexpected character")
		}
		i++
		if i == len(expr) {
			return fail("trailing dot")
		}
	}
	return p, nil
}

// Resolve returns the scalar at the path, and whether it resolved.
func (p Path) Resolve(doc map[string]any) (string, bool) {
	var current any = doc
	for _, part := range p.segments {
		if part.isKey {
			object, ok := current.(map[string]any)
			if !ok {
				return "", false
			}
			current, ok = object[part.key]
			if !ok {
				return "", false
			}
			continue
		}
		array, ok := current.([]any)
		if !ok {
			return "", false
		}
		index := part.index
		if index < 0 {
			index = len(array) + index
		}
		if index < 0 || index >= len(array) {
			return "", false
		}
		current = array[index]
	}

	switch value := current.(type) {
	case string:
		return value, true
	case bool:
		return strconv.FormatBool(value), true
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return "", false
		}
		return strconv.FormatFloat(value, 'f', -1, 64), true
	case float32:
		return strconv.FormatFloat(float64(value), 'f', -1, 32), true
	case int:
		return strconv.Itoa(value), true
	case int8:
		return strconv.FormatInt(int64(value), 10), true
	case int16:
		return strconv.FormatInt(int64(value), 10), true
	case int32:
		return strconv.FormatInt(int64(value), 10), true
	case int64:
		return strconv.FormatInt(value, 10), true
	case uint:
		return strconv.FormatUint(uint64(value), 10), true
	case uint8:
		return strconv.FormatUint(uint64(value), 10), true
	case uint16:
		return strconv.FormatUint(uint64(value), 10), true
	case uint32:
		return strconv.FormatUint(uint64(value), 10), true
	case uint64:
		return strconv.FormatUint(value, 10), true
	default:
		return "", false
	}
}

// String returns the original expression for diagnostics and config echo.
func (p Path) String() string { return p.expr }

// Canonical returns one stable spelling for the compiled path.
func (p Path) Canonical() string {
	if len(p.segments) == 0 {
		return ""
	}
	var out strings.Builder
	out.WriteString("$.")
	out.WriteString(p.segments[0].key)
	for _, part := range p.segments[1:] {
		if !part.isKey {
			fmt.Fprintf(&out, "[%d]", part.index)
			continue
		}
		if isBareCanonicalKey(part.key) {
			out.WriteByte('.')
			out.WriteString(part.key)
			continue
		}
		out.WriteString(`["`)
		out.WriteString(strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(part.key))
		out.WriteString(`"]`)
	}
	return out.String()
}

func isBareCanonicalKey(key string) bool {
	if key == "" {
		return false
	}
	allDigits := true
	for _, char := range key {
		if char == '.' || char == '[' {
			return false
		}
		if char < '0' || char > '9' {
			allDigits = false
		}
	}
	return !allDigits
}

// MarshalJSON stores the canonical expression rather than compiler internals.
func (p Path) MarshalJSON() ([]byte, error) { return json.Marshal(p.Canonical()) }

// UnmarshalJSON recompiles a durable canonical expression.
func (p *Path) UnmarshalJSON(raw []byte) error {
	var expression string
	if err := json.Unmarshal(raw, &expression); err != nil {
		return err
	}
	if expression == "" {
		*p = Path{}
		return nil
	}
	compiled, err := Compile(expression)
	if err != nil {
		return err
	}
	*p = compiled
	return nil
}
