// Package logging configures the structured internal logger.
//
// Internal logs carry rule ID, correlation key, output ID and reason codes.
// They must not carry event payloads by default, because logs can contain
// secrets and personal data.
package logging

import (
	"io"
	"log/slog"
	"strings"
)

// New builds a logger from the configured level and format.
func New(w io.Writer, level, format string) *slog.Logger {
	logger, _ := NewReloadable(w, level, format)
	return logger
}

// LevelController applies reloadable log-level changes to an existing handler.
type LevelController struct{ level *slog.LevelVar }

// Set applies a reloaded log level to the existing handler.
func (c *LevelController) Set(level string) { c.level.Set(parseLevel(level)) }

// NewReloadable builds a logger whose level can change without replacing it.
func NewReloadable(w io.Writer, level, format string) (*slog.Logger, *LevelController) {
	levelVar := &slog.LevelVar{}
	levelVar.Set(parseLevel(level))
	opts := &slog.HandlerOptions{Level: levelVar}

	var handler slog.Handler
	if strings.EqualFold(format, "text") {
		handler = slog.NewTextHandler(w, opts)
	} else {
		handler = slog.NewJSONHandler(w, opts)
	}
	return slog.New(handler).With(slog.String("service", "flowstitch")), &LevelController{level: levelVar}
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
