package ingest

import "log/slog"

// Logger is how the SDK logs: every message gets the "pug: " prefix.
type Logger struct{ base *slog.Logger }

// NewLogger logs to base, or to slog.Default() when base is nil.
func NewLogger(base *slog.Logger) Logger { return Logger{base} }

// Resolved per call so a client built before slog.SetDefault still follows it.
func (l Logger) get() *slog.Logger {
	if l.base != nil {
		return l.base
	}
	return slog.Default()
}

func (l Logger) Warn(msg string, args ...any)  { l.get().Warn("pug: "+msg, args...) }
func (l Logger) Error(msg string, args ...any) { l.get().Error("pug: "+msg, args...) }
