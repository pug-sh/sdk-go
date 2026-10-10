package pug

import "log/slog"

type logger struct{ base *slog.Logger }

// Resolved per call so a client built before slog.SetDefault still follows it.
func (l logger) get() *slog.Logger {
	if l.base != nil {
		return l.base
	}
	return slog.Default()
}

func (l logger) warn(msg string, args ...any)  { l.get().Warn("pug: "+msg, args...) }
func (l logger) error(msg string, args ...any) { l.get().Error("pug: "+msg, args...) }
