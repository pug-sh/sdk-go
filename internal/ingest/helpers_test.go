package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

const (
	testSession = "0190a5a6-0000-7000-8000-000000000001"
	testVersion = "0.0.0-test"
)

type record struct {
	level slog.Level
	msg   string
	attrs map[string]any
}

func (r record) String() string { return fmt.Sprintf("%s %q %v", r.level, r.msg, r.attrs) }

// logs captures what the SDK logs, standing in for sdk-node's console spies.
type logs struct {
	mu      sync.Mutex
	records []record
}

func (l *logs) Enabled(context.Context, slog.Level) bool { return true }
func (l *logs) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *logs) WithGroup(string) slog.Handler            { return l }

func (l *logs) Handle(_ context.Context, r slog.Record) error {
	rec := record{level: r.Level, msg: r.Message, attrs: map[string]any{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.Any()
		return true
	})
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, rec)
	return nil
}

func (l *logs) at(level slog.Level) []record {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []record
	for _, r := range l.records {
		if r.level == level {
			out = append(out, r)
		}
	}
	return out
}

func (l *logs) warns() []record { return l.at(slog.LevelWarn) }

func (l *logs) find(t *testing.T, level slog.Level, substr string) record {
	t.Helper()
	for _, r := range l.at(level) {
		if strings.Contains(r.msg, substr) {
			return r
		}
	}
	t.Fatalf("no %s log containing %q; got %v", level, substr, l.records)
	return record{}
}

func newLogs() (*logs, Logger) {
	l := &logs{}
	return l, Logger{slog.New(l)}
}
