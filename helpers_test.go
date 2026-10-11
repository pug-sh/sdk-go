package pug

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"

	eventsv1 "github.com/pug-sh/sdk-go/gen/sdk/events/v1"
	"github.com/pug-sh/sdk-go/gen/sdk/events/v1/eventsv1connect"
	sdkprofilesv1 "github.com/pug-sh/sdk-go/gen/sdk/profiles/v1"
	"github.com/pug-sh/sdk-go/gen/sdk/profiles/v1/sdkprofilesv1connect"
	"github.com/pug-sh/sdk-go/internal/ingest"
)

const testSession = "0190a5a6-0000-7000-8000-000000000001"

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

func newLogs() (*logs, ingest.Logger) {
	l := &logs{}
	return l, ingest.NewLogger(slog.New(l))
}

func ids(events []*Event) string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.GetEventId()
	}
	return strings.Join(out, ",")
}

type deadLetter struct {
	err error
	ids string
}

// deadLetters records what OnError receives.
type deadLetters struct {
	mu  sync.Mutex
	got []deadLetter
}

func (d *deadLetters) record(err error, events []*Event) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.got = append(d.got, deadLetter{err, ids(events)})
}

func (d *deadLetters) all() []deadLetter {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.got)
}

// panicky is a property value whose JSON encoding panics.
type panicky struct{}

func (panicky) MarshalJSON() ([]byte, error) { panic("boom") }

type fakeEvents struct {
	eventsv1connect.EventsServiceClient

	mu      sync.Mutex
	batches [][]*eventsv1.Event
	res     *eventsv1.BatchCreateResponse
	err     error
	// panicWith, when set, makes BatchCreate panic with it.
	panicWith string
}

func (f *fakeEvents) BatchCreate(_ context.Context, req *connect.Request[eventsv1.BatchCreateRequest]) (*connect.Response[eventsv1.BatchCreateResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.panicWith != "" {
		panic(f.panicWith)
	}
	if f.err != nil {
		return nil, f.err
	}
	f.batches = append(f.batches, req.Msg.GetEvents())
	res := f.res
	if res == nil {
		res = &eventsv1.BatchCreateResponse{}
	}
	return connect.NewResponse(res), nil
}

func (f *fakeEvents) sent() []*eventsv1.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*eventsv1.Event
	for _, b := range f.batches {
		out = append(out, b...)
	}
	return out
}

type fakeProfilesSDK struct {
	sdkprofilesv1connect.ProfilesSDKServiceClient

	mu   sync.Mutex
	reqs []*sdkprofilesv1.IdentifyRequest
	err  error
}

func (f *fakeProfilesSDK) Identify(_ context.Context, req *connect.Request[sdkprofilesv1.IdentifyRequest]) (*connect.Response[sdkprofilesv1.IdentifyResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req.Msg)
	if f.err != nil {
		return nil, f.err
	}
	return connect.NewResponse(&sdkprofilesv1.IdentifyResponse{}), nil
}

func (f *fakeProfilesSDK) calls() []*sdkprofilesv1.IdentifyRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reqs
}

func newTestClient(t *testing.T, rpc rpcClients, opts Options) (*Client, *logs) {
	t.Helper()
	l := &logs{}
	opts.Logger = slog.New(l)
	if rpc.events == nil {
		rpc.events = &fakeEvents{}
	}
	if rpc.profilesSDK == nil {
		rpc.profilesSDK = &fakeProfilesSDK{}
	}
	c := newClient(rpc, opts, testSession)
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c, l
}
