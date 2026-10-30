package pug

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	commoneventsv1 "github.com/pug-sh/sdk-go/gen/common/events/v1"
	commonv1 "github.com/pug-sh/sdk-go/gen/common/v1"
	eventsv1 "github.com/pug-sh/sdk-go/gen/sdk/events/v1"
	activityv1 "github.com/pug-sh/sdk-go/gen/shared/activity/v1"
	"github.com/pug-sh/sdk-go/gen/shared/activity/v1/activityv1connect"
	profilesv1 "github.com/pug-sh/sdk-go/gen/shared/profiles/v1"
	"github.com/pug-sh/sdk-go/gen/shared/profiles/v1/profilesv1connect"
)

func TestNewValidatesOptions(t *testing.T) {
	for _, tc := range []struct {
		opts Options
		want string
	}{
		{Options{Endpoint: "http://localhost:1"}, "APIKey is required"},
		{Options{APIKey: "pub_test", Endpoint: "http://localhost:1"}, "private key"},
		{Options{APIKey: "prv_test", Endpoint: "pug.example.com"}, "absolute http(s) URL"},
		{Options{APIKey: "prv_test", Endpoint: "ftp://pug.example.com"}, "absolute http(s) URL"},
		{Options{APIKey: "prv_test", Endpoint: "https://"}, "absolute http(s) URL"},
	} {
		c, err := New(tc.opts)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("New(%+v) = %v, want an error containing %q", tc.opts, err, tc.want)
		}
		if c != nil {
			t.Errorf("New(%+v) returned a client alongside the error", tc.opts)
		}
	}
}

func TestNewAppliesDefaults(t *testing.T) {
	c, err := New(Options{APIKey: "prv_test"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(t.Context())
	if c.timeout != defaultTimeout || c.events.SessionID == "" || c.Profiles == nil || c.Activity == nil || c.Insights == nil {
		t.Errorf("client = %+v", c)
	}
}

func TestCloseOnAnEmptyBuffer(t *testing.T) {
	c, err := New(Options{APIKey: "prv_test", Endpoint: "http://localhost:1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestTrackDropsBadDistinctIDs(t *testing.T) {
	for _, tc := range []struct{ id, want string }{
		{"", "non-empty distinctID"},
		{"cookieless-abc", "reserved 'cookieless-' prefix"},
		{"user\xff", "valid UTF-8"},
	} {
		events := &fakeEvents{}
		c, l := newTestClient(t, rpcClients{events: events}, Options{})
		c.Track(tc.id, "my.custom", Properties{"a": 1})
		_ = c.Flush(t.Context())
		l.find(t, slog.LevelError, tc.want)
		if sent := events.sent(); len(sent) != 0 {
			t.Errorf("%q: sent %v", tc.id, sent)
		}
	}
}

func TestTrackAfterCloseWarnsAndDrops(t *testing.T) {
	events := &fakeEvents{}
	dl := &deadLetters{}
	c, l := newTestClient(t, rpcClients{events: events}, Options{OnError: dl.record})
	_ = c.Close(t.Context())
	c.Track("user_1", "my.custom")
	c.TrackWellKnown("user_1", &commoneventsv1.FeatureUsedProperties{FeatureId: proto.String("x")})
	if n := len(l.warns()); n != 2 {
		t.Errorf("got %d warnings, want 2: %v", n, l.records)
	}
	if len(events.sent()) != 0 || len(dl.all()) != 0 {
		t.Errorf("sent %v, dead letters %v", events.sent(), dl.all())
	}
}

func TestTrackCarriesOptionsThroughToTheEvent(t *testing.T) {
	events := &fakeEvents{}
	c, _ := newTestClient(t, rpcClients{events: events}, Options{})
	const session = "0190a5a6-0000-7000-8000-000000000002"
	c.Track("user_1", "my.custom",
		WithTimestamp(time.Unix(0, 0)),
		WithSessionID(session),
		WithLocation(Location{
			Continent: "EU", Country: "de", Region: "BE", City: "Berlin", PostalCode: "10115",
			MetroCode: "1", Timezone: "Europe/Berlin", Coordinates: &Coordinates{Latitude: 52.52, Longitude: 13.405},
		}),
	)
	c.Track("user_1", "my.custom", WithSessionID(""))
	if err := c.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	sent := events.sent()
	if len(sent) != 2 {
		t.Fatalf("sent %d events", len(sent))
	}
	ev := sent[0]
	if got := ev.GetOccurTime().GetSeconds(); got != 0 {
		t.Errorf("occur time = %d", got)
	}
	if got := ev.GetSessionId(); got != session {
		t.Errorf("session id = %q, want %q", got, session)
	}
	ap := ev.GetAutoProperties()
	for key, want := range map[string]string{
		"$lib": "pug-go", "$sdkVersion": Version, "$platform": "server",
		"$continent": "EU", "$country": "DE", "$region": "BE", "$city": "Berlin",
		"$postalCode": "10115", "$metroCode": "1", "$timezone": "Europe/Berlin",
	} {
		if got := ap[key].GetStringValue(); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	if ap["$latitude"].GetDoubleValue() != 52.52 || ap["$longitude"].GetDoubleValue() != 13.405 {
		t.Errorf("coordinates = %v, %v", ap["$latitude"], ap["$longitude"])
	}
	if got := sent[1].GetSessionId(); got != testSession {
		t.Errorf("default session id = %q, want the client's %q", got, testSession)
	}
}

func TestTrackTakesPropertiesAmongItsOptions(t *testing.T) {
	events := &fakeEvents{}
	c, _ := newTestClient(t, rpcClients{events: events}, Options{})
	first := Properties{"plan": "free", "seats": 1}
	c.Track("user_1", "my.custom")
	c.Track("user_1", "my.custom", nil, (*Properties)(nil))
	c.Track("user_1", "my.custom", WithSessionID(testSession), first, Properties{"plan": "pro"})
	_ = c.Flush(t.Context())
	sent := events.sent()
	if len(sent) != 3 || len(sent[0].GetCustomProperties()) != 0 || len(sent[1].GetCustomProperties()) != 0 {
		t.Fatalf("sent %v", sent)
	}
	cp := sent[2].GetCustomProperties()
	if cp["plan"].GetStringValue() != "pro" || cp["seats"].GetIntValue() != 1 {
		t.Errorf("custom properties = %v, want the maps merged, the later one winning", cp)
	}
	if first["plan"] != "free" {
		t.Errorf("the caller's map was written to: %v", first)
	}
}

func TestTrackContainsPanics(t *testing.T) {
	events := &fakeEvents{}
	c, l := newTestClient(t, rpcClients{events: events}, Options{})
	c.Track("user_1", "my.custom", Properties{"ok": 1}, trackOption(func(*trackOptions) { panic("option boom") }))
	r := l.find(t, slog.LevelError, "unexpected panic in Track")
	if r.attrs["kind"] != "my.custom" || r.attrs["panic"] != "option boom" {
		t.Errorf("log = %v", r)
	}
	c.Track("user_1", "my.custom", Properties{"ok": 1, "bad": panicky{}})
	_ = c.Flush(t.Context())
	if sent := events.sent(); len(sent) != 1 || sent[0].GetCustomProperties()["ok"] == nil {
		t.Errorf("sent %v; a panicking property should cost only itself", sent)
	}
}

func TestTrackWellKnown(t *testing.T) {
	events := &fakeEvents{}
	c, l := newTestClient(t, rpcClients{events: events}, Options{})
	c.TrackWellKnown("user_1", &commoneventsv1.PurchaseProperties{
		Amount:   proto.Float64(49),
		Currency: proto.String("USD"),
	}, Properties{"coupon": "FALL", "amount": 1})
	_ = c.Flush(t.Context())
	sent := events.sent()
	if len(sent) != 1 {
		t.Fatalf("sent %d events; logs %v", len(sent), l.records)
	}
	cp := sent[0].GetCustomProperties()
	if sent[0].GetKind() != "purchase" || cp["amount"].GetDoubleValue() != 49 || cp["coupon"].GetStringValue() != "FALL" {
		t.Errorf("event = %v", sent[0])
	}
	if _, ok := cp["amount"].GetValue().(*commonv1.PropertyValue_DoubleValue); !ok {
		t.Errorf("amount = %v, want the typed double", cp["amount"])
	}
	if r := l.find(t, slog.LevelWarn, "typed field"); r.attrs["property"] != "amount" {
		t.Errorf("log = %v", r)
	}
}

func TestTrackWellKnownRefusals(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event proto.Message
		want  string
	}{
		{"nil", nil, "requires an event message"},
		{"web-only event", &commoneventsv1.PageViewProperties{}, "well-known server event"},
		{"mobile-only event", &commoneventsv1.AppOpenProperties{}, "well-known server event"},
		{"not an event", &eventsv1.Event{}, "well-known server event"},
		{"missing required field", &commoneventsv1.FeatureUsedProperties{}, "properties failed validation"},
		{"typed nil", (*commoneventsv1.FeatureUsedProperties)(nil), "properties failed validation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := &fakeEvents{}
			c, l := newTestClient(t, rpcClients{events: events}, Options{})
			c.TrackWellKnown("user_1", tc.event)
			_ = c.Flush(t.Context())
			l.find(t, slog.LevelError, tc.want)
			if sent := events.sent(); len(sent) != 0 {
				t.Errorf("sent %v", sent)
			}
		})
	}
}

func TestOnErrorReceivesUndeliverableEvents(t *testing.T) {
	events := &fakeEvents{err: connect.NewError(connect.CodeInvalidArgument, errors.New("bad batch"))}
	dl := &deadLetters{}
	c, _ := newTestClient(t, rpcClients{events: events}, Options{OnError: dl.record})
	c.Track("user_1", "my.custom")
	_ = c.Flush(t.Context())
	got := dl.all()
	if len(got) != 1 {
		t.Fatalf("dead letters = %v", got)
	}
	if perr, ok := errors.AsType[*Error](got[0].err); !ok || perr.Message != "bad batch" || !errors.Is(got[0].err, ErrInvalidArgument) {
		t.Errorf("err = %#v, want an *Error matching ErrInvalidArgument", got[0].err)
	}
	if connect.CodeOf(got[0].err) != connect.CodeInvalidArgument {
		t.Errorf("connect.CodeOf = %v", connect.CodeOf(got[0].err))
	}
}

// The batcher dead-letters raw errors; OnError gets a failed send as an *Error, like a read's.
func TestOnErrorReceivesAPanickingSendAsAnError(t *testing.T) {
	dl := &deadLetters{}
	c, _ := newTestClient(t, rpcClients{events: &fakeEvents{panicWith: "sink boom"}}, Options{OnError: dl.record})
	c.Track("user_1", "my.custom")
	_ = c.Flush(t.Context())
	got := dl.all()
	if len(got) != 1 || got[0].err.Error() != "pug: sending batch panicked: sink boom" {
		t.Fatalf("dead letters = %v", got)
	}
	if _, ok := errors.AsType[*Error](got[0].err); !ok {
		t.Errorf("err = %#v, want an *Error", got[0].err)
	}
}

func TestOnErrorReceivesAFailedSendAtCloseAsAnError(t *testing.T) {
	dl := &deadLetters{}
	events := &fakeEvents{err: connect.NewError(connect.CodeUnavailable, errors.New("down"))}
	c, _ := newTestClient(t, rpcClients{events: events}, Options{OnError: dl.record})
	c.Track("user_1", "my.custom")
	_ = c.Close(t.Context())
	got := dl.all()
	if len(got) != 1 || !errors.Is(got[0].err, ErrUnavailable) {
		t.Fatalf("dead letters = %v, want one matching ErrUnavailable", got)
	}
	if _, ok := errors.AsType[*Error](got[0].err); !ok {
		t.Errorf("err = %#v, want an *Error", got[0].err)
	}
}

func TestServerDroppedEventsAreLogged(t *testing.T) {
	l, log := newLogs()
	events := &fakeEvents{res: &eventsv1.BatchCreateResponse{
		Accepted: proto.Uint32(1), Dropped: proto.Uint32(2), DroppedByReason: map[string]uint32{"day_out_of_range": 2},
	}}
	if err := (eventSink{client: events, log: log}).Send(t.Context(), []*eventsv1.Event{{}, {}, {}}); err != nil {
		t.Fatal(err)
	}
	r := l.find(t, slog.LevelWarn, "server dropped events")
	if r.attrs["dropped"] != uint64(2) || r.attrs["sent"] != int64(3) {
		t.Errorf("log = %v", r)
	}
	if reasons, ok := r.attrs["reasons"].(map[string]uint32); !ok || reasons["day_out_of_range"] != 2 {
		t.Errorf("reasons = %v", r.attrs["reasons"])
	}
}

func TestIdentifySendsTheRequest(t *testing.T) {
	sdk := &fakeProfilesSDK{}
	c, _ := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{})
	since := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	c.Identify("user_1", Traits{"plan": "pro", "seats": 3, "tags": []string{"a", "b"}, "since": since},
		WithAnonymousID("anon-123"), WithDeviceID("device-1"))
	_ = c.Flush(t.Context())
	reqs := sdk.sentIdentifies()
	if len(reqs) != 1 {
		t.Fatalf("got %d identifies", len(reqs))
	}
	req := reqs[0]
	if req.GetExternalId() != "user_1" || req.GetAnonymousId() != "anon-123" || req.GetDeviceId() != "device-1" {
		t.Errorf("request = %v", req)
	}
	traits := req.GetTraits().AsMap()
	if traits["plan"] != "pro" || traits["seats"] != float64(3) || traits["since"] != "2026-01-02T03:04:05Z" {
		t.Errorf("traits = %v", traits)
	}
	if tags, ok := traits["tags"].([]any); !ok || len(tags) != 2 {
		t.Errorf("tags = %v", traits["tags"])
	}
}

func TestIdentifyMergesTraits(t *testing.T) {
	sdk := &fakeProfilesSDK{}
	c, _ := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{})
	c.Identify("user_1", Traits{"plan": "free", "seats": 3}, WithDeviceID("device-1"), Traits{"plan": "pro"})
	_ = c.Flush(t.Context())
	if traits := sdk.sentIdentifies()[0].GetTraits().AsMap(); traits["plan"] != "pro" || traits["seats"] != float64(3) {
		t.Errorf("traits = %v", traits)
	}
}

func TestIdentifyOmitsUnsetFields(t *testing.T) {
	sdk := &fakeProfilesSDK{}
	c, _ := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{})
	c.Identify("user_1", nil, Traits(nil), (*Traits)(nil), WithAnonymousID(""))
	_ = c.Flush(t.Context())
	req := sdk.sentIdentifies()[0]
	if req.Traits != nil || req.AnonymousId != nil || req.DeviceId != nil {
		t.Errorf("request = %v", req)
	}
}

func TestIdentifyRefusesBadInputWithoutDialing(t *testing.T) {
	for _, tc := range []struct {
		name       string
		externalID string
		opts       []IdentifyOption
		want       string
	}{
		{"empty external id", "", nil, "non-empty externalID"},
		{"anonymous id without anon-", "user_1", []IdentifyOption{WithAnonymousID("nope")}, "invalid identify request"},
		{"reserved prefix", "cookieless-abc", nil, "invalid identify request"},
		{"long device id", "user_1", []IdentifyOption{WithDeviceID(strings.Repeat("d", 37))}, "invalid identify request"},
		{"invalid UTF-8", "user\xff", nil, "valid UTF-8"},
		{"func trait", "user_1", []IdentifyOption{Traits{"f": func() {}}}, "JSON-serializable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sdk := &fakeProfilesSDK{}
			c, l := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{})
			c.Identify(tc.externalID, tc.opts...)
			_ = c.Flush(t.Context())
			l.find(t, slog.LevelError, tc.want)
			if n := sdk.attempts() + len(sdk.calls()); n != 0 {
				t.Errorf("dialed %d times", n)
			}
		})
	}
}

func TestIdentifyNamesTheReservedPrefix(t *testing.T) {
	c, l := newTestClient(t, rpcClients{}, Options{})
	c.Identify("cookieless-abc")
	if got := l.find(t, slog.LevelError, "invalid identify request").attrs["violations"].(string); !strings.Contains(got, "reserved 'cookieless-' prefix") {
		t.Errorf("violations = %q", got)
	}
}

func TestIdentifyLogsFailures(t *testing.T) {
	sdk := &fakeProfilesSDK{batchErr: connect.NewError(connect.CodeUnavailable, errors.New("down"))}
	c, l := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{})
	c.Identify("user_1")
	_ = c.Flush(t.Context())
	if r := l.find(t, slog.LevelWarn, "transient send failure; will retry"); r.attrs["identifies"] != int64(1) {
		t.Errorf("log = %v", r)
	}
}

func TestIdentifyAfterCloseWarns(t *testing.T) {
	sdk := &fakeProfilesSDK{}
	c, l := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{})
	_ = c.Close(t.Context())
	c.Identify("user_1")
	l.find(t, slog.LevelWarn, "Identify called after Close")
	if n := sdk.attempts() + len(sdk.calls()); n != 0 {
		t.Errorf("dialed %d times", n)
	}
}

func TestIdentifyQueuesUntilFlush(t *testing.T) {
	sdk := &fakeProfilesSDK{}
	c, _ := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{})
	c.Identify("user_1", String("plan", "pro"))
	c.Identify("user_2")
	if n := len(sdk.batchCalls()); n != 0 {
		t.Fatalf("sent %d batches before Flush", n)
	}
	_ = c.Flush(t.Context())
	batches := sdk.batchCalls()
	if len(batches) != 1 || externalIDs(batches[0]) != "user_1,user_2" {
		t.Errorf("batches = %v, want one holding user_1,user_2", batches)
	}
	if n := len(sdk.calls()); n != 0 {
		t.Errorf("made %d single Identify calls", n)
	}
}

func TestIdentifyFallsBackWithoutBatchIdentify(t *testing.T) {
	sdk := &fakeProfilesSDK{batchErr: connect.NewError(connect.CodeUnimplemented, errors.New("no such procedure"))}
	c, l := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{})
	c.Identify("user_1")
	c.Identify("user_2")
	_ = c.Flush(t.Context())
	c.Identify("user_3")
	_ = c.Flush(t.Context())
	if got := externalIDs(sdk.calls()); got != "user_1,user_2,user_3" {
		t.Errorf("single Identify calls = %q, want user_1,user_2,user_3", got)
	}
	if n := sdk.attempts(); n != 1 {
		t.Errorf("BatchIdentify attempted %d times, want once", n)
	}
	n := 0
	for _, r := range l.warns() {
		if strings.Contains(r.msg, "no BatchIdentify") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("logged the fallback %d times, want once: %v", n, l.records)
	}
}

func TestOnErrorReceivesUndeliverableIdentifies(t *testing.T) {
	sdk := &fakeProfilesSDK{batchErr: connect.NewError(connect.CodeInvalidArgument, errors.New("bad"))}
	dl := &deadLetters{}
	c, _ := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{OnError: dl.record})
	c.Identify("user_1")
	_ = c.Flush(t.Context())
	got := dl.all()
	if len(got) != 1 || connect.CodeOf(got[0].err) != connect.CodeInvalidArgument || got[0].identifies != "user_1" || got[0].ids != "" {
		t.Errorf("dead letters = %v", got)
	}
}

// One identify can't be split across NATS messages and NATS refuses a message over its default
// 1 MiB, so a 2 MiB identify fits a request and still can never be delivered. Queued, it would
// fail every send of its chunk.
func TestOversizedIdentifyIsDroppedWithoutBlockingTheQueue(t *testing.T) {
	sdk := &fakeProfilesSDK{}
	c, l := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{})
	c.Identify("user_big", Traits{"blob": strings.Repeat("x", 2<<20)})
	c.Identify("user_small")
	_ = c.Flush(t.Context())
	if got := externalIDs(sdk.sentIdentifies()); got != "user_small" {
		t.Errorf("sent %q, want only user_small", got)
	}
	if r := l.find(t, slog.LevelError, "too large to send"); r.attrs["limit"] != int64(900<<10) {
		t.Errorf("log = %v", r)
	}
}

// A slow server without BatchIdentify can't finish a chunk within one Timeout; each attempt
// must still make progress instead of resending the same first items.
func TestIdentifyFallbackMakesProgressPastTheTimeout(t *testing.T) {
	sdk := &fakeProfilesSDK{
		batchErr:      connect.NewError(connect.CodeUnimplemented, errors.New("no such procedure")),
		identifyDelay: 20 * time.Millisecond,
	}
	c, l := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{Timeout: 100 * time.Millisecond, Batch: BatchConfig{MaxWait: time.Hour}})
	for i := range 20 {
		c.Identify(fmt.Sprintf("user_%02d", i))
	}
	for i := 0; i < 30 && len(sdk.calls()) < 20; i++ {
		_ = c.Flush(t.Context())
	}
	seen := map[string]int{}
	for _, r := range sdk.calls() {
		seen[r.GetExternalId()]++
	}
	if len(seen) != 20 {
		t.Errorf("delivered %d distinct users, want 20; logs %v", len(seen), l.records)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("%s delivered %d times, want once", id, n)
		}
	}
}

func TestCloseSendsQueuedEventsAndIdentifies(t *testing.T) {
	events := &fakeEvents{}
	sdk := &fakeProfilesSDK{}
	c, _ := newTestClient(t, rpcClients{events: events, profilesSDK: sdk}, Options{})
	c.Track("user_1", "my.custom")
	c.Identify("user_1")
	if err := c.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := len(events.sent()); n != 1 {
		t.Errorf("sent %d events, want 1", n)
	}
	if batches := sdk.batchCalls(); len(batches) != 1 || externalIDs(batches[0]) != "user_1" {
		t.Errorf("batches = %v, want one holding user_1", batches)
	}
}

type fakeProfiles struct {
	profilesv1connect.ProfilesServiceClient

	got      *profilesv1.GetRequest
	deadline time.Duration
	err      error
}

func (f *fakeProfiles) Get(ctx context.Context, req *connect.Request[profilesv1.GetRequest]) (*connect.Response[profilesv1.GetResponse], error) {
	f.got = req.Msg
	if d, ok := ctx.Deadline(); ok {
		f.deadline = time.Until(d)
	}
	if f.err != nil {
		return nil, f.err
	}
	return connect.NewResponse(&profilesv1.GetResponse{Profile: &profilesv1.Profile{Id: req.Msg.Id}}), nil
}

type fakeActivity struct {
	activityv1connect.ActivityServiceClient

	sessions *activityv1.GetProfileSessionsRequest
	schema   *commonv1.GetFilterSchemaRequest
}

func (f *fakeActivity) GetProfileSessions(_ context.Context, req *connect.Request[activityv1.GetProfileSessionsRequest]) (*connect.Response[activityv1.GetProfileSessionsResponse], error) {
	f.sessions = req.Msg
	return connect.NewResponse(&activityv1.GetProfileSessionsResponse{NextPageToken: proto.String("next")}), nil
}

func (f *fakeActivity) GetFilterSchema(_ context.Context, req *connect.Request[commonv1.GetFilterSchemaRequest]) (*connect.Response[commonv1.GetFilterSchemaResponse], error) {
	f.schema = req.Msg
	return connect.NewResponse(&commonv1.GetFilterSchemaResponse{}), nil
}

func TestReadsPassThroughToTheRPC(t *testing.T) {
	profiles, activity := &fakeProfiles{}, &fakeActivity{}
	c, _ := newTestClient(t, rpcClients{profiles: profiles, activity: activity}, Options{})

	req := &activityv1.GetProfileSessionsRequest{DistinctId: proto.String("p1"), IncludeBots: proto.Bool(true)}
	res, err := c.Activity.ProfileSessions(t.Context(), req)
	if err != nil || res.GetNextPageToken() != "next" || activity.sessions != req {
		t.Errorf("ProfileSessions = %v, %v; server got %v", res, err, activity.sessions)
	}

	profile, err := c.Profiles.Get(t.Context(), "p1", IncludeBots())
	if err != nil || profile.GetId() != "p1" {
		t.Errorf("Get = %v, %v", profile, err)
	}
	if !proto.Equal(profiles.got, &profilesv1.GetRequest{Id: proto.String("p1"), IncludeBots: proto.Bool(true)}) {
		t.Errorf("server got %v", profiles.got)
	}
	if _, err := c.Profiles.Get(t.Context(), "p1"); err != nil || profiles.got.IncludeBots != nil {
		t.Errorf("Get without IncludeBots sent %v, %v", profiles.got, err)
	}

	if _, err := c.Activity.FilterSchema(t.Context(), nil); err != nil || activity.schema == nil {
		t.Errorf("FilterSchema(nil) = %v; server got %v", err, activity.schema)
	}
}

func TestReadsNormalizeErrors(t *testing.T) {
	cerr := connect.NewError(connect.CodeNotFound, errors.New("no such profile"))
	c, _ := newTestClient(t, rpcClients{profiles: &fakeProfiles{err: cerr}}, Options{})
	profile, err := c.Profiles.Get(t.Context(), "p1")
	if profile != nil {
		t.Errorf("profile = %v", profile)
	}
	var perr *Error
	if !errors.As(err, &perr) || perr.Code != connect.CodeNotFound || perr.Message != "no such profile" || !errors.Is(err, cerr) {
		t.Fatalf("err = %#v", err)
	}
	if got := err.Error(); got != "pug: not_found: no such profile" {
		t.Errorf("Error() = %q", got)
	}
}

func TestReadsDefaultTheirTimeout(t *testing.T) {
	profiles := &fakeProfiles{}
	c, _ := newTestClient(t, rpcClients{profiles: profiles}, Options{Timeout: time.Minute})
	_, _ = c.Profiles.Get(t.Context(), "p1")
	if profiles.deadline <= 59*time.Second || profiles.deadline > time.Minute {
		t.Errorf("deadline in %v, want the 1m default", profiles.deadline)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	defer cancel()
	_, _ = c.Profiles.Get(ctx, "p1")
	if profiles.deadline <= 59*time.Minute {
		t.Errorf("deadline in %v, want the caller's hour", profiles.deadline)
	}
}

func TestNegativeTimeoutFallsBack(t *testing.T) {
	c, l := newTestClient(t, rpcClients{}, Options{Timeout: -time.Second})
	if c.timeout != defaultTimeout {
		t.Errorf("timeout = %v", c.timeout)
	}
	l.find(t, slog.LevelWarn, "Timeout must not be negative")
}

func TestNewRejectsMalformedAPIKeys(t *testing.T) {
	for _, key := range []string{"prv_test\n", " prv_test", "prv_te st", "prv_tëst", "prv_\x00"} {
		if _, err := New(Options{APIKey: key}); err == nil || !strings.Contains(err.Error(), "printable ASCII") {
			t.Errorf("New(%q) = %v, want a malformed-key error", key, err)
		}
	}
}
