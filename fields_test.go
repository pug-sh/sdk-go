package pug

import (
	"log/slog"
	"math"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commoneventsv1 "github.com/pug-sh/sdk-go/gen/common/events/v1"
	commonv1 "github.com/pug-sh/sdk-go/gen/common/v1"
)

func TestFieldsMapToWireValues(t *testing.T) {
	events := &fakeEvents{}
	c, _ := newTestClient(t, rpcClients{events: events}, Options{})
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	c.Track("user_1", "my.custom",
		String("plan", "pro"), Int("seats", 3), Float("ratio", 1.5), Float("whole", 2), Bool("trial", true),
		Time("at", at), Duration("took", 1500*time.Millisecond), Any("tags", []string{"a", "b"}))
	_ = c.Flush(t.Context())
	sent := events.sent()
	if len(sent) != 1 {
		t.Fatalf("sent %d events", len(sent))
	}
	cp := sent[0].GetCustomProperties()
	if got := cp["plan"].GetStringValue(); got != "pro" {
		t.Errorf("plan = %v", cp["plan"])
	}
	if _, ok := cp["seats"].GetValue().(*commonv1.PropertyValue_IntValue); !ok || cp["seats"].GetIntValue() != 3 {
		t.Errorf("seats = %v, want int 3", cp["seats"])
	}
	if cp["ratio"].GetDoubleValue() != 1.5 {
		t.Errorf("ratio = %v, want double 1.5", cp["ratio"])
	}
	if _, ok := cp["whole"].GetValue().(*commonv1.PropertyValue_DoubleValue); !ok || cp["whole"].GetDoubleValue() != 2 {
		t.Errorf("whole = %v, want double 2", cp["whole"])
	}
	if _, ok := cp["trial"].GetValue().(*commonv1.PropertyValue_BoolValue); !ok || !cp["trial"].GetBoolValue() {
		t.Errorf("trial = %v, want bool true", cp["trial"])
	}
	if got := cp["at"].GetTimestampValue().GetSeconds(); got != at.Unix() {
		t.Errorf("at = %v, want %d", cp["at"], at.Unix())
	}
	if got := cp["took"].GetStringValue(); got != "1.5s" {
		t.Errorf("took = %q, want 1.5s", got)
	}
	if got := cp["tags"].GetStringValue(); got != `["a","b"]` {
		t.Errorf("tags = %q", got)
	}
}

func TestIntAcceptsEveryIntegerType(t *testing.T) {
	events := &fakeEvents{}
	c, l := newTestClient(t, rpcClients{events: events}, Options{})
	c.Track("user_1", "my.custom", Int("u8", uint8(7)), Int("neg", int64(-2)), Int("big", uint64(math.MaxUint64)))
	_ = c.Flush(t.Context())
	sent := events.sent()
	if len(sent) != 1 {
		t.Fatalf("sent %d events; a non-representable property should cost only itself", len(sent))
	}
	cp := sent[0].GetCustomProperties()
	if cp["u8"].GetIntValue() != 7 || cp["neg"].GetIntValue() != -2 {
		t.Errorf("u8 = %v, neg = %v", cp["u8"], cp["neg"])
	}
	if cp["big"] != nil {
		t.Errorf("big = %v, want it dropped", cp["big"])
	}
	if r := l.find(t, slog.LevelWarn, "property not representable"); r.attrs["property"] != "big" {
		t.Errorf("log = %v", r)
	}
}

func TestLaterValueWinsForARepeatedKey(t *testing.T) {
	events := &fakeEvents{}
	c, _ := newTestClient(t, rpcClients{events: events}, Options{})
	c.Track("user_1", "my.custom", Properties{"plan": "free", "seats": 1}, String("plan", "pro"))
	c.Track("user_1", "my.custom", String("plan", "pro"), Properties{"plan": "free"})
	_ = c.Flush(t.Context())
	sent := events.sent()
	if len(sent) != 2 {
		t.Fatalf("sent %d events", len(sent))
	}
	if cp := sent[0].GetCustomProperties(); cp["plan"].GetStringValue() != "pro" || cp["seats"].GetIntValue() != 1 {
		t.Errorf("first event = %v, want plan pro and seats 1", cp)
	}
	if cp := sent[1].GetCustomProperties(); cp["plan"].GetStringValue() != "free" {
		t.Errorf("second event = %v, want plan free", cp)
	}
}

func TestTrackSkipsNilOptions(t *testing.T) {
	events := &fakeEvents{}
	c, l := newTestClient(t, rpcClients{events: events}, Options{})
	var none Properties
	c.Track("user_1", "signup")
	c.Track("user_1", "signup", nil)
	c.Track("user_1", "signup", none)
	_ = c.Flush(t.Context())
	sent := events.sent()
	if len(sent) != 3 {
		t.Fatalf("sent %d events, want 3; logs %v", len(sent), l.records)
	}
	for i, ev := range sent {
		if cp := ev.GetCustomProperties(); len(cp) != 0 {
			t.Errorf("event %d custom properties = %v", i, cp)
		}
	}
	if errs := l.at(slog.LevelError); len(errs) != 0 {
		t.Errorf("errors logged: %v", errs)
	}
}

func TestIdentifyTakesTraitFields(t *testing.T) {
	sdk := &fakeProfilesSDK{}
	c, l := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{})
	since := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	c.Identify(t.Context(), "user_1",
		Name("Ada Lovelace"), FirstName("Ada"), LastName("Lovelace"), Email("ada@example.com"), Avatar("https://example.com/a.png"),
		String("plan", "pro"), Int("seats", 3), Bool("trial", true), Duration("tenure", 90*time.Second), Time("since", since),
		Traits{"tags": []string{"a"}}, WithAnonymousID("anon-1"))
	reqs := sdk.calls()
	if len(reqs) != 1 {
		t.Fatalf("got %d identify calls; logs %v", len(reqs), l.records)
	}
	traits := reqs[0].GetTraits().AsMap()
	for key, want := range map[string]any{
		"name": "Ada Lovelace", "firstName": "Ada", "lastName": "Lovelace", "email": "ada@example.com",
		"avatar": "https://example.com/a.png", "plan": "pro", "seats": float64(3), "trial": true,
		"tenure": "90s", "since": "2026-01-02T03:04:05Z",
	} {
		if traits[key] != want {
			t.Errorf("%s = %v, want %v", key, traits[key], want)
		}
	}
	if tags, ok := traits["tags"].([]any); !ok || len(tags) != 1 {
		t.Errorf("tags = %v", traits["tags"])
	}
	if got := reqs[0].GetAnonymousId(); got != "anon-1" {
		t.Errorf("anonymous id = %q", got)
	}
}

func TestNamedTraitWithEmptyValueIsOmitted(t *testing.T) {
	sdk := &fakeProfilesSDK{}
	c, _ := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{})
	c.Identify(t.Context(), "user_1", Email(""), FirstName("Ada"))
	traits := sdk.calls()[0].GetTraits().AsMap()
	if _, ok := traits["email"]; ok || traits["firstName"] != "Ada" {
		t.Errorf("traits = %v, want firstName and no email", traits)
	}
}

func TestLaterTraitWins(t *testing.T) {
	sdk := &fakeProfilesSDK{}
	c, _ := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{})
	c.Identify(t.Context(), "user_1", FirstName("Ada"), String("firstName", "Grace"))
	c.Identify(t.Context(), "user_2", String("firstName", "Grace"), FirstName("Ada"))
	reqs := sdk.calls()
	if len(reqs) != 2 {
		t.Fatalf("got %d identify calls", len(reqs))
	}
	if got := reqs[0].GetTraits().AsMap()["firstName"]; got != "Grace" {
		t.Errorf("first identify firstName = %v, want Grace", got)
	}
	if got := reqs[1].GetTraits().AsMap()["firstName"]; got != "Ada" {
		t.Errorf("second identify firstName = %v, want Ada", got)
	}
}

func TestNonFiniteTraitIsSkipped(t *testing.T) {
	sdk := &fakeProfilesSDK{}
	c, l := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{})
	c.Identify(t.Context(), "user_1", Float("score", math.NaN()), String("plan", "pro"))
	reqs := sdk.calls()
	if len(reqs) != 1 {
		t.Fatalf("got %d identify calls; a non-finite trait should cost only itself; logs %v", len(reqs), l.records)
	}
	traits := reqs[0].GetTraits().AsMap()
	if _, ok := traits["score"]; ok || traits["plan"] != "pro" {
		t.Errorf("traits = %v, want plan and no score", traits)
	}
	if r := l.find(t, slog.LevelWarn, "trait not representable"); r.attrs["trait"] != "score" {
		t.Errorf("log = %v", r)
	}
}

// Traits merge without copying a lone map, so dropping a bad value must not write to the
// caller's map.
func TestNonFiniteTraitLeavesTheCallersMapAlone(t *testing.T) {
	sdk := &fakeProfilesSDK{}
	c, _ := newTestClient(t, rpcClients{profilesSDK: sdk}, Options{})
	traits := Traits{"score": math.NaN(), "plan": "pro"}
	c.Identify(t.Context(), "user_1", traits)
	if _, ok := traits["score"]; !ok || len(traits) != 2 {
		t.Errorf("the caller's map was written to: %v", traits)
	}
	if got := sdk.calls()[0].GetTraits().AsMap(); got["plan"] != "pro" || got["score"] != nil {
		t.Errorf("traits = %v, want plan and no score", got)
	}
}

func TestTrackWellKnownTakesFieldExtras(t *testing.T) {
	events := &fakeEvents{}
	c, l := newTestClient(t, rpcClients{events: events}, Options{})
	c.TrackWellKnown("user_1", &commoneventsv1.PurchaseProperties{Amount: proto.Float64(49), Currency: proto.String("USD")},
		String("coupon", "FALL"), Int("amount", 1))
	_ = c.Flush(t.Context())
	sent := events.sent()
	if len(sent) != 1 {
		t.Fatalf("sent %d events; logs %v", len(sent), l.records)
	}
	cp := sent[0].GetCustomProperties()
	if _, ok := cp["amount"].GetValue().(*commonv1.PropertyValue_DoubleValue); !ok || cp["amount"].GetDoubleValue() != 49 {
		t.Errorf("amount = %v, want the typed double 49", cp["amount"])
	}
	if cp["coupon"].GetStringValue() != "FALL" {
		t.Errorf("coupon = %v", cp["coupon"])
	}
	if r := l.find(t, slog.LevelWarn, "typed field"); r.attrs["property"] != "amount" {
		t.Errorf("log = %v", r)
	}
}
