package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commoneventsv1 "github.com/pug-sh/sdk-go/gen/common/events/v1"
	commonv1 "github.com/pug-sh/sdk-go/gen/common/v1"
	eventsv1 "github.com/pug-sh/sdk-go/gen/sdk/events/v1"
)

func testBuilder(log Logger) Builder {
	return Builder{Log: log, SessionID: testSession, Version: testVersion}
}

// track builds an event as Client.Track does; opts, if given, holds the one event's Options.
func track(t *testing.T, kind string, props Properties, opts ...Options) (*eventsv1.Event, *logs) {
	t.Helper()
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	l, log := newLogs()
	return testBuilder(log).Build(kind, "user_1", props, o), l
}

func mustTrack(t *testing.T, kind string, props Properties, opts ...Options) (*eventsv1.Event, *logs) {
	t.Helper()
	ev, l := track(t, kind, props, opts...)
	if ev == nil {
		t.Fatalf("event dropped; logs: %v", l.records)
	}
	return ev, l
}

func strOf(t *testing.T, pv *commonv1.PropertyValue) string {
	t.Helper()
	v, ok := pv.GetValue().(*commonv1.PropertyValue_StringValue)
	if !ok {
		t.Fatalf("got %v, want a string value", pv)
	}
	return v.StringValue
}

func keys(m map[string]*commonv1.PropertyValue) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func TestCustomEventMapsPropertyTypes(t *testing.T) {
	ev, _ := mustTrack(t, "my.custom", Properties{
		"amount": 5,
		"ratio":  1.5,
		"ok":     true,
		"name":   "hello",
		"when":   time.Unix(0, 0),
	})
	cp := ev.GetCustomProperties()
	if cp["amount"].GetIntValue() != 5 {
		t.Errorf("amount = %v, want int 5", cp["amount"])
	}
	if cp["ratio"].GetDoubleValue() != 1.5 {
		t.Errorf("ratio = %v, want double 1.5", cp["ratio"])
	}
	if _, ok := cp["ok"].GetValue().(*commonv1.PropertyValue_BoolValue); !ok {
		t.Errorf("ok = %v, want a bool", cp["ok"])
	}
	if got := strOf(t, cp["name"]); got != "hello" {
		t.Errorf("name = %q", got)
	}
	if cp["when"].GetTimestampValue().GetSeconds() != 0 {
		t.Errorf("when = %v, want the epoch", cp["when"])
	}
	if ev.GetKind() != "my.custom" || ev.GetDistinctId() != "user_1" || ev.GetSessionId() != testSession {
		t.Errorf("identity = %q %q %q", ev.GetKind(), ev.GetDistinctId(), ev.GetSessionId())
	}
}

func TestEventCarriesSDKAutoProperties(t *testing.T) {
	ev, _ := mustTrack(t, "my.custom", nil)
	ap := ev.GetAutoProperties()
	if got := keys(ap); !slices.Equal(got, []string{"$lib", "$platform", "$sdkVersion"}) {
		t.Fatalf("auto properties = %v", got)
	}
	if strOf(t, ap["$lib"]) != "pug-go" || strOf(t, ap["$platform"]) != "server" || strOf(t, ap["$sdkVersion"]) != testVersion {
		t.Errorf("auto properties = %v", ap)
	}
}

func TestEventFailingValidationIsDropped(t *testing.T) {
	ev, l := track(t, "order completed", nil)
	if ev != nil {
		t.Fatal("want the event dropped")
	}
	r := l.find(t, slog.LevelError, "failed validation")
	if !strings.Contains(r.attrs["violations"].(string), "kind") {
		t.Errorf("violations = %v", r.attrs["violations"])
	}
}

func TestEmptySessionIDKeepsTheDefault(t *testing.T) {
	ev, _ := mustTrack(t, "my.custom", nil, Options{SessionID: ""})
	if ev.GetSessionId() != testSession {
		t.Errorf("session = %q", ev.GetSessionId())
	}
}

// uuid.Validate also accepts braced and hyphenless forms, which the session_id rule refuses.
func TestNonUUIDSessionIDKeepsTheDefault(t *testing.T) {
	for _, id := range []string{"not-a-uuid", "{0190a5a6-0000-7000-8000-000000000002}", "0190a5a6000070008000000000000002"} {
		ev, l := mustTrack(t, "my.custom", nil, Options{SessionID: id})
		if ev.GetSessionId() != testSession {
			t.Errorf("%q: session = %q, want the default", id, ev.GetSessionId())
		}
		l.find(t, slog.LevelWarn, "must be a UUID")
	}
}

func TestDollarPrefixedCustomKeyDropsTheEvent(t *testing.T) {
	if ev, _ := track(t, "my.custom", Properties{"$plan": "pro"}); ev != nil {
		t.Fatal("want the event dropped")
	}
}

func TestKindOutsideTheCharsetDropsTheEvent(t *testing.T) {
	if ev, _ := track(t, "order completed", nil); ev != nil {
		t.Fatal("want the event dropped")
	}
}

func TestNonRepresentableValuesAreSkipped(t *testing.T) {
	ev, l := mustTrack(t, "my.custom", Properties{
		"inf":   math.Inf(1),
		"nan":   math.NaN(),
		"fn":    func() {},
		"huge":  uint64(math.MaxUint64),
		"cmplx": complex(1, 2),
		"good":  "x",
	})
	if got := keys(ev.GetCustomProperties()); !slices.Equal(got, []string{"good"}) {
		t.Errorf("custom properties = %v, want only good", got)
	}
	if n := len(l.warns()); n != 5 {
		t.Errorf("got %d warnings, want 5: %v", n, l.records)
	}
}

type plan string

type tier int

type address struct {
	City string `json:"city"`
}

type textID [2]byte

func (id textID) MarshalText() ([]byte, error) { return []byte("id-" + string(id[:])), nil }

type panicky struct{}

func (panicky) MarshalJSON() ([]byte, error) { panic("boom") }

func TestCustomValuesMapByGoType(t *testing.T) {
	name := "ada"
	var nilPtr *string
	ts := time.Date(2026, 5, 17, 4, 30, 0, 0, time.UTC)
	ev, l := mustTrack(t, "my.custom", Properties{
		"float":      float64(10),
		"float32":    float32(0.5),
		"uint8":      uint8(7),
		"named":      plan("pro"),
		"namedInt":   tier(3),
		"ptr":        &name,
		"nilPtr":     nilPtr,
		"nil":        nil,
		"nilMap":     map[string]int(nil),
		"timePtr":    &ts,
		"tspb":       timestamppb.New(ts),
		"duration":   1500 * time.Millisecond,
		"durpb":      &durationpb.Duration{Seconds: 90},
		"numInt":     json.Number("42"),
		"numFloat":   json.Number("4.5"),
		"text":       textID{'a', 'b'},
		"addr":       netip.MustParseAddr("10.0.0.1"),
		"struct":     address{City: "Berlin & <Mitte>"},
		"slice":      []string{"a", "b"},
		"emptySlice": []string{},
		"bytes":      []byte{1, 2, 3},
		"raw":        json.RawMessage(`{"a":1}`),
		"panicky":    panicky{},
	})
	cp := ev.GetCustomProperties()
	for key, want := range map[string]*commonv1.PropertyValue{
		"float":      doubleValue(10),
		"float32":    doubleValue(0.5),
		"uint8":      intValue(7),
		"named":      stringValue(Logger{}, "pro"),
		"namedInt":   intValue(3),
		"ptr":        stringValue(Logger{}, "ada"),
		"duration":   stringValue(Logger{}, "1.5s"),
		"durpb":      stringValue(Logger{}, "90s"),
		"numInt":     intValue(42),
		"numFloat":   doubleValue(4.5),
		"text":       stringValue(Logger{}, "id-ab"),
		"addr":       stringValue(Logger{}, "10.0.0.1"),
		"struct":     stringValue(Logger{}, `{"city":"Berlin & <Mitte>"}`),
		"slice":      stringValue(Logger{}, `["a","b"]`),
		"emptySlice": stringValue(Logger{}, `[]`),
		"bytes":      stringValue(Logger{}, "AQID"),
		"raw":        stringValue(Logger{}, `{"a":1}`),
	} {
		if !proto.Equal(cp[key], want) {
			t.Errorf("%s = %v, want %v", key, cp[key], want)
		}
	}
	for _, key := range []string{"timePtr", "tspb"} {
		if got := cp[key].GetTimestampValue().AsTime(); !got.Equal(ts) {
			t.Errorf("%s = %v, want %v", key, got, ts)
		}
	}
	for _, key := range []string{"nilPtr", "nil", "nilMap", "panicky"} {
		if _, ok := cp[key]; ok {
			t.Errorf("%s should be omitted, got %v", key, cp[key])
		}
	}
	if r := l.find(t, slog.LevelWarn, "not representable"); r.attrs["property"] != "panicky" {
		t.Errorf("warned about %v, want panicky", r.attrs["property"])
	}
}

func TestStringsAreRepairedAndTruncated(t *testing.T) {
	long := strings.Repeat("a", 1023) + "é"
	ev, l := mustTrack(t, "my.custom", Properties{
		"bad\xffkey": "bad\xffvalue",
		"long":       long,
	})
	cp := ev.GetCustomProperties()
	if got := strOf(t, cp["bad\uFFFDkey"]); got != "bad\uFFFDvalue" {
		t.Errorf("repaired value = %q", got)
	}
	// "é" is two bytes straddling the cap, so the cut backs up to the rune boundary.
	if got := strOf(t, cp["long"]); got != strings.Repeat("a", 1023) {
		t.Errorf("truncated to %d bytes, want 1023", len(got))
	}
	l.find(t, slog.LevelWarn, "exceeds 1024 bytes")
	if _, err := proto.Marshal(ev); err != nil {
		t.Fatalf("marshal: %v", err)
	}
}

func TestWellKnownCatalogIsNotEmpty(t *testing.T) {
	if len(wellKnown().byKind) == 0 {
		t.Fatal("empty catalog")
	}
}

func TestWellKnownFieldsUseTheirProtoType(t *testing.T) {
	ev, _ := mustTrack(t, "purchase", Properties{"amount": 9.99, "quantity": 2, "currency": "USD"})
	cp := ev.GetCustomProperties()
	if cp["amount"].GetDoubleValue() != 9.99 || cp["quantity"].GetIntValue() != 2 || strOf(t, cp["currency"]) != "USD" {
		t.Errorf("custom properties = %v", cp)
	}
}

// amount is a proto double, so a whole number must stay a double even when passed as an int.
func TestWellKnownWholeNumberDoubleStaysDouble(t *testing.T) {
	ev, _ := mustTrack(t, "purchase", Properties{"amount": 10, "quantity": 1.0, "currency": "USD"})
	cp := ev.GetCustomProperties()
	if _, ok := cp["amount"].GetValue().(*commonv1.PropertyValue_DoubleValue); !ok {
		t.Errorf("amount = %v, want a double", cp["amount"])
	}
	if _, ok := cp["quantity"].GetValue().(*commonv1.PropertyValue_IntValue); !ok {
		t.Errorf("quantity = %v, want an int", cp["quantity"])
	}
}

func TestWellKnownExtrasAndUnsetFields(t *testing.T) {
	ev, _ := mustTrack(t, "purchase", Properties{"amount": 5, "currency": "USD", "referrer": "news", "product_id": "p1"})
	cp := ev.GetCustomProperties()
	if got := keys(cp); !slices.Equal(got, []string{"amount", "currency", "product_id", "referrer"}) {
		t.Errorf("custom properties = %v", got)
	}
}

func TestWellKnownProtoNameGetsAHint(t *testing.T) {
	ev, l := mustTrack(t, "purchase", Properties{"amount": 5, "product_id": "p1"})
	if strOf(t, ev.GetCustomProperties()["product_id"]) != "p1" {
		t.Errorf("custom properties = %v", ev.GetCustomProperties())
	}
	if w := l.warns(); len(w) != 1 || w[0].attrs["property"] != "product_id" || w[0].attrs["json_name"] != "productId" {
		t.Errorf("warnings = %v", w)
	}

	ev, l = track(t, "feature_used", Properties{"feature_id": "export"})
	if ev != nil {
		t.Fatal("want the event dropped")
	}
	if r := l.find(t, slog.LevelWarn, "did you mean"); r.attrs["json_name"] != "featureId" {
		t.Errorf("log = %v", r)
	}

	l, log := newLogs()
	refund := &commoneventsv1.OrderRefundedProperties{Amount: proto.Float64(5)}
	if testBuilder(log).BuildWellKnown("order_refunded", "user_1", refund.ProtoReflect(), Properties{"order_id": "o1"}, Options{}) != nil {
		t.Fatal("want the typed event dropped")
	}
	if r := l.find(t, slog.LevelWarn, "did you mean"); r.attrs["json_name"] != "orderId" {
		t.Errorf("log = %v", r)
	}
}

func TestWellKnownNilFieldIsUnset(t *testing.T) {
	var sku *string
	ev, _ := mustTrack(t, "purchase", Properties{"amount": 5, "sku": sku, "brand": nil})
	if got := keys(ev.GetCustomProperties()); !slices.Equal(got, []string{"amount"}) {
		t.Errorf("custom properties = %v", got)
	}
}

func TestWellKnownPointerFieldsAreFollowed(t *testing.T) {
	ev, _ := mustTrack(t, "purchase", Properties{"amount": proto.Float64(5), "productId": proto.String("p1")})
	cp := ev.GetCustomProperties()
	if cp["amount"].GetDoubleValue() != 5 || strOf(t, cp["productId"]) != "p1" {
		t.Errorf("custom properties = %v", cp)
	}
}

func TestWellKnownWrongTypeDropsTheEvent(t *testing.T) {
	base := Properties{"productId": "p1", "amount": 9.99, "currency": "USD", "quantity": 2}
	for _, tc := range []struct {
		field string
		value any
		want  string
	}{
		{"quantity", 1.5, "quantity: expected int32, got 1.5"},
		{"quantity", 1 << 31, "quantity: 2147483648 overflows int32"},
		{"quantity", "2", "quantity: expected int32, got string"},
		{"quantity", true, "quantity: expected int32, got bool"},
		{"amount", "9.99", "amount: expected double, got string"},
		{"amount", json.Number("x"), "amount: expected double, got json.Number"},
		{"productId", 123, "productId: expected string, got int"},
		{"productId", json.Number("123"), "productId: expected string, got json.Number"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			props := maps.Clone(base)
			props[tc.field] = tc.value
			ev, l := track(t, "purchase", props)
			if ev != nil {
				t.Fatal("want the event dropped")
			}
			if got := l.find(t, slog.LevelError, "invalid properties").attrs["err"]; got != tc.want {
				t.Errorf("err = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWellKnownFailingValidationDropsTheEvent(t *testing.T) {
	ev, l := track(t, "purchase", Properties{"amount": 0, "currency": "usd"})
	if ev != nil {
		t.Fatal("want the event dropped")
	}
	got := l.find(t, slog.LevelError, "properties failed validation").attrs["violations"].(string)
	if !strings.Contains(got, "amount") || !strings.Contains(got, "currency") {
		t.Errorf("violations = %q", got)
	}
}

func TestWellKnownMissingRequiredFieldDropsTheEvent(t *testing.T) {
	if ev, _ := track(t, "feature_used", Properties{"featureName": "Export"}); ev != nil {
		t.Fatal("want the event dropped")
	}
}

func TestWellKnownInt64Fields(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  int64
	}{
		{"int", 1024, 1024},
		{"whole float", 1024.0, 1024},
		{"json number", json.Number("9007199254740993"), 9007199254740993},
		{"json exponent", json.Number("1e3"), 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev, _ := mustTrack(t, "file_uploaded", Properties{"fileId": "f1", "sizeBytes": tc.value})
			if got := ev.GetCustomProperties()["sizeBytes"]; !proto.Equal(got, intValue(tc.want)) {
				t.Errorf("sizeBytes = %v, want int %d", got, tc.want)
			}
		})
	}
}

func TestWellKnownInt64Refusals(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"string", " ", "sizeBytes: expected int64, got string"},
		{"float past 2^53", float64(1 << 53), "sizeBytes: expected a safe integer, got 9007199254740992"},
		{"uint64 past int64", uint64(math.MaxUint64), "sizeBytes: 18446744073709551615 overflows int64"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev, l := track(t, "file_uploaded", Properties{"fileId": "f1", "sizeBytes": tc.value})
			if ev != nil {
				t.Fatal("want the event dropped")
			}
			if got := l.find(t, slog.LevelError, "invalid properties").attrs["err"]; got != tc.want {
				t.Errorf("err = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWellKnownStringWithInvalidUTF8IsKept(t *testing.T) {
	ev, _ := mustTrack(t, "purchase", Properties{"productId": "gift \xf0\x9f"})
	if got := strOf(t, ev.GetCustomProperties()["productId"]); got != "gift \uFFFD" {
		t.Errorf("productId = %q", got)
	}
}

func TestWellKnownDurationWireForm(t *testing.T) {
	for _, tc := range []struct {
		position any
		want     string
	}{
		{&durationpb.Duration{}, "0s"},
		{&durationpb.Duration{Nanos: 500_000_000}, "0.5s"},
		{&durationpb.Duration{Seconds: 1, Nanos: 5_000_000}, "1.005s"},
		{&durationpb.Duration{Seconds: 90}, "90s"},
		{&durationpb.Duration{Seconds: 90, Nanos: 500_000_000}, "90.5s"},
		{&durationpb.Duration{Seconds: 1, Nanos: -500_000_000}, "0.5s"},
		{&durationpb.Duration{Seconds: 1, Nanos: 1_000_000_000}, "2s"},
		{90 * time.Second, "90s"},
		{1500 * time.Millisecond, "1.5s"},
		{time.Duration(0), "0s"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			ev, _ := mustTrack(t, "video_play", Properties{"videoId": "v1", "position": tc.position})
			if got := strOf(t, ev.GetCustomProperties()["position"]); got != tc.want {
				t.Errorf("position = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWellKnownDurationRefusals(t *testing.T) {
	for _, position := range []any{90, "90s", map[string]any{"ms": 90_000}, time.Unix(90, 0)} {
		ev, l := track(t, "video_play", Properties{"videoId": "v1", "position": position})
		if ev != nil {
			t.Fatalf("%v: want the event dropped", position)
		}
		got := l.find(t, slog.LevelError, "invalid properties").attrs["err"].(string)
		if !strings.HasPrefix(got, "position: expected a Duration (time.Duration or *durationpb.Duration), got ") {
			t.Errorf("err = %q", got)
		}
	}
}

func TestWellKnownNegativeDurationFailsValidation(t *testing.T) {
	if ev, _ := track(t, "video_play", Properties{"videoId": "v1", "position": -time.Second}); ev != nil {
		t.Fatal("want the event dropped")
	}
}

func TestUnsetDurationDoesNotWarn(t *testing.T) {
	_, l := mustTrack(t, "video_play", Properties{"videoId": "v1"})
	if w := l.warns(); len(w) != 0 {
		t.Errorf("warnings = %v", w)
	}
}

func TestTypedEventMatchesTheUntypedOne(t *testing.T) {
	_, log := newLogs()
	msg := &commoneventsv1.VideoSeekedProperties{
		VideoId:      proto.String("v1"),
		FromPosition: durationpb.New(5 * time.Second),
		ToPosition:   durationpb.New(9500 * time.Millisecond),
	}
	b := testBuilder(log)
	typed := b.BuildWellKnown("video_seeked", "user_1", msg.ProtoReflect(), Properties{"source": "scrubber"}, Options{})
	untyped := b.Build("video_seeked", "user_1", Properties{
		"videoId": "v1", "fromPosition": 5 * time.Second, "toPosition": 9500 * time.Millisecond, "source": "scrubber",
	}, Options{})
	if typed == nil || untyped == nil {
		t.Fatal("event dropped")
	}
	if !proto.Equal(&eventsv1.Event{CustomProperties: typed.GetCustomProperties()}, &eventsv1.Event{CustomProperties: untyped.GetCustomProperties()}) {
		t.Errorf("typed %v != untyped %v", typed.GetCustomProperties(), untyped.GetCustomProperties())
	}
}

func TestOccurTime(t *testing.T) {
	ev, _ := mustTrack(t, "my.custom", nil, Options{Timestamp: time.UnixMilli(1_700_000_000_000), HasTimestamp: true})
	if got := ev.GetOccurTime().GetSeconds(); got != 1_700_000_000 {
		t.Errorf("occur time = %d", got)
	}
	ev, _ = mustTrack(t, "my.custom", nil, Options{Timestamp: time.Unix(0, 0), HasTimestamp: true})
	if got := ev.GetOccurTime().AsTime(); !got.Equal(time.Unix(0, 0)) {
		t.Errorf("occur time = %v, want the epoch", got)
	}
	ev, _ = mustTrack(t, "my.custom", nil, Options{Timestamp: time.UnixMilli(1_700_000_000_000).Add(1234 * time.Microsecond), HasTimestamp: true})
	if got := ev.GetOccurTime().GetNanos(); got != 1_234_000 {
		t.Errorf("occur nanos = %d", got)
	}
}

func TestOutOfRangeTimestampFallsBackToNow(t *testing.T) {
	for _, ts := range []time.Time{{}, time.Unix(-1, 0), time.Unix(1_700_000_000_000, 0)} {
		before := time.Now().Add(-time.Second)
		ev, l := mustTrack(t, "my.custom", nil, Options{Timestamp: ts, HasTimestamp: true})
		if got := ev.GetOccurTime().AsTime(); got.Before(before) || got.After(time.Now().Add(time.Second)) {
			t.Errorf("%v: occur time = %v, want now", ts, got)
		}
		l.find(t, slog.LevelWarn, "out-of-range track timestamp")
	}
}

func location(t *testing.T, loc Location) (*eventsv1.Event, *logs) {
	t.Helper()
	return mustTrack(t, "my.custom", nil, Options{Location: &loc})
}

func TestLocationRendersEveryField(t *testing.T) {
	ev, l := location(t, Location{
		Continent: "EU", Country: "DE", Region: "Berlin", City: " Berlin ", PostalCode: "10115",
		MetroCode: "807", Timezone: "Europe/Berlin", Coordinates: &Coordinates{Latitude: 52.52, Longitude: 13.405},
	})
	ap := ev.GetAutoProperties()
	for key, want := range map[string]string{
		"$continent": "EU", "$country": "DE", "$region": "Berlin", "$city": "Berlin",
		"$postalCode": "10115", "$metroCode": "807", "$timezone": "Europe/Berlin",
	} {
		if got := strOf(t, ap[key]); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	if ap["$latitude"].GetDoubleValue() != 52.52 || ap["$longitude"].GetDoubleValue() != 13.405 {
		t.Errorf("coordinates = %v %v", ap["$latitude"], ap["$longitude"])
	}
	if w := l.warns(); len(w) != 0 {
		t.Errorf("warnings = %v", w)
	}
}

func TestLocationCoordinatesAreAlwaysDoubles(t *testing.T) {
	ev, _ := location(t, Location{Coordinates: &Coordinates{Latitude: 52, Longitude: 13}})
	for _, key := range []string{"$latitude", "$longitude"} {
		if _, ok := ev.GetAutoProperties()[key].GetValue().(*commonv1.PropertyValue_DoubleValue); !ok {
			t.Errorf("%s = %v, want a double", key, ev.GetAutoProperties()[key])
		}
	}
}

func TestLocationAcceptsPolesAndAntimeridian(t *testing.T) {
	ev, _ := location(t, Location{Coordinates: &Coordinates{Latitude: -90, Longitude: 180}})
	if ev.GetAutoProperties()["$latitude"].GetDoubleValue() != -90 || ev.GetAutoProperties()["$longitude"].GetDoubleValue() != 180 {
		t.Errorf("auto properties = %v", ev.GetAutoProperties())
	}
}

// A lone coordinate reads as a real position with the other axis at 0.
func TestLocationBadAxisCostsThePair(t *testing.T) {
	for _, c := range []Coordinates{{100, 13}, {45, 181}, {math.NaN(), 13}, {45, math.Inf(-1)}} {
		ev, l := location(t, Location{City: "Berlin", Coordinates: &c})
		ap := ev.GetAutoProperties()
		if _, ok := ap["$latitude"]; ok {
			t.Errorf("%v: latitude sent", c)
		}
		if _, ok := ap["$longitude"]; ok {
			t.Errorf("%v: longitude sent", c)
		}
		if strOf(t, ap["$city"]) != "Berlin" {
			t.Errorf("%v: city lost", c)
		}
		l.find(t, slog.LevelWarn, "ignoring location coordinates")
	}
	ev, _ := location(t, Location{Coordinates: &Coordinates{Latitude: 45, Longitude: 100}})
	if ev.GetAutoProperties()["$longitude"].GetDoubleValue() != 100 {
		t.Error("longitude 100 is within ±180")
	}
}

func TestLocationNormalizesCountry(t *testing.T) {
	ev, _ := location(t, Location{Country: " de "})
	if got := strOf(t, ev.GetAutoProperties()["$country"]); got != "DE" {
		t.Errorf("country = %q", got)
	}
}

func TestLocationDropsUnknownCountry(t *testing.T) {
	ev, l := location(t, Location{Country: "XX", City: "Berlin"})
	if _, ok := ev.GetAutoProperties()["$country"]; ok {
		t.Error("country sent")
	}
	if strOf(t, ev.GetAutoProperties()["$city"]) != "Berlin" {
		t.Error("city lost")
	}
	l.find(t, slog.LevelWarn, "ISO 3166-1")
}

func TestLocationWithNoUsableFieldWarns(t *testing.T) {
	for _, loc := range []Location{{}, {City: "  ", Country: ""}} {
		ev, l := location(t, loc)
		if got := keys(ev.GetAutoProperties()); !slices.Equal(got, []string{"$lib", "$platform", "$sdkVersion"}) {
			t.Errorf("auto properties = %v", got)
		}
		l.find(t, slog.LevelWarn, "no usable fields")
	}
}

func TestNoLocationWritesNoGeo(t *testing.T) {
	ev, l := mustTrack(t, "my.custom", nil)
	if got := keys(ev.GetAutoProperties()); !slices.Equal(got, []string{"$lib", "$platform", "$sdkVersion"}) {
		t.Errorf("auto properties = %v", got)
	}
	if len(l.records) != 0 {
		t.Errorf("logs = %v", l.records)
	}
}

func TestOversizedEventIsDropped(t *testing.T) {
	props := Properties{}
	value := strings.Repeat("x", maxStringBytes)
	for i := range maxBatchBytes/maxStringBytes + 1 {
		props[fmt.Sprintf("p%06d", i)] = value
	}
	ev, l := track(t, "my.custom", props)
	if ev != nil {
		t.Fatal("want the event dropped")
	}
	l.find(t, slog.LevelError, "too large")
}

func TestScalarConversions(t *testing.T) {
	for _, tc := range []struct {
		kind protoreflect.Kind
		in   any
		want any
		err  string
	}{
		{protoreflect.Uint32Kind, 7, uint32(7), ""},
		{protoreflect.Uint32Kind, -1, nil, "-1 overflows uint32"},
		{protoreflect.Uint32Kind, uint64(1 << 32), nil, "4294967296 overflows uint32"},
		{protoreflect.Uint64Kind, uint64(math.MaxUint64), uint64(math.MaxUint64), ""},
		{protoreflect.Uint64Kind, json.Number("18446744073709551615"), uint64(math.MaxUint64), ""},
		{protoreflect.Uint64Kind, 2.0, uint64(2), ""},
		{protoreflect.Int64Kind, json.Number("2.5"), nil, "expected int64, got 2.5"},
		{protoreflect.Int64Kind, json.Number("abc"), nil, `expected int64, got "abc"`},
		{protoreflect.Int32Kind, math.Inf(1), nil, "expected int32, got +Inf"},
		{protoreflect.FloatKind, 1.5, float32(1.5), ""},
		{protoreflect.DoubleKind, uint8(3), float64(3), ""},
		{protoreflect.BoolKind, "true", nil, "expected bool, got string"},
		{protoreflect.BytesKind, []byte("x"), nil, errUnsupportedField.Error()},
	} {
		got, err := scalar(tc.kind, reflect.ValueOf(tc.in))
		switch {
		case tc.err != "":
			if err == nil || err.Error() != tc.err {
				t.Errorf("%s(%v) error = %v, want %q", tc.kind, tc.in, err, tc.err)
			}
		case err != nil:
			t.Errorf("%s(%v) = %v", tc.kind, tc.in, err)
		case got.Interface() != tc.want:
			t.Errorf("%s(%v) = %v (%T), want %v (%T)", tc.kind, tc.in, got.Interface(), got.Interface(), tc.want, tc.want)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	for _, tc := range []struct {
		seconds int64
		nanos   int32
		want    string
	}{
		{0, 0, "0s"},
		{3600, 0, "3600s"},
		{0, 1, "0.000000001s"},
		{-1, -500_000_000, "-1.5s"},
		{math.MaxInt64, 999_999_999, "9223372036854775807.999999999s"},
	} {
		if got := formatDuration(tc.seconds, tc.nanos); got != tc.want {
			t.Errorf("formatDuration(%d, %d) = %q, want %q", tc.seconds, tc.nanos, got, tc.want)
		}
	}
}

type failingText struct{}

func (failingText) MarshalText() ([]byte, error) { return nil, errors.New("no text") }

func TestCustomValueRefusals(t *testing.T) {
	for name, v := range map[string]any{
		"time past year 9999": time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC),
		"failing MarshalText": failingText{},
		"bad json.Number":     json.Number("abc"),
		"channel":             make(chan int),
		"unencodable map":     map[string]any{"f": func() {}},
	} {
		if pv, err := customValue(Logger{}, v); err == nil {
			t.Errorf("%s: got %v, want an error", name, pv)
		}
	}
}

func TestJSONNumberProperties(t *testing.T) {
	ev, _ := mustTrack(t, "my.custom", Properties{
		"int":        json.Number("42"),
		"exponent":   json.Number("1e3"),
		"wholeFloat": json.Number("1.0"),
		"fraction":   json.Number("4.5"),
		"huge":       json.Number("1e300"),
		"pastInt64":  json.Number("9223372036854775808"),
	})
	cp := ev.GetCustomProperties()
	for key, want := range map[string]*commonv1.PropertyValue{
		"int":        intValue(42),
		"exponent":   intValue(1000),
		"wholeFloat": intValue(1),
		"fraction":   doubleValue(4.5),
		"huge":       doubleValue(1e300),
		"pastInt64":  doubleValue(9223372036854775808),
	} {
		if !proto.Equal(cp[key], want) {
			t.Errorf("%s = %v, want %v", key, cp[key], want)
		}
	}
}
