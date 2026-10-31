package ingest

import (
	"bytes"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/pug-sh/sdk-go/gen/common/v1"
	eventsv1 "github.com/pug-sh/sdk-go/gen/sdk/events/v1"
)

const (
	libName  = "pug-go"
	platform = "server"

	// PropertyValue.string_value is capped at 1024 code points. A UTF-8 byte count is always
	// at least the code-point count, so a 1024-byte cap is stricter, never looser.
	maxStringBytes = 1024

	maxPointerDepth = 32
)

var (
	durationName = (&durationpb.Duration{}).ProtoReflect().Descriptor().FullName()
	timeType     = reflect.TypeFor[time.Time]()

	minOccurTime = time.Unix(0, 0)
	maxOccurTime = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

	errUnsupportedField = errors.New("unsupported field type")
)

// Properties are an event's properties, as pug.Properties hands them over.
type Properties = map[string]any

// Options are one event's settings, set by pug's TrackOptions.
type Options struct {
	Timestamp    time.Time
	HasTimestamp bool
	SessionID    string
	Location     *Location
}

// Location mirrors pug.Location.
type Location struct {
	Continent   string
	Country     string
	Region      string
	City        string
	PostalCode  string
	MetroCode   string
	Timezone    string
	Coordinates *Coordinates
}

// Coordinates mirrors pug.Coordinates.
type Coordinates struct {
	Latitude  float64
	Longitude float64
}

// Builder builds the events one client tracks.
type Builder struct {
	Log Logger
	// SessionID is the session of an event that sets none.
	SessionID string
	// Version is sent with every event as the $sdkVersion auto-property.
	Version string
}

// Build builds and validates an Event from Track's arguments. It returns nil, having logged
// why, when the event must be dropped.
func (b Builder) Build(kind, distinctID string, props Properties, o Options) *eventsv1.Event {
	log := b.Log
	custom := make(map[string]*commonv1.PropertyValue, len(props))
	source := "custom"
	if mt, ok := wellKnown().byKind[kind]; ok {
		source = "well-known"
		if !wellKnownProperties(log, kind, mt.New(), props, custom) {
			return nil
		}
	} else {
		customProperties(log, kind, props, custom)
	}
	return b.newEvent(kind, distinctID, source, custom, o)
}

// BuildWellKnown is Build for TrackWellKnown's typed properties message, whose kind the
// caller has already looked up.
func (b Builder) BuildWellKnown(kind, distinctID string, msg protoreflect.Message, extras Properties, o Options) *eventsv1.Event {
	log := b.Log
	fields := msg.Descriptor().Fields()
	hintProtoNames(log, kind, fields, extras)
	if !validProperties(log, kind, msg.Interface()) {
		return nil
	}
	custom := make(map[string]*commonv1.PropertyValue, fields.Len()+len(extras))
	knownProperties(log, msg, custom)
	rest := make(Properties, len(extras))
	for k, v := range extras {
		if fields.ByJSONName(k) != nil {
			log.Warn("extra property shares a typed field's name; skipping", "kind", kind, "property", k)
			continue
		}
		rest[k] = v
	}
	customProperties(log, kind, rest, custom)
	return b.newEvent(kind, distinctID, "well-known", custom, o)
}

func (b Builder) newEvent(kind, distinctID, source string, custom map[string]*commonv1.PropertyValue, o Options) *eventsv1.Event {
	log := b.Log
	id, err := uuid.NewV7()
	if err != nil {
		log.Error("event dropped: generating an event id failed", "kind", kind, "err", err)
		return nil
	}
	auto := map[string]*commonv1.PropertyValue{
		"$lib":        stringValue(log, libName),
		"$sdkVersion": stringValue(log, b.Version),
		"$platform":   stringValue(log, platform),
	}
	locationProperties(log, kind, o.Location, auto)
	session := b.SessionID
	if o.SessionID != "" {
		if validSessionID(o.SessionID) {
			session = o.SessionID
		} else {
			log.Warn("WithSessionID must be a UUID; using the client's session", "kind", kind)
		}
	}
	ev := &eventsv1.Event{
		EventId:          proto.String(id.String()),
		AutoProperties:   auto,
		CustomProperties: custom,
		DistinctId:       proto.String(distinctID),
		Kind:             proto.String(kind),
		OccurTime:        occurTime(log, o),
		SessionId:        proto.String(session),
	}
	if err := protovalidate.GlobalValidator.Validate(ev); err != nil {
		log.Error("event dropped: failed validation", "kind", kind, "source", source, "violations", Violations(err))
		return nil
	}
	if size := proto.Size(ev); size > maxBatchBytes {
		log.Error("event dropped: too large to send", "kind", kind, "bytes", size, "limit", maxBatchBytes)
		return nil
	}
	return ev
}

// Violations lists a protovalidate error's violations, comma-separated.
func Violations(err error) string {
	verr, ok := errors.AsType[*protovalidate.ValidationError](err)
	if !ok {
		return err.Error()
	}
	parts := make([]string, len(verr.Violations))
	for i, v := range verr.Violations {
		parts[i] = v.String()
	}
	return strings.Join(parts, ", ")
}

func validProperties(log Logger, kind string, msg proto.Message) bool {
	if err := protovalidate.GlobalValidator.Validate(msg); err != nil {
		log.Error("event dropped: properties failed validation",
			"kind", kind, "schema", msg.ProtoReflect().Descriptor().FullName(), "violations", Violations(err))
		return false
	}
	return true
}

// Typed fields render by their proto type rather than their Go value, so a whole-number double
// stays a double.
func wellKnownProperties(log Logger, kind string, msg protoreflect.Message, props Properties, out map[string]*commonv1.PropertyValue) bool {
	md := msg.Descriptor()
	fields := md.Fields()
	hintProtoNames(log, kind, fields, props)
	for i := range fields.Len() {
		fd := fields.Get(i)
		v, ok := props[fd.JSONName()]
		if !ok {
			continue
		}
		err := setField(msg, fd, v)
		switch {
		case errors.Is(err, errUnsupportedField):
			log.Warn("unsupported well-known field type; skipping", "field", fd.FullName(), "type", fd.Kind())
		case err != nil:
			log.Error("event dropped: invalid properties",
				"kind", kind, "schema", md.FullName(), "err", fd.JSONName()+": "+err.Error())
			return false
		}
	}
	if !validProperties(log, kind, msg.Interface()) {
		return false
	}
	knownProperties(log, msg, out)
	extras := make(Properties, len(props))
	for k, v := range props {
		if fields.ByJSONName(k) == nil {
			extras[k] = v
		}
	}
	customProperties(log, kind, extras, out)
	return true
}

// Fields are keyed by JSON name, as in sdk-node. A key that is a proto name is most likely a
// slip: it doesn't set the field, and goes out as an untyped custom property.
func hintProtoNames(log Logger, kind string, fields protoreflect.FieldDescriptors, props Properties) {
	for k := range props {
		if fd := fields.ByName(protoreflect.Name(k)); fd != nil && fd.JSONName() != k {
			log.Warn("property is not a field; did you mean its JSON name?", "kind", kind, "property", k, "json_name", fd.JSONName())
		}
	}
}

// The session_id rule takes only the hyphenated form; uuid.Validate also accepts braced, urn:
// and hyphenless ones.
func validSessionID(id string) bool {
	return len(id) == 36 && uuid.Validate(id) == nil
}

func knownProperties(log Logger, msg protoreflect.Message, out map[string]*commonv1.PropertyValue) {
	fields := msg.Descriptor().Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		if !msg.Has(fd) {
			continue
		}
		if pv := knownValue(log, fd, msg.Get(fd)); pv != nil {
			out[fd.JSONName()] = pv
		} else {
			log.Warn("unsupported well-known field type; skipping", "field", fd.FullName(), "type", fd.Kind())
		}
	}
}

func knownValue(log Logger, fd protoreflect.FieldDescriptor, v protoreflect.Value) *commonv1.PropertyValue {
	if fd.IsList() || fd.IsMap() {
		return nil
	}
	switch fd.Kind() {
	case protoreflect.StringKind:
		return stringValue(log, v.String())
	case protoreflect.BoolKind:
		return boolValue(v.Bool())
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		return doubleValue(v.Float())
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return intValue(v.Int())
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if u := v.Uint(); u <= math.MaxInt64 {
			return intValue(int64(u))
		}
	case protoreflect.MessageKind:
		if fd.Message().FullName() == durationName {
			d := v.Message()
			f := d.Descriptor().Fields()
			return stringValue(log, formatDuration(d.Get(f.ByName("seconds")).Int(), int32(d.Get(f.ByName("nanos")).Int())))
		}
	default:
	}
	return nil
}

// Neither the message nor protovalidate type-checks values, so a wrong type is refused here
// rather than coerced.
func setField(msg protoreflect.Message, fd protoreflect.FieldDescriptor, v any) error {
	if fd.IsList() || fd.IsMap() {
		return errUnsupportedField
	}
	if fd.Kind() == protoreflect.MessageKind {
		if fd.Message().FullName() != durationName {
			return errUnsupportedField
		}
		d, err := toDuration(v)
		if d != nil {
			msg.Set(fd, protoreflect.ValueOfMessage(d.ProtoReflect()))
		}
		return err
	}
	rv, ok := indirect(v)
	if !ok {
		return nil
	}
	val, err := scalar(fd.Kind(), rv)
	if err == nil {
		msg.Set(fd, val)
	}
	return err
}

func toDuration(v any) (*durationpb.Duration, error) {
	switch d := v.(type) {
	case nil:
		return nil, nil
	case time.Duration:
		return durationpb.New(d), nil
	case *time.Duration:
		if d == nil {
			return nil, nil
		}
		return durationpb.New(*d), nil
	case *durationpb.Duration:
		if d == nil {
			return nil, nil
		}
		return d, nil
	}
	return nil, fmt.Errorf("expected a Duration (time.Duration or *durationpb.Duration), got %T", v)
}

func scalar(kind protoreflect.Kind, rv reflect.Value) (protoreflect.Value, error) {
	_, isNumber := reflect.TypeAssert[json.Number](rv)
	switch kind {
	case protoreflect.StringKind:
		if rv.Kind() == reflect.String && !isNumber {
			return protoreflect.ValueOfString(rv.String()), nil
		}
	case protoreflect.BoolKind:
		if rv.Kind() == reflect.Bool {
			return protoreflect.ValueOfBool(rv.Bool()), nil
		}
	case protoreflect.DoubleKind:
		if f, ok := asFloat(rv); ok {
			return protoreflect.ValueOfFloat64(f), nil
		}
	case protoreflect.FloatKind:
		if f, ok := asFloat(rv); ok {
			return protoreflect.ValueOfFloat32(float32(f)), nil
		}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		n, err := integerIn(kind, rv, minInt32, maxInt32)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt32(int32(n.Int64())), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		n, err := integerIn(kind, rv, minInt64, maxInt64)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt64(n.Int64()), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		n, err := integerIn(kind, rv, zero, maxUint32)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint32(uint32(n.Uint64())), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		n, err := integerIn(kind, rv, zero, maxUint64)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint64(n.Uint64()), nil
	default:
		return protoreflect.Value{}, errUnsupportedField
	}
	return protoreflect.Value{}, fmt.Errorf("expected %s, got %s", kind, rv.Type())
}

func asFloat(rv reflect.Value) (float64, bool) {
	if n, ok := reflect.TypeAssert[json.Number](rv); ok {
		f, err := n.Float64()
		return f, err == nil
	}
	switch {
	case rv.CanFloat():
		return rv.Float(), true
	case rv.CanInt():
		return float64(rv.Int()), true
	case rv.CanUint():
		return float64(rv.Uint()), true
	}
	return 0, false
}

var (
	zero      = new(big.Int)
	minInt32  = big.NewInt(math.MinInt32)
	maxInt32  = big.NewInt(math.MaxInt32)
	minInt64  = big.NewInt(math.MinInt64)
	maxInt64  = big.NewInt(math.MaxInt64)
	maxUint32 = big.NewInt(math.MaxUint32)
	maxUint64 = new(big.Int).SetUint64(math.MaxUint64)
)

func integerIn(kind protoreflect.Kind, rv reflect.Value, lo, hi *big.Int) (*big.Int, error) {
	n, err := wholeNumber(kind, rv)
	if err != nil {
		return nil, err
	}
	if n.Cmp(lo) < 0 || n.Cmp(hi) > 0 {
		return nil, fmt.Errorf("%s overflows %s", n, kind)
	}
	return n, nil
}

func wholeNumber(kind protoreflect.Kind, rv reflect.Value) (*big.Int, error) {
	if num, ok := reflect.TypeAssert[json.Number](rv); ok {
		if n, ok := new(big.Int).SetString(num.String(), 10); ok {
			return n, nil
		}
		f, err := num.Float64()
		if err != nil {
			return nil, fmt.Errorf("expected %s, got %q", kind, num.String())
		}
		return wholeFloat(kind, f)
	}
	switch {
	case rv.CanInt():
		return big.NewInt(rv.Int()), nil
	case rv.CanUint():
		return new(big.Int).SetUint64(rv.Uint()), nil
	case rv.CanFloat():
		return wholeFloat(kind, rv.Float())
	}
	return nil, fmt.Errorf("expected %s, got %s", kind, rv.Type())
}

func wholeFloat(kind protoreflect.Kind, f float64) (*big.Int, error) {
	if f != math.Trunc(f) || math.IsInf(f, 0) {
		return nil, fmt.Errorf("expected %s, got %v", kind, f)
	}
	// Past 2^53 a float may already have been rounded, so it can't stand for an exact integer.
	if math.Abs(f) >= 1<<53 {
		return nil, fmt.Errorf("expected a safe integer, got %s", strconv.FormatFloat(f, 'f', -1, 64))
	}
	return big.NewInt(int64(f)), nil
}

func customProperties(log Logger, kind string, props Properties, out map[string]*commonv1.PropertyValue) {
	for k, v := range props {
		pv, err := customValue(log, v)
		if err != nil {
			log.Warn("property not representable; skipping", "kind", kind, "property", k, "type", fmt.Sprintf("%T", v), "err", err)
			continue
		}
		if pv != nil {
			out[strings.ToValidUTF8(k, "\uFFFD")] = pv
		}
	}
}

// A nil result with a nil error means the value is absent and the property is omitted.
func customValue(log Logger, v any) (*commonv1.PropertyValue, error) {
	switch v := v.(type) {
	case nil:
		return nil, nil
	case string:
		return stringValue(log, v), nil
	case bool:
		return boolValue(v), nil
	case time.Time:
		return timeValue(v)
	case *time.Time:
		if v == nil {
			return nil, nil
		}
		return timeValue(*v)
	case time.Duration:
		return stringValue(log, FormatDuration(v)), nil
	case json.Number:
		return numberValue(v)
	case *timestamppb.Timestamp:
		if v == nil {
			return nil, nil
		}
		return timeValue(v.AsTime())
	case *durationpb.Duration:
		if v == nil {
			return nil, nil
		}
		return stringValue(log, formatDuration(v.GetSeconds(), v.GetNanos())), nil
	}
	rv, ok := indirect(v)
	if !ok {
		return nil, nil
	}
	if tm, ok := v.(encoding.TextMarshaler); ok {
		return textValue(log, tm)
	}
	if rv.Type() != reflect.TypeOf(v) {
		switch rv.Kind() {
		case reflect.Map, reflect.Slice, reflect.Array, reflect.Struct:
			if rv.Type() != timeType {
				return jsonValue(log, v)
			}
		default:
		}
		return customValue(log, rv.Interface())
	}
	switch rv.Kind() {
	case reflect.String:
		return stringValue(log, rv.String()), nil
	case reflect.Bool:
		return boolValue(rv.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intValue(rv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if u := rv.Uint(); u <= math.MaxInt64 {
			return intValue(int64(u)), nil
		}
		return nil, fmt.Errorf("%d overflows int64", rv.Uint())
	case reflect.Float32, reflect.Float64:
		return floatValue(rv.Float())
	case reflect.Map, reflect.Slice:
		if rv.IsNil() {
			return nil, nil
		}
		return jsonValue(log, v)
	case reflect.Array, reflect.Struct:
		return jsonValue(log, v)
	default:
		return nil, fmt.Errorf("unsupported type %T", v)
	}
}

func indirect(v any) (reflect.Value, bool) {
	rv := reflect.ValueOf(v)
	for range maxPointerDepth {
		switch rv.Kind() {
		case reflect.Invalid:
			return rv, false
		case reflect.Pointer, reflect.Interface:
			if rv.IsNil() {
				return rv, false
			}
			rv = rv.Elem()
		default:
			return rv, true
		}
	}
	return rv, false
}

// A JSON number is untyped, so "1e3" and "1.0" are ints here, as they are to sdk-node.
func numberValue(n json.Number) (*commonv1.PropertyValue, error) {
	if i, err := n.Int64(); err == nil {
		return intValue(i), nil
	}
	f, err := n.Float64()
	if err != nil {
		return nil, err
	}
	if f == math.Trunc(f) && math.Abs(f) < 1<<53 {
		return intValue(int64(f)), nil
	}
	return floatValue(f)
}

func floatValue(f float64) (*commonv1.PropertyValue, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, fmt.Errorf("non-finite number %v", f)
	}
	return doubleValue(f), nil
}

func timeValue(t time.Time) (*commonv1.PropertyValue, error) {
	ts := timestamppb.New(t)
	if err := ts.CheckValid(); err != nil {
		return nil, err
	}
	return &commonv1.PropertyValue{Value: &commonv1.PropertyValue_TimestampValue{TimestampValue: ts}}, nil
}

func textValue(log Logger, tm encoding.TextMarshaler) (pv *commonv1.PropertyValue, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("MarshalText panicked: %v", r)
		}
	}()
	b, err := tm.MarshalText()
	if err != nil {
		return nil, err
	}
	return stringValue(log, string(b)), nil
}

// HTML escaping is off so the text matches what the other SDKs send.
func jsonValue(log Logger, v any) (pv *commonv1.PropertyValue, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("JSON encoding panicked: %v", r)
		}
	}()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	b := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	if string(b) == "null" {
		return nil, nil
	}
	if b[0] == '"' {
		var s string
		if json.Unmarshal(b, &s) == nil {
			return stringValue(log, s), nil
		}
	}
	return stringValue(log, string(b)), nil
}

// Invalid UTF-8 is repaired because protobuf-go refuses to marshal it, which would fail the
// whole batch.
func stringValue(log Logger, s string) *commonv1.PropertyValue {
	s = strings.ToValidUTF8(s, "\uFFFD")
	if len(s) > maxStringBytes {
		log.Warn("property string exceeds 1024 bytes; truncating", "bytes", len(s))
		n := maxStringBytes
		for n > 0 && !utf8.RuneStart(s[n]) {
			n--
		}
		s = s[:n]
	}
	return &commonv1.PropertyValue{Value: &commonv1.PropertyValue_StringValue{StringValue: s}}
}

func intValue(n int64) *commonv1.PropertyValue {
	return &commonv1.PropertyValue{Value: &commonv1.PropertyValue_IntValue{IntValue: n}}
}

func doubleValue(f float64) *commonv1.PropertyValue {
	return &commonv1.PropertyValue{Value: &commonv1.PropertyValue_DoubleValue{DoubleValue: f}}
}

func boolValue(b bool) *commonv1.PropertyValue {
	return &commonv1.PropertyValue{Value: &commonv1.PropertyValue_BoolValue{BoolValue: b}}
}

// FormatDuration renders d in options.proto's wire form for a Duration, such as "1.5s".
func FormatDuration(d time.Duration) string {
	return formatDuration(int64(d/time.Second), int32(d%time.Second))
}

// formatDuration renders options.proto's wire form for a Duration, "1.5s", rather than
// protojson's "1.500s". Summing first fixes nanos that are out of range or signed against the
// seconds.
func formatDuration(seconds int64, nanos int32) string {
	billion := big.NewInt(1e9)
	total := new(big.Int).Mul(big.NewInt(seconds), billion)
	total.Add(total, big.NewInt(int64(nanos)))
	sign := ""
	if total.Sign() < 0 {
		sign = "-"
		total.Neg(total)
	}
	whole, frac := new(big.Int).QuoRem(total, billion, new(big.Int))
	fraction := strings.TrimRight(fmt.Sprintf("%09d", frac.Int64()), "0")
	if fraction != "" {
		fraction = "." + fraction
	}
	return sign + whole.String() + fraction + "s"
}

func occurTime(log Logger, o Options) *timestamppb.Timestamp {
	if !o.HasTimestamp {
		return timestamppb.Now()
	}
	if o.Timestamp.Before(minOccurTime) || o.Timestamp.After(maxOccurTime) {
		log.Warn("ignoring out-of-range track timestamp; using the current time", "timestamp", o.Timestamp)
		return timestamppb.Now()
	}
	return timestamppb.New(o.Timestamp)
}

// locationProperties renders a Location as geo auto-properties. The server skips geo
// enrichment on private-key requests, which is all this SDK makes, so a dropped field is a
// permanent hole rather than one filled in later.
func locationProperties(log Logger, kind string, loc *Location, out map[string]*commonv1.PropertyValue) {
	if loc == nil {
		return
	}
	added := 0
	set := func(key, value string) {
		if v := strings.TrimSpace(value); v != "" {
			out[key] = stringValue(log, v)
			added++
		}
	}
	set("$continent", loc.Continent)
	if country := strings.ToUpper(strings.TrimSpace(loc.Country)); country != "" {
		if isCountryCode(country) {
			set("$country", country)
		} else {
			log.Warn(`ignoring location country; expected an ISO 3166-1 alpha-2 code like "DE"`, "kind", kind, "country", loc.Country)
		}
	}
	set("$region", loc.Region)
	set("$city", loc.City)
	set("$postalCode", loc.PostalCode)
	set("$metroCode", loc.MetroCode)
	set("$timezone", loc.Timezone)
	if c := loc.Coordinates; c != nil {
		// Always doubles: a whole-number coordinate in the int slot would split the column
		// every other writer of $latitude/$longitude files as Float64.
		latOK := checkCoordinate(log, kind, "latitude", c.Latitude, 90)
		lngOK := checkCoordinate(log, kind, "longitude", c.Longitude, 180)
		if latOK && lngOK {
			out["$latitude"] = doubleValue(c.Latitude)
			out["$longitude"] = doubleValue(c.Longitude)
			added += 2
		}
	}
	if added == 0 {
		log.Warn("ignoring location: no usable fields; the event will carry no geo", "kind", kind)
	}
}

// A lone coordinate reads as a real position with the other axis at 0, so one bad axis costs
// the pair.
func checkCoordinate(log Logger, kind, axis string, v, limit float64) bool {
	if math.IsNaN(v) || math.Abs(v) > limit {
		log.Warn("ignoring location coordinates: "+axis+" must be within ±"+strconv.Itoa(int(limit)),
			"kind", kind, axis, v)
		return false
	}
	return true
}
