package pug

import (
	"time"

	"google.golang.org/protobuf/proto"
)

// Properties are an event's custom properties. Strings, booleans, integers and floats keep
// their type; time.Time becomes a timestamp and time.Duration a duration string like "1.5s";
// json.Number is an integer when it has no fraction; a value implementing
// encoding.TextMarshaler is sent as its text; other maps, slices and structs are sent as JSON
// strings. Nil values are omitted, and a value that can't be represented (a func, NaN, a
// uint64 past the int64 range) is dropped with a warning.
type Properties map[string]any

// Traits are profile properties for [Client.Identify], shallow-merged into the stored ones.
// They must be JSON-serializable.
type Traits map[string]any

// TrackOption customizes one tracked event.
type TrackOption func(*trackOptions)

type trackOptions struct {
	timestamp    time.Time
	hasTimestamp bool
	sessionID    string
	location     *Location
}

func trackOpts(opts []TrackOption) trackOptions {
	var o trackOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// WithTimestamp sets when the event happened; it defaults to now. A time before the Unix
// epoch or after year 9999 is almost certainly a unit mistake, so it is ignored with a
// warning.
func WithTimestamp(t time.Time) TrackOption {
	return func(o *trackOptions) { o.timestamp, o.hasTimestamp = t, true }
}

// WithSessionID overrides the client's per-instance session id. It must be a UUID; an empty
// id keeps the default.
func WithSessionID(id string) TrackOption {
	return func(o *trackOptions) { o.sessionID = id }
}

// WithLocation records the visitor's location. Pug derives geo only from browser requests, so
// an event sent with a private key carries the location set here or none at all.
func WithLocation(loc Location) TrackOption {
	return func(o *trackOptions) { o.location = &loc }
}

// Location is where the visitor is, for an event your backend sends on their behalf. Strings
// are trimmed and empty ones omitted. A field that looks like a mistake (an unknown country,
// a coordinate out of range) is dropped with a warning and the rest is still sent.
type Location struct {
	Continent string
	// Country is an ISO 3166-1 alpha-2 code such as "DE". Case and surrounding spaces are
	// normalized.
	Country    string
	Region     string
	City       string
	PostalCode string
	MetroCode  string
	// Timezone is an IANA name such as "Europe/Berlin".
	Timezone    string
	Coordinates *Coordinates
}

// Coordinates are a position in decimal degrees. Latitude and longitude are sent together or
// not at all.
type Coordinates struct {
	Latitude  float64
	Longitude float64
}

// IdentifyOption customizes a [Client.Identify] call.
type IdentifyOption func(*identifyOptions)

type identifyOptions struct {
	anonymousID string
	deviceID    string
}

// WithAnonymousID names the SDK anonymous id (it must start with "anon-") whose profile is
// merged into the identified one.
func WithAnonymousID(id string) IdentifyOption {
	return func(o *identifyOptions) { o.anonymousID = id }
}

// WithDeviceID assigns a device to the profile.
func WithDeviceID(id string) IdentifyOption {
	return func(o *identifyOptions) { o.deviceID = id }
}

// GetOption customizes a single-profile lookup.
type GetOption func(*getOptions)

type getOptions struct {
	includeBots *bool
}

// IncludeBots counts traffic tagged as a bot at ingest in the profile's activity summary. It
// is left out by default.
func IncludeBots() GetOption {
	return func(o *getOptions) { o.includeBots = proto.Bool(true) }
}
