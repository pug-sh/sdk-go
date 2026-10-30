package pug

import (
	"maps"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/pug-sh/sdk-go/internal/ingest"
)

// Properties are an event's custom properties as a map, for keys only known at run time,
// passed to [Client.Track] or [Client.TrackWellKnown] among its options like a [Field]; given
// more than once, they are merged, later keys winning. Strings, booleans, integers and floats keep their type; time.Time becomes a
// timestamp and time.Duration a duration string like "1.5s"; json.Number is an integer when it
// has no fraction; a value implementing encoding.TextMarshaler is sent as its text; other maps,
// slices and structs are sent as JSON strings. Nil values are omitted, and a value that can't
// be represented (a func, NaN, a uint64 past the int64 range) is dropped with a warning.
type Properties map[string]any

func (p Properties) applyTrack(o *trackOptions) { o.props = merge(o.props, p) }

// Traits are profile properties, passed to [Client.Identify] among its options and
// shallow-merged into the stored ones; given more than once, they are merged, later keys
// winning. They must be JSON-serializable.
type Traits map[string]any

func (t Traits) applyIdentify(o *identifyOptions) { o.traits = merge(o.traits, t) }

// The caller's maps are only read, never written, so a lone map is used as is.
func merge[M ~map[string]any](dst, src M) M {
	switch {
	case src == nil:
		return dst
	case dst == nil:
		return src
	}
	out := maps.Clone(dst)
	maps.Copy(out, src)
	return out
}

// TrackOption customizes one tracked event: a [Field], its [Properties], or an option such
// as [WithTimestamp]. Nil options are skipped.
type TrackOption interface{ applyTrack(o *trackOptions) }

type trackOption func(*trackOptions)

func (f trackOption) applyTrack(o *trackOptions) { f(o) }

type trackOptions struct {
	ingest.Options
	// props holds the Properties given among the options, merged in call order.
	props Properties
}

func trackOpts(opts []TrackOption) trackOptions {
	var o trackOptions
	for _, opt := range opts {
		// A nil *Properties is a TrackOption through Properties' method, but calling it panics.
		if p, ok := opt.(*Properties); opt == nil || ok && p == nil {
			continue
		}
		opt.applyTrack(&o)
	}
	return o
}

// WithTimestamp sets when the event happened; it defaults to now. A time before the Unix
// epoch or after year 9999 is almost certainly a unit mistake, so it is ignored with a
// warning.
func WithTimestamp(t time.Time) TrackOption {
	return trackOption(func(o *trackOptions) { o.Timestamp, o.HasTimestamp = t, true })
}

// WithSessionID overrides the client's per-instance session id. It must be a UUID; an empty
// id keeps the default, and any other id is ignored with a warning.
func WithSessionID(id string) TrackOption {
	return trackOption(func(o *trackOptions) { o.SessionID = id })
}

// WithLocation records the visitor's location. Pug derives geo only from browser requests, so
// an event sent with a private key carries the location set here or none at all.
func WithLocation(loc Location) TrackOption {
	// Copied when applied, so Coordinates is still read during Track, as it always was.
	return trackOption(func(o *trackOptions) {
		o.Location = &ingest.Location{
			Continent: loc.Continent, Country: loc.Country, Region: loc.Region, City: loc.City,
			PostalCode: loc.PostalCode, MetroCode: loc.MetroCode, Timezone: loc.Timezone,
		}
		if c := loc.Coordinates; c != nil {
			o.Location.Coordinates = &ingest.Coordinates{Latitude: c.Latitude, Longitude: c.Longitude}
		}
	})
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

// IdentifyOption customizes a [Client.Identify] call: the profile's [Traits], or an option
// such as [WithAnonymousID].
type IdentifyOption interface{ applyIdentify(o *identifyOptions) }

type identifyOption func(*identifyOptions)

func (f identifyOption) applyIdentify(o *identifyOptions) { f(o) }

type identifyOptions struct {
	traits      Traits
	anonymousID string
	deviceID    string
}

// WithAnonymousID names the SDK anonymous id (it must start with "anon-") whose profile is
// merged into the identified one.
func WithAnonymousID(id string) IdentifyOption {
	return identifyOption(func(o *identifyOptions) { o.anonymousID = id })
}

// WithDeviceID assigns a device to the profile.
func WithDeviceID(id string) IdentifyOption {
	return identifyOption(func(o *identifyOptions) { o.deviceID = id })
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
