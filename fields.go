package pug

import (
	"time"

	"github.com/pug-sh/sdk-go/internal/ingest"
)

// Field is one event property or profile trait, made by String, Int, Float, Bool, Time,
// Duration or Any. It goes in the same list as the call's other options, so the key sits next
// to its value and the value's type is fixed at compile time.
type Field struct {
	key   string
	value any
}

func (f Field) applyTrack(o *trackOptions) { o.props = merge(o.props, Properties{f.key: f.value}) }

func (f Field) applyIdentify(o *identifyOptions) {
	// encoding/json would write a Duration as nanoseconds; a trait gets the same "1.5s" an
	// event property does.
	if d, ok := f.value.(time.Duration); ok {
		o.traits = merge(o.traits, Traits{f.key: ingest.FormatDuration(d)})
		return
	}
	o.traits = merge(o.traits, Traits{f.key: f.value})
}

// namedTrait is a trait the dashboard reads under a fixed key. It implements only
// IdentifyOption, so passing one to Track fails to compile. An empty value is omitted, so a
// blank column can't erase a stored trait.
type namedTrait struct{ key, value string }

func (t namedTrait) applyIdentify(o *identifyOptions) {
	if t.value != "" {
		o.traits = merge(o.traits, Traits{t.key: t.value})
	}
}

// Name is the profile's display name, sent as the "name" trait.
func Name(value string) IdentifyOption { return namedTrait{"name", value} }

// FirstName is the profile's first name, sent as the "firstName" trait. With LastName, the
// dashboard composes a display name when Name is unset.
func FirstName(value string) IdentifyOption { return namedTrait{"firstName", value} }

// LastName is the profile's last name, sent as the "lastName" trait.
func LastName(value string) IdentifyOption { return namedTrait{"lastName", value} }

// Email is the profile's email address, sent as the "email" trait.
func Email(value string) IdentifyOption { return namedTrait{"email", value} }

// Avatar is the URL of the profile's picture, sent as the "avatar" trait. The dashboard shows
// only an absolute https URL.
func Avatar(value string) IdentifyOption { return namedTrait{"avatar", value} }

// integer is every Go integer type Int accepts.
type integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

// String is a string property. One over 1024 bytes is truncated with a warning.
func String(key, value string) Field { return Field{key, value} }

// Int is an integer property of any integer type. One past the int64 range is dropped with a
// warning.
func Int[T integer](key string, value T) Field {
	// Widening to int64 or uint64 keeps every value exact and drops any methods of a named
	// type, so a type with a MarshalText method is still sent as a number.
	if value < 0 {
		return Field{key, int64(value)}
	}
	return Field{key, uint64(value)}
}

// Float is a floating-point property, sent as a double even when it is a whole number. NaN and
// ±Inf are dropped with a warning.
func Float(key string, value float64) Field { return Field{key, value} }

// Bool is a boolean property.
func Bool(key string, value bool) Field { return Field{key, value} }

// Time is a timestamp property.
func Time(key string, value time.Time) Field { return Field{key, value} }

// Duration is a duration property, sent as a string such as "1.5s".
func Duration(key string, value time.Duration) Field { return Field{key, value} }

// Any is a property of any other type, mapped by its Go type as a Properties value is.
func Any(key string, value any) Field { return Field{key, value} }
