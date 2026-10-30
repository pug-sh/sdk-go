package pug

import "time"

// Field is one event property or profile trait, made by String, Int, Float, Bool, Time,
// Duration or Any. It goes in the same list as the call's other options, so the key sits next
// to its value and the value's type is fixed at compile time.
type Field struct {
	key   string
	value any
}

func (f Field) applyTrack(o *trackOptions) { o.props = merge(o.props, Properties{f.key: f.value}) }

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
