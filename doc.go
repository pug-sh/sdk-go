// Package pug is the Pug server-side SDK for Go: event tracking, profile identify, and
// analytics reads.
//
// A [Client] authenticates with a private project API key (prv_…). Ingestion never fails:
// [Client.Track], [Client.TrackWellKnown] and [Client.Identify] validate their input, log and
// drop anything invalid, and hand events and identifies to background senders that batch
// them, so a tracking call can't take down a request path. Properties and traits are typed
// fields such as [String] and [Int], passed in the same list as the call's options. Anything
// that can't be delivered is reported to [Options.OnError].
//
// Reads ([Client.Profiles], [Client.Activity] and [Client.Insights]) run in the caller's
// control flow: they return errors of type [*Error] and are never retried. [errors.Is] matches
// an [*Error] to the sentinel for its status code, such as [ErrNotFound].
//
// Call [Client.Close] on shutdown so queued events and identifies are not lost.
package pug
