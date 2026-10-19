// Package pug is the Pug server-side SDK for Go: event tracking, profile identify, and
// analytics reads.
//
// A [Client] authenticates with a private project API key (prv_…). Ingestion never fails:
// [Client.Track] and [Client.TrackWellKnown] validate their input, log and drop anything
// invalid, and hand events to a background sender that batches them, so a tracking call
// can't take down a request path. [Client.Identify] logs its failures too.
//
// Reads ([Client.Profiles], [Client.Activity] and [Client.Insights]) run in the caller's
// control flow: they return errors of type [*Error] and are never retried. [errors.Is] matches
// an [*Error] to the sentinel for its status code, such as [ErrNotFound].
//
// Call [Client.Close] on shutdown so buffered events are not lost.
package pug
