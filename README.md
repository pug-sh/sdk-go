<p align="center">
  <img src="https://app.pug.sh/logo.svg" alt="Pug" width="120" height="120" />
</p>

# github.com/pug-sh/sdk-go

Pug server-side SDK for Go: event tracking, profile identify, and analytics reads.

It authenticates with a **private** project API key (`prv_…`). Ingestion never fails, so a
tracking call can't take down your request path.

## Install

```bash
go get github.com/pug-sh/sdk-go
```

Requires Go 1.26 or newer.

## Quick start

```go
import (
	pug "github.com/pug-sh/sdk-go"
	commoneventsv1 "github.com/pug-sh/sdk-go/gen/common/events/v1"
	insightsv1 "github.com/pug-sh/sdk-go/gen/shared/insights/v1"
	"google.golang.org/protobuf/proto"
)

client, err := pug.New(pug.Options{
	APIKey:   os.Getenv("PUG_API_KEY"),
	Endpoint: "https://pug.example.com",
})
if err != nil {
	log.Fatal(err)
}
ctx := context.Background()

// Track an event. distinctID (who the event is for) comes first: a server has no ambient user.
// Properties are typed fields, in the same list as the options.
client.Track("user_42", "order.completed", pug.Float("amount", 49.0), pug.String("currency", "USD"))

// Well-known events get their properties validated against the event's schema...
client.Track("user_42", "feature_used", pug.String("featureId", "export"))
// ...or take the typed schema itself, so the fields are checked at compile time.
client.TrackWellKnown("user_42", &commoneventsv1.FeatureUsedProperties{FeatureId: proto.String("export")})

// Identify a profile. Like Track, it returns at once and never fails; identifies are queued and
// sent in batches.
client.Identify("user_42", pug.Email("ada@example.com"), pug.String("plan", "pro"))

// Reads return *pug.Error, which errors.Is matches to a sentinel per status code.
profile, err := client.Profiles.GetByExternalID(ctx, "user_42")
if errors.Is(err, pug.ErrNotFound) {
	// ...
}
for p, err := range client.Profiles.List(ctx, nil) {
	if err != nil {
		break
	}
	fmt.Println(p.GetExternalId())
}
trend, err := client.Insights.Query(ctx, &insightsv1.QueryRequest{ /* spec, time range, granularity */ })

// Drain on shutdown so no queued events or identifies are lost.
err = client.Close(ctx)
```

## Behaviour

- **Ingestion never fails.** `Track`, `TrackWellKnown` and `Identify` validate input client-side,
  log and drop anything invalid, and return nothing. They are non-blocking: background
  goroutines send events and identifies in batches by size (`MaxSize`) or interval (`MaxWait`).
  Identifies go out with `BatchIdentify`; a server that predates it gets one `Identify` call each.
- **Delivery is best-effort with retry.** A transient send failure keeps events queued and
  retries after `MaxWait`; a permanent one (a fixed set of client-error codes, plus any
  non-transport error) dead-letters them via `OnError`. `Close` drains and reports anything still
  undelivered through `OnError`, which covers both events and identifies.
- **Reads are request/response and return errors** (`*pug.Error`): they run in your control
  flow, so you own retry. They are not auto-retried. Your `ctx` deadline bounds them; without
  one, `Timeout` (10s) does.
- **Well-known events** have typed, validated properties; any other `kind` is accepted as a
  custom event with loose properties.
- Warnings and errors go to `log/slog` (`slog.Default()` unless you pass a `Logger`).

## API

### `pug.New(pug.Options)`

| Option       | Type              | Default            | Description                                                                                             |
| ------------ | ----------------- | ------------------ | ------------------------------------------------------------------------------------------------------- |
| `APIKey`     | `string`          | none               | **Required.** Private project key (`prv_…`).                                                            |
| `Endpoint`   | `string`          | hosted endpoint    | Pug server origin.                                                                                      |
| `Batch`      | `pug.BatchConfig` | see below          | Ingestion buffers, one each for events and identifies: `MaxSize` (100, capped at 1000), `MaxWait` (5s), `MaxQueueSize` (10,000). |
| `OnError`    | `pug.OnError`     | none               | Dead-letter hook for queued events and identifies that couldn't be delivered.                          |
| `Logger`     | `*slog.Logger`    | `slog.Default()`   | Receives the SDK's warnings and errors.                                                                 |
| `HTTPClient` | `*http.Client`    | default transport  | Sends the requests.                                                                                     |
| `Timeout`    | `time.Duration`   | 10s                | Bounds each request whose context has no deadline.                                                      |

```go
pug.New(pug.Options{
	APIKey: apiKey, // required, prv_…
	Batch:  pug.BatchConfig{MaxSize: 100, MaxWait: 5 * time.Second, MaxQueueSize: 10_000},
	OnError: func(err error, lost pug.Undelivered) {
		// dead-letter sink for lost.Events and lost.Identifies; err matches pug.ErrQueueOverflow
		// or pug.ErrClosed, or is a *pug.Error for a failed send, e.g. errors.Is(err, pug.ErrUnauthenticated)
	},
})
```

`OnError` may run on the goroutine that called `Track` or `Identify` or on a background sender,
so it must be safe for concurrent use, return promptly, and not call `Flush` or `Close`.

### Ingestion (never fails)

- `client.Track(distinctID, kind, opts...)`: enqueue an event; `opts` holds its property
  fields, `Properties` maps and settings. Non-blocking.
- `client.TrackWellKnown(distinctID, event, opts...)`: enqueue a well-known event from its typed
  message (e.g. `&commoneventsv1.PurchaseProperties{...}`); the kind comes from the schema, and
  fields or `Properties` in `opts` ride along as custom properties.
- `client.Identify(externalID, opts...)`: queue a create-or-update of a profile; `opts` holds its
  trait fields, named traits, `Traits` maps and settings. Non-blocking.
- `client.Flush(ctx)`: send the queued events and identifies now.
- `client.Close(ctx)`: drain and shut down; call on graceful exit so nothing queued is lost.
  Events and identifies still undelivered when `ctx` ends are reported to `OnError`.

Properties and traits are typed fields, passed in the same list as the call's options:

| Field                                     | Event property                     | Trait              |
| ----------------------------------------- | ---------------------------------- | ------------------ |
| `pug.String(key, v)`                      | string                             | string             |
| `pug.Int(key, v)` (any integer type)      | int                                | number             |
| `pug.Float(key, v)`                       | double                             | number             |
| `pug.Bool(key, v)`                        | bool                               | bool               |
| `pug.Time(key, v)`                        | timestamp                          | RFC 3339 string    |
| `pug.Duration(key, v)`                    | duration string, e.g. `"1.5s"`     | the same string    |
| `pug.Any(key, v)`                         | mapped by Go type (table below)    | JSON               |

When keys are only known at run time, pass a `pug.Properties` map to `Track` or a `pug.Traits` map
to `Identify`; maps and fields mix freely, and a key set twice in one call takes the later value.

The kind is a `pug.Kind`. A string literal converts on its own, and a kind held in a variable
needs `pug.Kind(name)`, so a distinct ID passed where the kind belongs fails to compile.

`Identify` also takes named traits, which write the keys Pug's dashboard reads. They only compile
in `Identify`, and an empty value is omitted:

| Named trait          | Key         |
| -------------------- | ----------- |
| `pug.Name(v)`        | `name`      |
| `pug.FirstName(v)`   | `firstName` |
| `pug.LastName(v)`    | `lastName`  |
| `pug.Email(v)`       | `email`     |
| `pug.Avatar(v)`      | `avatar` (the dashboard shows only an absolute https URL) |

A NaN or ±Inf trait is dropped with a warning, and the rest of the identify still goes out.

`Track` options: `pug.WithTimestamp(t)`, `pug.WithSessionID(id)` (overrides the per-client
session; it must be a UUID, or it's ignored with a warning), `pug.WithLocation(loc)` (the
visitor's location, see below).
`Identify` options: `pug.WithAnonymousID(id)` (must start with `anon-`; triggers anon→identified
merge), `pug.WithDeviceID(id)`.

All options, fields and maps included, are optional and can go in any order. A `map[string]any`
needs converting, as `pug.Properties(m)`. A wrapper that takes its own props and
`opts ...pug.TrackOption` forwards them as `append([]pug.TrackOption{props}, opts...)...`.

For a well-known kind, `Track`'s properties are keyed by the schema fields' JSON names
(`productId`, not `product_id`) and must match their types; a field of the wrong type drops the
event with an error. A key that is a field's proto name (`product_id`) doesn't set the field: it
is sent as a custom property with a warning naming the JSON name, and if the field is required
the event is dropped. `pug.Any` values and `pug.Properties` map by Go type:

| Go value                                    | Sent as                                  |
| ------------------------------------------- | ---------------------------------------- |
| `string`, `bool`                            | string, bool                             |
| integers                                    | int (a `uint64` past `MaxInt64` is dropped) |
| `float32`, `float64`                        | double (NaN and ±Inf are dropped)        |
| `json.Number`                               | int when it has no fraction, else double |
| `time.Time`, `*timestamppb.Timestamp`       | timestamp                                |
| `time.Duration`, `*durationpb.Duration`     | duration string, e.g. `"1.5s"`           |
| `encoding.TextMarshaler`                    | its text                                 |
| other maps, slices, structs                 | JSON string                              |
| `nil`                                       | omitted                                  |

Strings are capped at 1024 bytes (truncated with a warning). Decode JSON request bodies with
`json.Decoder.UseNumber` to keep whole numbers ints.

Pug only derives geo from CDN headers on browser (public-key) requests. This SDK uses a private
key, so the server adds no geo of its own. Without a location, an event has no geo at all:

```go
client.Track("user-123", "checkout_completed", pug.String("plan", "pro"),
	pug.WithLocation(pug.Location{Country: "DE", City: "Berlin"}),
)
```

`Location` fields: `Continent`, `Country`, `Region`, `City`, `PostalCode`, `MetroCode`,
`Timezone`, and `Coordinates` (a latitude/longitude pair). `Country` must be an ISO 3166-1
alpha-2 code (case and surrounding spaces are normalized). Empty strings are omitted silently.
A field that looks like a mistake (an unknown country, a coordinate out of range) is dropped
with a warning and the rest is still sent. If nothing is usable the event goes out with no geo.

### Reads (return `*pug.Error`)

These run in your control flow:

- `client.Profiles`: `Get`, `GetByExternalID`, `Delete`, `List` (an auto-paginating
  `iter.Seq2`).
- `client.Activity`: `Feed`, `EventExplorer`, `Heatmap`, `ProfileSessions`, `ProfileStats`,
  `FilterSchema`, `PropertyValues`.
- `client.Insights`: `Query`, `SegmentUsers`, `FilterSchema`, `PropertyValues`.

Requests and responses are the generated protobuf types under `gen/`
(`github.com/pug-sh/sdk-go/gen/shared/insights/v1`, `.../shared/activity/v1`,
`.../shared/profiles/v1`, `.../common/v1`).

Reads leave out traffic tagged as a bot at ingest unless you ask for it: pass
`pug.IncludeBots()` to `Profiles.Get` and `GetByExternalID`, or set `IncludeBots` on the request.

Errors normalize to `*pug.Error`, which carries the underlying Connect `Code` and wraps the cause.
`errors.Is` matches it to the sentinel for its code (`pug.ErrNotFound`, `pug.ErrUnauthenticated`,
`pug.ErrUnavailable` and so on, one per Connect code):

```go
if errors.Is(err, pug.ErrNotFound) {
	// ...
}
var perr *pug.Error
if errors.As(err, &perr) {
	log.Println(perr.Code, perr.Message)
}
```

## Contributing

See [`docs/development.md`](./docs/development.md) for building, testing, and how the generated
code and the well-known event catalog are maintained.

## License

MIT
