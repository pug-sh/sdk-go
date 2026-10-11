# Development

Internal guide for working on `sdk-go`. End-user docs live in the [README](../README.md).

## Toolchain

- **[Go](https://go.dev/dl/)** 1.26 or newer: builds, tests, and runs the protobuf generators
  (`protoc-gen-go` and `protoc-gen-connect-go` are `tool` dependencies in `go.mod`, run through
  `go tool`).
- **[golangci-lint](https://golangci-lint.run)** v2: lint and import formatting (see
  `.golangci.yml`). It must be built with a Go at least as new as the one whose standard
  library it loads.
- **[buf](https://buf.build/docs/installation)**: re-vendor and regenerate the protobuf code.
  Only needed when updating protos.

## Protobuf types (vendored)

Protobuf types are **vendored**, not pulled from a Buf Schema Registry Go module: the generated
code is committed under `gen/` and imported by the SDK and its users, so `go get` needs nothing
but the Go module proxy. The `.proto` sources are mirrored under `proto/` from a **pinned** BSR
commit, the same commit and allowlist as sdk-node. See the `Makefile`:

| Command             | What it does                                                                                                                           |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `make sync-protos`  | `buf export buf.build/pugsh/pug:$(PROTO_COMMIT)` (read-only) → `proto/` mirror, allowlisted to the SDK packages (incl. the server-only `shared/**`). Needs BSR network. |
| `make protos`       | `buf generate` → `gen/`. Offline.                                                                                                      |
| `make proto-latest` | Print the newest commit on the module's `main` branch (to bump `PROTO_COMMIT`).                                                        |

To take a newer schema: run `make proto-latest`, set `PROTO_COMMIT` in the `Makefile`, then
`make sync-protos && make protos`, run the tests, and review + commit the `proto/` and `gen/`
diff. Builds and CI never touch the BSR: they compile the committed snapshot, and CI fails if a
fresh `make protos` differs from it.

`buf.gen.yaml` keeps the upstream Go package names (`eventsv1`, `commoneventsv1`,
`sdkprofilesv1`, …) under the `github.com/pug-sh/sdk-go/gen` prefix. It does **not** generate
`buf/validate/validate.proto`: protovalidate-go links its own copy, and a second registration of
the same file panics at init, so imports of it point at
`buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate` instead.

The same applies to any program that links two copies of the pug protos (this module's `gen/`
alongside the monorepo's `internal/gen`, say): protobuf-go refuses the duplicate registration
at startup.

## Well-known events

Unlike sdk-node, there is no generated registry: the catalog is read at runtime from the
`(common.events.v1.kind)` option of the messages in the generated `commoneventsv1` package
(`wellknown.go`), and the typed messages themselves are what `TrackWellKnown` takes.

- **Filter policy** (same as sdk-node): an event is well-known here if its
  `(common.events.v1.platforms)` option contains `PLATFORM_SERVER` **or** is unset. Events
  tagged only with web/mobile/desktop platforms are custom events to this SDK, and
  `TrackWellKnown` refuses their messages.
- `testdata/well_known_events.golden` lists the catalog (`kind` → message). A proto sync that
  adds, drops or re-targets an event fails `TestWellKnownCatalogMatchesGolden`; review the change
  and accept it with `go test -run TestWellKnownCatalogMatchesGolden -update .`.

`Track` looks up the kind in the catalog: known kinds get their properties type-checked against
the schema (by the fields' JSON names, keeping int-vs-double from the field type) and validated
with protovalidate; unknown kinds map their properties by Go type.

## Scripts

| Command        | What it does                                                        |
| -------------- | ------------------------------------------------------------------- |
| `make build`   | `go build ./...`                                                    |
| `make test`    | `go test ./... -race -count=1 -shuffle=on`                          |
| `make cover`   | Tests with a coverage profile in `coverage.out`                     |
| `make lint`    | `golangci-lint run ./...`                                           |
| `make fmt`     | `golangci-lint fmt` (goimports, `gen/` excluded)                    |
| `make vuln`    | `govulncheck ./...`                                                 |
| `make check`   | `lint`, `test`, `build`                                             |
| `make ci`      | `protos`, `check-codegen`, then `check` (what CI runs)              |

`Version` in `version.go` is sent with every event as `$sdkVersion`. Bump it together with the
release tag (`vX.Y.Z`).

## Source layout

| File                    | Responsibility                                                                  |
| ----------------------- | ------------------------------------------------------------------------------- |
| `client.go`             | `Client`, `Options`, `New`: ingestion (`Track`/`TrackWellKnown`/`Identify`) and lifecycle; Connect clients with the `x-api-key` interceptor; the `BatchCreate` sink. |
| `event.go`              | Builds and validates `Event` protos; maps Go values and typed schemas to `PropertyValue`; location. |
| `batch.go`              | Batching transport: one sender goroutine, flush triggers, retry, dead-letter.   |
| `reads.go`              | `Profiles`/`Activity`/`Insights` read facades.                                  |
| `wellknown.go`          | Well-known event catalog from the proto options.                                |
| `options.go`            | `Properties`, `Traits`, per-call options, `Location`.                           |
| `errors.go`             | `Error`, its per-code sentinels, and its normalization from Connect errors.     |
| `countries.go`          | ISO 3166-1 alpha-2 set, mirroring the server's `internal/geo`.                  |
| `log.go`, `version.go`  | `slog` wrapper; SDK version.                                                    |

## Behaviour notes (vs sdk-node)

The contract matches sdk-node; these are the places the Go port deliberately differs:

- **One sender goroutine.** Flushes run sequentially instead of overlapping, so event order
  holds across flushes.
- **Retry backoff.** A send stops at the first transiently failing chunk and re-queues it with
  the unsent ones, then waits `MaxWait` from the failure before retrying; size triggers during
  that wait don't retry early. During `Close`, the failed chunk is dead-lettered and the unsent
  rest reported as undelivered. `Close`'s flush is the last send: a `Flush` queued behind it
  sends nothing, so `Close` can't outlive its `ctx` waiting on one.
- **Every drop is logged**, whether or not `OnError` is set; `OnError` receives the events too.
- **Byte-aware chunks.** Requests are also cut at 4 MiB, and an event that alone exceeds it is
  dropped at `Track`: the server refuses bodies over 8 MiB with a transient code, so an
  oversized batch would otherwise retry forever.
- **`MaxQueueSize` holds after a retry**, too: re-queued events that overflow it drop the oldest.
- **Invalid UTF-8.** protobuf-go refuses to marshal it, failing a whole batch, so property
  strings and keys are repaired with U+FFFD and a non-UTF-8 `distinctID` is rejected.
- **Types over heuristics.** A Go `float64` is always a double and an integer always an int;
  the whole-number-is-int rule exists in sdk-node only because JavaScript has one number type.
- **Properties are options.** Go has no optional parameters and only a final variadic one, so
  `Properties` and `Traits` implement `TrackOption` and `IdentifyOption`: optional like
  sdk-node's `props?`, in any position, and merged when given more than once.
- **Errors match by code.** `*Error` has an `Is` method and a sentinel per Connect code
  (`ErrNotFound`, …), and `OnError` gets a failed send as an `*Error` where sdk-node passes the
  raw `ConnectError`, so one `errors.Is` check works for reads and dead letters.
- **Proto-name hint.** Well-known fields are keyed by JSON name, as in sdk-node, but a key that is
  a field's proto name (`product_id`) also logs a did-you-mean warning, as in sdk-python, and
  still goes out as a custom property. Unlike sdk-python, the warning comes before validation, so
  it also shows when the slip leaves a required field unset and the event is dropped.

## Testing notes

- Tests run through real protovalidate (the validator is not mocked), so they exercise the
  actual validation contract.
- Batcher tests run in `testing/synctest` bubbles, so timer-driven behaviour (`MaxWait`, retry
  backoff, send timeouts) is deterministic and instant. `TestBatchAccountsForEveryEvent` instead
  uses real concurrency and checks every event is delivered or dead-lettered exactly once; run
  it under `-race`.
- Client tests use fake Connect clients; `e2e_test.go` runs the SDK against an in-process Connect
  server (`httptest`), covering the wire format, the API key header, streaming `List`, error
  mapping and timeouts.
- The never-fails contract is asserted through a capturing `slog.Handler` standing in for
  sdk-node's console spies.

Before opening a PR: `make check`.
