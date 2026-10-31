# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Read `docs/development.md` before touching protos or codegen, batching, well-known events, or any behaviour sdk-node shares. It has the proto bump workflow, the golden-file update command, and the deliberate differences from sdk-node.

## Verifying

- Run `make check` (lint, test, build) before calling a change done.
- If `go.mod` or `proto/` changed, also run `go mod tidy -diff` and `make ci`. CI regenerates `gen/` and fails on any diff.

## Generated code

- Never hand-edit `gen/` (buf output) or `proto/` (a `buf export` mirror of `buf.build/pugsh/pug`). Schema changes go upstream in the pug monorepo and arrive by bumping `PROTO_COMMIT`.
- The generators are `tool` deps in `go.mod`, so bumping `google.golang.org/protobuf` or `connectrpc.com/connect` means running `make protos` and committing `gen/` with the bump.

## Code

- Keep the root package to the public API. Implementation belongs in `internal/ingest`; see the source layout in `docs/development.md`.
- Don't use stdlib APIs newer than the `go` version in `go.mod`, even when the local toolchain is newer. CI also tests oldstable.
- Ingestion (`Track`, `TrackWellKnown`, `Identify`) never returns an error or panics into the caller. It logs, drops, and reports buffered failures through `OnError`. Only reads return errors, as `*pug.Error`.
- Log through `ingest.Logger` (`internal/ingest/log.go`), never slog directly; it adds the `pug: ` prefix. Errors returned to users (sentinels, `New` validation) carry that prefix themselves.
- Set optional proto fields with `proto.String`, `proto.Bool`, etc., not `new(x)`.
- Tests use stdlib `testing` only (no testify or go-cmp) and the fakes in each package's `helpers_test.go`.
- Ask before changing a root-package exported identifier's name, signature or behaviour. README.md's examples aren't compiled, so update them by hand.

## Other SDKs

- sdk-node is the reference contract. Check what it does before changing public behaviour, and record deliberate divergences under "Behaviour notes (vs sdk-node)" in `docs/development.md`. Sibling SDKs may be checked out next to this repo (`../sdk-node`, `../sdk-python`, …).
- sdk-python, sdk-ruby and sdk-rust copy sdk-go's design, so say when a change needs porting to them. `internal/ingest/testdata/well_known_events.golden` must stay byte-identical to sdk-python's `tests/testdata/well_known_events.golden`.
- `PROTO_COMMIT` is pinned to the same BSR commit in sdk-node, sdk-flutter, sdk-python, sdk-ruby and sdk-rust. Bump it only in lockstep with them.

## Git

- Branches are `feat/…`, `fix/…` or `chore/…`, merged by PR.
- Commit subjects are plain imperative sentences with no `feat:`-style prefix; the body explains why.
- A release is a `Release vX.Y.Z` commit that bumps `Version` in `version.go`, tagged `vX.Y.Z`.
