# sdk-go developer tasks. Mirrors sdk-node / sdk-web / sdk-android / sdk-flutter proto vendoring:
# the module carries its generated code (gen/), so consumers just `go get` with no Buf Schema
# Registry involvement.

.PHONY: sync-protos protos check-codegen proto-latest build fmt lint test cover vuln check ci

# `ci` must run `protos` before `check-codegen` diffs gen/, and `protos` rewrites files `build`
# reads. Keep everything serial even under `make -jN` so a parallel run can't race.
.NOTPARALLEL:

# BSR module + pinned commit that proto/ is vendored from. The pin makes `make sync-protos`
# reproducible and taking newer upstream protos a deliberate, reviewable change: builds and CI
# never touch BSR (proto/ + gen/ are committed). To bump: run `make proto-latest` for the newest
# commit, set PROTO_COMMIT below, then `make sync-protos && make protos` and review the diff.
PROTO_MODULE := buf.build/pugsh/pug
PROTO_COMMIT := 8b5d0680b10c4693b4d9d15d3dbfc663

# Re-vendor proto/ from the pinned BSR commit. `buf export` is a read-only download; --path is an
# allowlist and buf pulls in transitive imports (buf/validate, google WKTs) automatically. Like
# sdk-node, this server SDK also vendors the shared/** services it reads from. Add a --path when
# the SDK starts consuming a new package.
sync-protos:
	@command -v buf >/dev/null || { echo "buf CLI required: https://buf.build/docs/installation"; exit 1; }
	rm -rf proto
	buf export $(PROTO_MODULE):$(PROTO_COMMIT) --output proto \
	  --path sdk \
	  --path common/events/v1 \
	  --path common/v1/property_value.proto \
	  --path shared/activity/v1 \
	  --path shared/insights/v1 \
	  --path shared/profiles/v1
	@echo "Synced SDK protos into proto/ at $(PROTO_COMMIT). Run 'make protos' to regenerate gen/."

# Regenerate the committed Go code from the vendored proto/ mirror. The plugins run through
# `go tool`, pinned in go.mod. If the well-known-event catalog changes, the catalog golden test
# fails until `go test ./internal/ingest -run TestWellKnownCatalogMatchesGolden -update` is run
# and reviewed.
protos:
	@command -v buf >/dev/null || { echo "buf CLI required: https://buf.build/docs/installation"; exit 1; }
	buf generate

# CI gate: the committed generated tree must match a fresh generate. In `ci`, `protos` runs
# first (ordering guaranteed by .NOTPARALLEL), so this diff catches drift anywhere in gen/.
check-codegen:
	@git diff --exit-code -- gen \
	  || { echo "codegen drift: run 'make protos' and commit"; exit 1; }

# Print the newest commit on the module's main branch, to update PROTO_COMMIT above.
proto-latest:
	@buf registry module commit resolve $(PROTO_MODULE):main

build:
	go build ./...

fmt:
	golangci-lint fmt

lint:
	golangci-lint run ./...

test:
	go test ./... -race -count=1 -shuffle=on

cover:
	go test ./... -race -count=1 -covermode=atomic -coverprofile=coverage.out

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

check: lint test build

# Strict CI target: regenerate protobufs from scratch, then run every check.
ci: protos check-codegen check
