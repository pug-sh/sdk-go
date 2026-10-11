package ingest

import (
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	commoneventsv1 "github.com/pug-sh/sdk-go/gen/common/events/v1"
)

var update = flag.Bool("update", false, "rewrite testdata golden files")

const catalogGolden = "testdata/well_known_events.golden"

// The catalog is derived from the vendored protos at runtime, so this golden file is what
// makes a proto sync that adds, drops or re-targets an event show up in review.
func TestWellKnownCatalogMatchesGolden(t *testing.T) {
	var lines []string
	for kind, mt := range wellKnown().byKind {
		lines = append(lines, fmt.Sprintf("%s %s", kind, mt.Descriptor().FullName()))
	}
	slices.Sort(lines)
	got := strings.Join(lines, "\n") + "\n"
	if *update {
		if err := os.WriteFile(catalogGolden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(catalogGolden)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Errorf("well-known catalog drifted from %s; review the change and rerun with -update", catalogGolden)
	}
}

func TestWellKnownCatalogPicksServerEvents(t *testing.T) {
	c := wellKnown()
	for _, kind := range []string{"purchase", "feature_used", "signup", "video_play", "error_occurred"} {
		if _, ok := c.byKind[kind]; !ok {
			t.Errorf("%s missing", kind)
		}
	}
	for _, kind := range []string{"page_view", "screen_view", "click", "app_open", "app_crashed"} {
		if _, ok := c.byKind[kind]; ok {
			t.Errorf("%s is not a server event", kind)
		}
	}
	for kind, mt := range c.byKind {
		if c.byName[mt.Descriptor().FullName()] != kind {
			t.Errorf("byName disagrees with byKind for %s", kind)
		}
	}
}

// options.proto's contract for the event options themselves, checked over every event
// message rather than only the server ones.
func TestEventOptionsContract(t *testing.T) {
	seen := map[string]protoreflect.FullName{}
	protoregistry.GlobalFiles.RangeFilesByPackage(commoneventsv1.File_common_events_v1_options_proto.Package(), func(fd protoreflect.FileDescriptor) bool {
		for i := range fd.Messages().Len() {
			md := fd.Messages().Get(i)
			kind, _ := proto.GetExtension(md.Options(), commoneventsv1.E_Kind).(string)
			if kind == "" {
				continue
			}
			if prev, dup := seen[kind]; dup {
				t.Errorf("kind %q is on both %s and %s", kind, prev, md.FullName())
			}
			seen[kind] = md.FullName()
			platforms, _ := proto.GetExtension(md.Options(), commoneventsv1.E_Platforms).([]commoneventsv1.Platform)
			if slices.Contains(platforms, commoneventsv1.Platform_PLATFORM_UNSPECIFIED) {
				t.Errorf("%s lists PLATFORM_UNSPECIFIED", md.FullName())
			}
		}
		return true
	})
	if len(seen) == 0 {
		t.Fatal("no event messages registered")
	}
}
