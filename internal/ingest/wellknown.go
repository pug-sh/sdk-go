package ingest

import (
	"cmp"
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	commoneventsv1 "github.com/pug-sh/sdk-go/gen/common/events/v1"
)

type catalog struct {
	byKind map[string]protoreflect.MessageType
	byName map[protoreflect.FullName]string
}

// wellKnown maps each well-known event kind to its *Properties message, read from the
// (common.events.v1.kind) option of the generated schemas. Like sdk-node's generated registry,
// it holds the events whose (common.events.v1.platforms) option lists PLATFORM_SERVER or is
// unset; events tagged only for web, mobile or desktop are custom events here.
var wellKnown = sync.OnceValue(func() catalog {
	var mds []protoreflect.MessageDescriptor
	pkg := commoneventsv1.File_common_events_v1_options_proto.Package()
	protoregistry.GlobalFiles.RangeFilesByPackage(pkg, func(fd protoreflect.FileDescriptor) bool {
		for i := range fd.Messages().Len() {
			mds = append(mds, fd.Messages().Get(i))
		}
		return true
	})
	slices.SortFunc(mds, func(a, b protoreflect.MessageDescriptor) int { return cmp.Compare(a.FullName(), b.FullName()) })

	c := catalog{byKind: map[string]protoreflect.MessageType{}, byName: map[protoreflect.FullName]string{}}
	for _, md := range mds {
		kind, _ := proto.GetExtension(md.Options(), commoneventsv1.E_Kind).(string)
		if kind == "" || !forServer(md) {
			continue
		}
		if _, dup := c.byKind[kind]; dup {
			continue
		}
		mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName())
		if err != nil {
			continue
		}
		c.byKind[kind] = mt
		c.byName[md.FullName()] = kind
	}
	return c
})

// WellKnownKind returns the kind of the well-known server event whose properties message is
// named name.
func WellKnownKind(name protoreflect.FullName) (string, bool) {
	kind, ok := wellKnown().byName[name]
	return kind, ok
}

func forServer(md protoreflect.MessageDescriptor) bool {
	platforms, _ := proto.GetExtension(md.Options(), commoneventsv1.E_Platforms).([]commoneventsv1.Platform)
	return len(platforms) == 0 || slices.Contains(platforms, commoneventsv1.Platform_PLATFORM_SERVER)
}
