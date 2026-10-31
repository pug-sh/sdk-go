package pug

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"buf.build/go/protovalidate"
	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	eventsv1 "github.com/pug-sh/sdk-go/gen/sdk/events/v1"
	"github.com/pug-sh/sdk-go/gen/sdk/events/v1/eventsv1connect"
	sdkprofilesv1 "github.com/pug-sh/sdk-go/gen/sdk/profiles/v1"
	"github.com/pug-sh/sdk-go/gen/sdk/profiles/v1/sdkprofilesv1connect"
	"github.com/pug-sh/sdk-go/gen/shared/activity/v1/activityv1connect"
	"github.com/pug-sh/sdk-go/gen/shared/insights/v1/insightsv1connect"
	"github.com/pug-sh/sdk-go/gen/shared/profiles/v1/profilesv1connect"
	"github.com/pug-sh/sdk-go/internal/ingest"
)

const (
	defaultEndpoint = "https://api.pugs.dev"
	defaultTimeout  = 10 * time.Second
	reservedPrefix  = "cookieless-"
)

// Options configures a [Client]. Only APIKey is required.
type Options struct {
	// APIKey is the project's private API key (prv_…).
	APIKey string
	// Endpoint is the Pug server origin. Defaults to the hosted endpoint.
	Endpoint string
	// Batch tunes the ingestion buffer.
	Batch BatchConfig
	// OnError receives queued events and identifies that could not be delivered: a permanent
	// send failure, a queue overflow, or ones still undelivered at Close.
	OnError OnError
	// Logger receives the SDK's warnings and errors. Defaults to slog.Default().
	Logger *slog.Logger
	// HTTPClient sends the requests. Defaults to a client using http.DefaultTransport.
	HTTPClient *http.Client
	// Timeout bounds each request whose context has no deadline. Defaults to 10s.
	Timeout time.Duration
}

// BatchConfig tunes the ingestion buffers. Events and identifies each get a buffer with these
// settings. Zero fields take their defaults.
type BatchConfig struct {
	// MaxSize is the buffered-event count that triggers a send, and the most events sent in
	// one request. Defaults to 100; capped at 1000.
	MaxSize int
	// MaxWait is the longest an event waits before its batch is sent, and the retry interval
	// after a transient failure. Defaults to 5s.
	MaxWait time.Duration
	// MaxQueueSize bounds the buffer: when it is full, the oldest event is dropped and
	// reported to OnError. Defaults to 10,000.
	MaxQueueSize int
}

// OnError is a dead-letter hook for queued events and identifies that could not be delivered.
// err matches [ErrQueueOverflow] or [ErrClosed] under [errors.Is], or is an [*Error] for a
// failed send. It runs on the goroutine that called Track or Identify or on a background
// sender, so it must be safe for concurrent use, return promptly, and not call Flush or Close.
type OnError func(err error, lost Undelivered)

// Undelivered is what one failure lost. Either slice may be empty.
type Undelivered struct {
	Events     []*Event
	Identifies []*Identify
}

// Event is the wire form of a tracked event, as handed to [OnError].
type Event = eventsv1.Event

// Identify is the wire form of a queued identify, as handed to [OnError].
type Identify = sdkprofilesv1.IdentifyRequest

// Client is a Pug server-side client, safe for concurrent use. Ingestion is buffered and
// sent by a background goroutine, so call [Client.Close] on shutdown.
type Client struct {
	// Profiles reads and deletes profiles.
	Profiles *ProfilesClient
	// Activity reads raw events and per-profile activity.
	Activity *ActivityClient
	// Insights runs analytics queries.
	Insights *InsightsClient

	rpc        rpcClients
	events     ingest.Builder
	queue      *ingest.Batcher[*Event]
	identifies *ingest.Batcher[*Identify]
	log        ingest.Logger
	timeout    time.Duration
	closed     atomic.Bool
}

// New returns a Client. It fails only on unusable options: a missing, malformed or non-private
// API key, or an Endpoint that is not an absolute http(s) URL.
func New(opts Options) (*Client, error) {
	switch {
	case opts.APIKey == "":
		return nil, errors.New("pug: APIKey is required")
	// net/http refuses such a header on every request, and the failure looks transient, so
	// events would retry until they overflow.
	case strings.ContainsFunc(opts.APIKey, func(r rune) bool { return r <= ' ' || r > '~' }):
		return nil, errors.New("pug: APIKey must be printable ASCII without spaces (check for a trailing newline)")
	case !strings.HasPrefix(opts.APIKey, "prv_"):
		return nil, errors.New("pug: APIKey must be a private key (prv_…) for the server SDK")
	}
	endpoint := cmp.Or(opts.Endpoint, defaultEndpoint)
	if u, err := url.Parse(endpoint); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("pug: Endpoint must be an absolute http(s) URL, got %q", endpoint)
	}
	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	sessionID, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("pug: generating a session id: %w", err)
	}
	return newClient(newRPCClients(httpClient, endpoint, opts.APIKey), opts, sessionID.String()), nil
}

func newClient(rpc rpcClients, opts Options, sessionID string) *Client {
	log := ingest.NewLogger(opts.Logger)
	timeout := opts.Timeout
	if timeout < 0 {
		log.Warn("Timeout must not be negative; using the default", "value", timeout, "default", defaultTimeout)
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	c := &Client{rpc: rpc, log: log, timeout: timeout}
	c.events = ingest.Builder{Log: log, SessionID: sessionID, Version: Version}
	// Resolved once, so a bad setting warns once rather than once per queue.
	batch := ingest.BatchConfig(opts.Batch).Resolve(log)
	var onEvents func(error, []*Event)
	var onIdentifies func(error, []*Identify)
	if onError := opts.OnError; onError != nil {
		onEvents = func(err error, events []*Event) { onError(sendError(err), Undelivered{Events: events}) }
		onIdentifies = func(err error, identifies []*Identify) { onError(sendError(err), Undelivered{Identifies: identifies}) }
	}
	c.queue = ingest.NewBatcher[*Event](eventSink{client: rpc.events, log: log}, batch, onEvents, log, timeout, ingest.EventItems)
	c.identifies = ingest.NewBatcher[*Identify](&identifySink{client: rpc.profilesSDK, log: log}, batch, onIdentifies, log, timeout, ingest.IdentifyItems)
	c.Profiles = &ProfilesClient{c}
	c.Activity = &ActivityClient{c}
	c.Insights = &InsightsClient{c}
	return c
}

// Kind is an event's kind, such as "order.completed". An untyped string constant converts to
// it implicitly; a string variable needs Kind(s), so a distinct ID passed where the kind
// belongs fails to compile.
type Kind string

// Track enqueues an event of kind for distinctID and returns at once; a background goroutine
// sends events in batches. Its properties are fields (String, Int, …) and Properties maps, in
// the same list as the With… options. It never fails: invalid input is logged and dropped.
//
// For a well-known kind such as "purchase", properties are checked against the event's schema,
// keyed by the fields' JSON names ("productId"; "product_id" logs a warning), and other keys
// ride along as custom properties. Any other kind is a custom event with free-form properties.
// [Client.TrackWellKnown] takes the typed schema instead.
func (c *Client) Track(distinctID string, kind Kind, opts ...TrackOption) {
	defer c.recoverPanic("Track", "kind", string(kind))
	if !c.accept(distinctID, string(kind)) {
		return
	}
	o := trackOpts(opts)
	if ev := c.events.Build(string(kind), distinctID, o.props, o.Options); ev != nil {
		c.queue.Enqueue(ev)
	}
}

// TrackWellKnown enqueues a well-known event from its typed properties message, such as
// &commoneventsv1.PurchaseProperties{...}; the kind comes from the message's schema.
// [Field]s and [Properties] among opts are sent as custom properties alongside the typed
// fields, except one keyed by a typed field's JSON name, which is dropped. Like Track, it never
// fails: invalid input is logged and dropped.
func (c *Client) TrackWellKnown(distinctID string, event proto.Message, opts ...TrackOption) {
	defer c.recoverPanic("TrackWellKnown", "message", fmt.Sprintf("%T", event))
	if event == nil {
		c.log.Error("TrackWellKnown requires an event message")
		return
	}
	msg := event.ProtoReflect()
	kind, ok := ingest.WellKnownKind(msg.Descriptor().FullName())
	if !ok {
		c.log.Error("TrackWellKnown requires a well-known server event message", "message", msg.Descriptor().FullName())
		return
	}
	if !c.accept(distinctID, kind) {
		return
	}
	o := trackOpts(opts)
	if ev := c.events.BuildWellKnown(kind, distinctID, msg, o.props, o.Options); ev != nil {
		c.queue.Enqueue(ev)
	}
}

func (c *Client) accept(distinctID, kind string) bool {
	switch {
	case c.closed.Load():
		c.log.Warn("Track called after Close; event dropped", "kind", kind)
	case distinctID == "":
		c.log.Error("Track requires a non-empty distinctID", "kind", kind)
	// The server rejects a whole batch over one such id, taking other users' events with it.
	case strings.HasPrefix(distinctID, reservedPrefix):
		c.log.Error("Track distinctID must not start with the reserved 'cookieless-' prefix", "kind", kind)
	// protobuf-go refuses to marshal a string that isn't UTF-8, which would fail the whole
	// batch.
	case !utf8.ValidString(distinctID):
		c.log.Error("Track distinctID must be valid UTF-8", "kind", kind)
	default:
		return true
	}
	return false
}

// Identify queues a create-or-update of the profile for externalID and returns at once; a
// background goroutine sends identifies in batches with BatchIdentify, or with one Identify
// call each on a server that predates it. Its traits are fields (String, Int, …), named traits
// (Name, FirstName, LastName, Email, Avatar) and Traits maps, in the same list as the With…
// options, and are shallow-merged into the stored properties. It never fails: invalid input is
// logged and dropped, and an identify that can't be delivered is reported to OnError.
func (c *Client) Identify(externalID string, opts ...IdentifyOption) {
	defer c.recoverPanic("Identify")
	if c.closed.Load() {
		c.log.Warn("Identify called after Close; dropped")
		return
	}
	if externalID == "" {
		c.log.Error("Identify requires a non-empty externalID")
		return
	}
	o := identifyOpts(opts)
	if !utf8.ValidString(externalID) || !utf8.ValidString(o.anonymousID) || !utf8.ValidString(o.deviceID) {
		c.log.Error("Identify ids must be valid UTF-8")
		return
	}
	req := &sdkprofilesv1.IdentifyRequest{ExternalId: proto.String(externalID)}
	if o.anonymousID != "" {
		req.AnonymousId = proto.String(o.anonymousID)
	}
	if o.deviceID != "" {
		req.DeviceId = proto.String(o.deviceID)
	}
	o.traits = c.dropNonFinite(o.traits)
	if len(o.traits) > 0 {
		s, err := toStruct(o.traits)
		if err != nil {
			c.log.Error("Identify traits must be JSON-serializable", "err", err)
			return
		}
		req.Traits = s
	}
	if err := protovalidate.GlobalValidator.Validate(req); err != nil {
		c.log.Error("invalid identify request", "violations", ingest.Violations(err))
		return
	}
	if size := proto.Size(req); size > ingest.MaxIdentifyBytes {
		c.log.Error("identify dropped: too large to send", "bytes", size, "limit", ingest.MaxIdentifyBytes)
		return
	}
	c.identifies.Enqueue(req)
}

// dropNonFinite returns traits without their top-level NaN or ±Inf values, which would
// otherwise fail the whole identify's JSON encoding, so a bad value costs only its own trait.
// traits may be the caller's own map, so it is copied before anything is removed.
func (c *Client) dropNonFinite(traits Traits) Traits {
	out, copied := traits, false
	for k, v := range traits {
		var f float64
		switch v := v.(type) {
		case float64:
			f = v
		case float32:
			f = float64(v)
		default:
			continue
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			c.log.Warn("trait not representable; skipping", "trait", k, "value", f)
			if !copied {
				out, copied = maps.Clone(traits), true
			}
			delete(out, k)
		}
	}
	return out
}

// A JSON round trip accepts anything encoding/json does (structs, typed slices, time.Time),
// and refuses what a Struct can't carry, such as NaN.
func toStruct(traits Traits) (*structpb.Struct, error) {
	b, err := json.Marshal(map[string]any(traits))
	if err != nil {
		return nil, err
	}
	s := &structpb.Struct{}
	if err := protojson.Unmarshal(b, s); err != nil {
		return nil, err
	}
	return s, nil
}

// Flush sends the queued events and identifies now. It returns once an attempt in progress and
// its own attempt have finished, or with ctx's error if ctx ends first. It does not guarantee
// empty queues: a transient failure keeps items queued for a later retry. Use Close to drain
// on shutdown.
func (c *Client) Flush(ctx context.Context) error {
	return both(ctx, c.queue.Flush, c.identifies.Flush)
}

// Close drains both queues and stops their background senders; later Track and Identify calls
// are dropped with a warning. Events and identifies that cannot be delivered, including any
// left when ctx ends, are reported to OnError. It returns ctx's error if ctx ended before the
// drain finished.
func (c *Client) Close(ctx context.Context) error {
	c.closed.Store(true)
	return both(ctx, c.queue.Close, c.identifies.Close)
}

// both runs the event and identify queues' step concurrently and returns the events error,
// else the identifies one.
func both(ctx context.Context, events, identifies func(context.Context) error) error {
	errc := make(chan error, 1)
	go func() { errc <- identifies(ctx) }()
	err := events(ctx)
	if ierr := <-errc; err == nil {
		err = ierr
	}
	return err
}

func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.timeout)
}

// recoverPanic upholds the never-fails contract of the ingestion methods against a panic
// from user code they call into, such as a MarshalJSON method in traits.
func (c *Client) recoverPanic(method string, args ...any) {
	if r := recover(); r != nil {
		c.log.Error("unexpected panic in "+method, append(args, "panic", r)...)
	}
}

// rpcClients are the Connect clients for the Pug API; each request carries the API key.
type rpcClients struct {
	events      eventsv1connect.EventsServiceClient
	profilesSDK sdkprofilesv1connect.ProfilesSDKServiceClient
	profiles    profilesv1connect.ProfilesServiceClient
	activity    activityv1connect.ActivityServiceClient
	insights    insightsv1connect.InsightsServiceClient
}

func newRPCClients(httpClient connect.HTTPClient, endpoint, apiKey string) rpcClients {
	opt := connect.WithInterceptors(apiKeyInterceptor{key: apiKey})
	return rpcClients{
		events:      eventsv1connect.NewEventsServiceClient(httpClient, endpoint, opt),
		profilesSDK: sdkprofilesv1connect.NewProfilesSDKServiceClient(httpClient, endpoint, opt),
		profiles:    profilesv1connect.NewProfilesServiceClient(httpClient, endpoint, opt),
		activity:    activityv1connect.NewActivityServiceClient(httpClient, endpoint, opt),
		insights:    insightsv1connect.NewInsightsServiceClient(httpClient, endpoint, opt),
	}
}

type apiKeyInterceptor struct{ key string }

func (i apiKeyInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("x-api-key", i.key)
		return next(ctx, req)
	}
}

func (i apiKeyInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		conn.RequestHeader().Set("x-api-key", i.key)
		return conn
	}
}

func (apiKeyInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

type eventSink struct {
	client eventsv1connect.EventsServiceClient
	log    ingest.Logger
}

func (s eventSink) Send(ctx context.Context, events []*eventsv1.Event) error {
	res, err := s.client.BatchCreate(ctx, connect.NewRequest(&eventsv1.BatchCreateRequest{Events: events}))
	if err != nil {
		return err
	}
	// A refused event isn't an RPC error, so this is the only sign it was lost.
	if dropped := res.Msg.GetDropped(); dropped > 0 {
		s.log.Warn("server dropped events", "dropped", dropped, "sent", len(events), "reasons", res.Msg.GetDroppedByReason())
	}
	return nil
}

// identifySink sends queued identifies with BatchIdentify, or with one Identify call each on a
// server that predates it.
type identifySink struct {
	client    sdkprofilesv1connect.ProfilesSDKServiceClient
	log       ingest.Logger
	unbatched atomic.Bool // the server has no BatchIdentify
}

func (s *identifySink) Send(ctx context.Context, items []*Identify) error {
	if !s.unbatched.Load() {
		_, err := s.client.BatchIdentify(ctx, connect.NewRequest(&sdkprofilesv1.BatchIdentifyRequest{Identifies: items}))
		// A server without the route answers 404, which Connect reads as Unimplemented. This has
		// to be caught here: the batcher treats Unimplemented as permanent and would drop the
		// identifies.
		if connect.CodeOf(err) != connect.CodeUnimplemented {
			return err
		}
		if s.unbatched.CompareAndSwap(false, true) {
			s.log.Warn("server has no BatchIdentify; sending identifies one at a time")
		}
	}
	// One deadline covers the whole chunk, so a slow server may not get through it; reporting
	// the delivered prefix lets the next attempt carry on from the first unsent item.
	for i, item := range items {
		if _, err := s.client.Identify(ctx, connect.NewRequest(item)); err != nil {
			return &ingest.PartialSendError{Sent: i, Err: err}
		}
	}
	return nil
}
