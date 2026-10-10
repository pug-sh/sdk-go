package pug

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	sdkprofilesv1 "github.com/pug-sh/sdk-go/gen/sdk/profiles/v1"
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
	// OnError receives buffered events that could not be delivered: a permanent send failure,
	// a queue overflow, or events still undelivered at Close. Identify failures are logged,
	// not routed here.
	OnError OnError
	// Logger receives the SDK's warnings and errors. Defaults to slog.Default().
	Logger *slog.Logger
	// HTTPClient sends the requests. Defaults to a client using http.DefaultTransport.
	HTTPClient *http.Client
	// Timeout bounds each request whose context has no deadline. Defaults to 10s.
	Timeout time.Duration
}

// BatchConfig tunes the ingestion buffer. Zero fields take their defaults.
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

// OnError is a dead-letter hook for buffered events that could not be delivered. It runs on
// the goroutine that called Track or on the background sender, so it must be safe for
// concurrent use, return promptly, and not call Flush or Close.
type OnError func(err error, events []*Event)

// Event is the wire form of a tracked event, as handed to [OnError].
type Event = eventsv1.Event

// Client is a Pug server-side client, safe for concurrent use. Ingestion is buffered and
// sent by a background goroutine, so call [Client.Close] on shutdown.
type Client struct {
	// Profiles reads and deletes profiles.
	Profiles *ProfilesClient
	// Activity reads raw events and per-profile activity.
	Activity *ActivityClient
	// Insights runs analytics queries.
	Insights *InsightsClient

	rpc       rpcClients
	queue     *batcher
	log       logger
	timeout   time.Duration
	sessionID string
	closed    atomic.Bool
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
	log := logger{opts.Logger}
	timeout := opts.Timeout
	if timeout < 0 {
		log.warn("Timeout must not be negative; using the default", "value", timeout, "default", defaultTimeout)
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	c := &Client{rpc: rpc, log: log, timeout: timeout, sessionID: sessionID}
	c.queue = newBatcher(eventSink{client: rpc.events, log: log}, opts.Batch, opts.OnError, log, timeout)
	c.Profiles = &ProfilesClient{c}
	c.Activity = &ActivityClient{c}
	c.Insights = &InsightsClient{c}
	return c
}

// Track enqueues an event of kind for distinctID and returns at once; a background goroutine
// sends events in batches. It never fails: invalid input is logged and dropped.
//
// For a well-known kind such as "purchase", props are checked against the event's schema,
// keyed by the fields' JSON names ("productId"), and other keys ride along as custom
// properties. Any other kind is a custom event with free-form props. [Client.TrackWellKnown]
// takes the typed schema instead.
func (c *Client) Track(distinctID, kind string, props Properties, opts ...TrackOption) {
	defer c.recoverPanic("Track", "kind", kind)
	if !c.accept(distinctID, kind) {
		return
	}
	if ev := toEvent(c.log, kind, c.sessionID, distinctID, props, trackOpts(opts)); ev != nil {
		c.queue.enqueue(ev)
	}
}

// TrackWellKnown enqueues a well-known event from its typed properties message, such as
// &commoneventsv1.PurchaseProperties{...}; the kind comes from the message's schema. extras
// are sent as custom properties alongside the typed fields, except one named like a typed
// field, which is dropped. Like Track, it never fails: invalid input is logged and dropped.
func (c *Client) TrackWellKnown(distinctID string, event proto.Message, extras Properties, opts ...TrackOption) {
	defer c.recoverPanic("TrackWellKnown", "message", fmt.Sprintf("%T", event))
	if event == nil {
		c.log.error("TrackWellKnown requires an event message")
		return
	}
	msg := event.ProtoReflect()
	kind, ok := wellKnown().byName[msg.Descriptor().FullName()]
	if !ok {
		c.log.error("TrackWellKnown requires a well-known server event message", "message", msg.Descriptor().FullName())
		return
	}
	if !c.accept(distinctID, kind) {
		return
	}
	if ev := toWellKnownEvent(c.log, kind, c.sessionID, distinctID, msg, extras, trackOpts(opts)); ev != nil {
		c.queue.enqueue(ev)
	}
}

func (c *Client) accept(distinctID, kind string) bool {
	switch {
	case c.closed.Load():
		c.log.warn("Track called after Close; event dropped", "kind", kind)
	case distinctID == "":
		c.log.error("Track requires a non-empty distinctID", "kind", kind)
	// The server rejects a whole batch over one such id, taking other users' events with it.
	case strings.HasPrefix(distinctID, reservedPrefix):
		c.log.error("Track distinctID must not start with the reserved 'cookieless-' prefix", "kind", kind)
	// protobuf-go refuses to marshal a string that isn't UTF-8, which would fail the whole
	// batch.
	case !utf8.ValidString(distinctID):
		c.log.error("Track distinctID must be valid UTF-8", "kind", kind)
	default:
		return true
	}
	return false
}

// Identify creates or updates the profile for externalID, shallow-merging traits into its
// stored properties. It blocks until the server answers. It never fails: invalid input and
// request failures are logged.
func (c *Client) Identify(ctx context.Context, externalID string, traits Traits, opts ...IdentifyOption) {
	defer c.recoverPanic("Identify")
	if c.closed.Load() {
		c.log.warn("Identify called after Close; dropped")
		return
	}
	if externalID == "" {
		c.log.error("Identify requires a non-empty externalID")
		return
	}
	var o identifyOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if !utf8.ValidString(externalID) || !utf8.ValidString(o.anonymousID) || !utf8.ValidString(o.deviceID) {
		c.log.error("Identify ids must be valid UTF-8")
		return
	}
	req := &sdkprofilesv1.IdentifyRequest{ExternalId: proto.String(externalID)}
	if o.anonymousID != "" {
		req.AnonymousId = proto.String(o.anonymousID)
	}
	if o.deviceID != "" {
		req.DeviceId = proto.String(o.deviceID)
	}
	if traits != nil {
		s, err := toStruct(traits)
		if err != nil {
			c.log.error("Identify traits must be JSON-serializable", "err", err)
			return
		}
		req.Traits = s
	}
	if err := protovalidate.GlobalValidator.Validate(req); err != nil {
		c.log.error("invalid identify request", "violations", violations(err))
		return
	}
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	if _, err := c.rpc.profilesSDK.Identify(ctx, connect.NewRequest(req)); err != nil {
		c.log.error("identify failed", "external_id", externalID, "err", err)
	}
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

// Flush sends the buffered events now. It returns once an attempt in progress and its own
// attempt have finished, or with ctx's error if ctx ends first. It does not guarantee an empty
// queue: a transient failure keeps events queued for a later retry. Use Close to drain on
// shutdown.
func (c *Client) Flush(ctx context.Context) error {
	return c.queue.flush(ctx)
}

// Close drains the buffer and stops the background sender; later Track and Identify calls
// are dropped with a warning. Events that cannot be delivered, including any left when ctx
// ends, are reported to OnError. It returns ctx's error if ctx ended before the drain
// finished.
func (c *Client) Close(ctx context.Context) error {
	c.closed.Store(true)
	return c.queue.close(ctx)
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
		c.log.error("unexpected panic in "+method, append(args, "panic", r)...)
	}
}
