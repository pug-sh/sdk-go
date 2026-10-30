package ingest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
)

const (
	defaultMaxSize      = 100
	defaultMaxWait      = 5 * time.Second
	defaultMaxQueueSize = 10_000

	// BatchCreateRequest.events and BatchIdentifyRequest.identifies are both capped at 1000
	// by their protos.
	maxBatchEvents = 1000
	// The server refuses request bodies over 8 MiB with ResourceExhausted, a transient code,
	// so an oversized batch would be retried forever. Half the cap leaves room for the
	// request envelope.
	maxBatchBytes = 4 << 20
)

var (
	// ErrClosed is reported to OnError for events that arrived after Close or were still
	// undelivered when it finished.
	ErrClosed = errors.New("pug: client is closed")
	// ErrQueueOverflow is reported to OnError for the oldest events, dropped to make room
	// when the buffer is full.
	ErrQueueOverflow = errors.New("pug: queue overflow: dropped oldest event")
)

// BatchConfig mirrors pug.BatchConfig. Zero fields take their defaults.
type BatchConfig struct {
	MaxSize      int
	MaxWait      time.Duration
	MaxQueueSize int
}

// Resolve gives zero and negative fields their defaults, warning about the negative ones, and
// clamps MaxSize to the 1000-item request cap. NewBatcher resolves its config itself, so an
// already resolved one logs nothing new.
func (c BatchConfig) Resolve(log Logger) BatchConfig {
	c.MaxSize = batchSetting(log, "MaxSize", c.MaxSize, defaultMaxSize)
	c.MaxWait = batchSetting(log, "MaxWait", c.MaxWait, defaultMaxWait)
	c.MaxQueueSize = batchSetting(log, "MaxQueueSize", c.MaxQueueSize, defaultMaxQueueSize)
	if c.MaxSize > maxBatchEvents {
		log.Warn("Batch.MaxSize exceeds the 1000-event request cap; clamping", "value", c.MaxSize)
		c.MaxSize = maxBatchEvents
	}
	return c
}

// Items names what a Batcher carries, in its log messages.
type Items struct{ One, Many string }

var (
	// EventItems names a Batcher of tracked events.
	EventItems = Items{One: "event", Many: "events"}
	// IdentifyItems names a Batcher of queued identifies.
	IdentifyItems = Items{One: "identify", Many: "identifies"}
)

// Sink delivers one batch of items.
type Sink[T proto.Message] interface {
	Send(ctx context.Context, items []T) error
}

// Batcher buffers items and sends them in batches. Sends run on a single goroutine, so
// batches never overlap and go out in order.
type Batcher[T proto.Message] struct {
	sink     Sink[T]
	onError  func(err error, items []T)
	log      Logger
	names    Items
	timeout  time.Duration
	maxSize  int
	maxWait  time.Duration
	maxQueue int

	sendCtx     context.Context //nolint:containedctx // lifetime of the background sender, canceled by an expired Close
	cancelSends context.CancelFunc
	kick        chan struct{}
	flushes     chan chan struct{}
	quit        chan struct{}
	stopped     chan struct{}
	closeDone   chan struct{}

	mu       sync.Mutex
	buf      []T
	closed   bool
	drained  bool
	backoff  bool
	timer    *time.Timer
	timerGen uint64
}

// NewBatcher starts a Batcher's background sender. onError, if set, receives every item that
// cannot be delivered; names words the log messages.
func NewBatcher[T proto.Message](s Sink[T], cfg BatchConfig, onError func(err error, items []T), log Logger, timeout time.Duration, names Items) *Batcher[T] {
	cfg = cfg.Resolve(log)
	b := &Batcher[T]{
		sink:      s,
		onError:   onError,
		log:       log,
		names:     names,
		timeout:   timeout,
		maxSize:   cfg.MaxSize,
		maxWait:   cfg.MaxWait,
		maxQueue:  cfg.MaxQueueSize,
		kick:      make(chan struct{}, 1),
		flushes:   make(chan chan struct{}),
		quit:      make(chan struct{}),
		stopped:   make(chan struct{}),
		closeDone: make(chan struct{}),
	}
	b.sendCtx, b.cancelSends = context.WithCancel(context.Background()) //nolint:gosec // G118: Close calls cancelSends; gosec loses it through the generic receiver
	go b.run()
	return b
}

func batchSetting[T int | time.Duration](log Logger, name string, v, def T) T {
	if v < 0 {
		log.Warn("Batch."+name+" must not be negative; using the default", "value", v, "default", def)
	}
	if v <= 0 {
		return def
	}
	return v
}

func (b *Batcher[T]) run() {
	defer close(b.stopped)
	for {
		select {
		case <-b.quit:
			return
		case <-b.kick:
			// A kick during backoff is a stale size trigger; once closed, close's own flush
			// does the final send.
			b.mu.Lock()
			skip := b.backoff || b.closed
			b.mu.Unlock()
			if !skip {
				b.flushOnce()
			}
		case done := <-b.flushes:
			b.flushOnce()
			close(done)
		}
	}
}

func (b *Batcher[T]) Enqueue(item T) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		b.log.Warn("client closed; " + b.names.One + " dropped")
		b.deadLetter(ErrClosed, []T{item})
		return
	}
	var dropped []T
	if len(b.buf) >= b.maxQueue {
		dropped = b.dropOldestLocked(len(b.buf) - b.maxQueue + 1)
	}
	b.buf = append(b.buf, item)
	full := len(b.buf) >= b.maxSize && !b.backoff
	if !full {
		b.armLocked()
	}
	b.mu.Unlock()
	if dropped != nil {
		b.log.Warn("queue full; dropping oldest " + b.names.One)
		b.deadLetter(ErrQueueOverflow, dropped)
	}
	if full {
		b.signal()
	}
}

func (b *Batcher[T]) Flush(ctx context.Context) error {
	done := make(chan struct{})
	select {
	case b.flushes <- done:
	case <-b.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Batcher[T]) Close(ctx context.Context) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		select {
		case <-b.closeDone:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	b.closed = true
	b.disarmLocked()
	b.mu.Unlock()

	err := b.Flush(ctx)
	// That was the final attempt. A Flush queued behind it must not send again, or close would
	// wait out those sends whatever its ctx says.
	b.mu.Lock()
	b.drained = true
	b.mu.Unlock()
	b.cancelSends()
	close(b.quit)
	<-b.stopped

	b.mu.Lock()
	undelivered := b.buf
	b.buf = nil
	b.mu.Unlock()
	if len(undelivered) > 0 {
		b.log.Error("client closed with undelivered "+b.names.Many+"; dropped", b.names.Many, len(undelivered))
		b.deadLetter(fmt.Errorf("%w with %d undelivered %s", ErrClosed, len(undelivered), b.names.Many), undelivered)
	}
	close(b.closeDone)
	return err
}

// flushOnce stops at the first transient failure, since the rest would likely fail the same
// way. Open, the failed and unsent chunks are re-queued in order for a retry after MaxWait;
// closed, the failed chunk is dead-lettered and the rest left for close to report.
func (b *Batcher[T]) flushOnce() {
	b.mu.Lock()
	if b.drained {
		b.mu.Unlock()
		return
	}
	b.disarmLocked()
	batch := b.buf
	b.buf = nil
	b.mu.Unlock()
	if len(batch) == 0 {
		return
	}

	chunks := b.split(batch)
	for i, chunk := range chunks {
		err := b.sendChunk(chunk)
		switch {
		case err == nil:
			continue
		case isPermanent(err):
			b.log.Error("permanent send failure; "+b.names.Many+" dropped", b.names.Many, len(chunk), "err", err)
			b.deadLetter(err, chunk)
			continue
		}
		b.mu.Lock()
		closed := b.closed
		b.mu.Unlock()
		if closed {
			b.log.Error("client closed; "+b.names.Many+" dropped after a transient send failure", b.names.Many, len(chunk), "err", err)
			b.deadLetter(err, chunk)
			b.requeue(slices.Concat(chunks[i+1:]...), false)
		} else {
			b.log.Warn("transient send failure; will retry", b.names.Many, len(chunk), "err", err)
			b.requeue(slices.Concat(chunks[i:]...), true)
		}
		return
	}
	b.mu.Lock()
	b.backoff = false
	b.mu.Unlock()
}

func (b *Batcher[T]) split(batch []T) [][]T {
	var chunks [][]T
	start, size := 0, 0
	for i, item := range batch {
		n := proto.Size(item)
		if i > start && (i-start >= b.maxSize || size+n > maxBatchBytes) {
			chunks = append(chunks, batch[start:i:i])
			start, size = i, 0
		}
		size += n
	}
	return append(chunks, batch[start:])
}

func (b *Batcher[T]) sendChunk(chunk []T) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("sending batch panicked: %v", r)
		}
	}()
	ctx, cancel := context.WithTimeout(b.sendCtx, b.timeout)
	defer cancel()
	return b.sink.Send(ctx, chunk)
}

func (b *Batcher[T]) requeue(items []T, retry bool) {
	b.mu.Lock()
	b.buf = append(items, b.buf...)
	var dropped []T
	if over := len(b.buf) - b.maxQueue; over > 0 {
		dropped = b.dropOldestLocked(over)
	}
	if retry {
		// A timer an enqueue armed during the send would fire early; the wait runs from now.
		b.backoff = true
		b.disarmLocked()
		b.armLocked()
	}
	b.mu.Unlock()
	if dropped != nil {
		b.log.Warn("queue full; dropping oldest "+b.names.Many, b.names.Many, len(dropped))
		b.deadLetter(ErrQueueOverflow, dropped)
	}
}

func (b *Batcher[T]) dropOldestLocked(n int) []T {
	dropped := slices.Clone(b.buf[:n])
	clear(b.buf[:n])
	b.buf = b.buf[n:]
	return dropped
}

func (b *Batcher[T]) armLocked() {
	if b.timer != nil || b.closed {
		return
	}
	b.timerGen++
	gen := b.timerGen
	b.timer = time.AfterFunc(b.maxWait, func() { b.timerFired(gen) })
}

func (b *Batcher[T]) disarmLocked() {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
		b.timerGen++
	}
}

func (b *Batcher[T]) timerFired(gen uint64) {
	b.mu.Lock()
	if gen != b.timerGen {
		b.mu.Unlock()
		return
	}
	b.timer = nil
	b.backoff = false
	b.mu.Unlock()
	b.signal()
}

func (b *Batcher[T]) signal() {
	select {
	case b.kick <- struct{}{}:
	default:
	}
}

// A panicking callback must not take down the sender, which would strand every later item.
func (b *Batcher[T]) deadLetter(err error, items []T) {
	if b.onError == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			b.log.Error("OnError panicked; "+b.names.Many+" may be lost", b.names.Many, len(items), "panic", r)
		}
	}()
	b.onError(err, items)
}

// Retrying cannot fix these client-error codes, nor an error that did not come from the RPC
// layer, which is a bug.
func isPermanent(err error) bool {
	cerr, ok := errors.AsType[*connect.Error](err)
	if !ok {
		return true
	}
	switch cerr.Code() {
	case connect.CodeInvalidArgument, connect.CodeNotFound, connect.CodeAlreadyExists,
		connect.CodePermissionDenied, connect.CodeFailedPrecondition, connect.CodeUnimplemented,
		connect.CodeUnauthenticated:
		return true
	default:
		return false
	}
}
