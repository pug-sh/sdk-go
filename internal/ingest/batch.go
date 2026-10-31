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

	eventsv1 "github.com/pug-sh/sdk-go/gen/sdk/events/v1"
)

const (
	defaultMaxSize      = 100
	defaultMaxWait      = 5 * time.Second
	defaultMaxQueueSize = 10_000

	// BatchCreateRequest.events is capped at 1000 by the events proto.
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

// Sink delivers one batch of events.
type Sink interface {
	Send(ctx context.Context, events []*eventsv1.Event) error
}

// Batcher buffers events and sends them in batches. Sends run on a single goroutine, so
// batches never overlap and go out in order.
type Batcher struct {
	sink     Sink
	onError  func(err error, events []*eventsv1.Event)
	log      Logger
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
	buf      []*eventsv1.Event
	closed   bool
	drained  bool
	backoff  bool
	timer    *time.Timer
	timerGen uint64
}

// NewBatcher starts a Batcher's background sender. onError, if set, receives every event that
// cannot be delivered.
func NewBatcher(s Sink, cfg BatchConfig, onError func(err error, events []*eventsv1.Event), log Logger, timeout time.Duration) *Batcher {
	b := &Batcher{
		sink:      s,
		onError:   onError,
		log:       log,
		timeout:   timeout,
		maxSize:   batchSetting(log, "MaxSize", cfg.MaxSize, defaultMaxSize),
		maxWait:   batchSetting(log, "MaxWait", cfg.MaxWait, defaultMaxWait),
		maxQueue:  batchSetting(log, "MaxQueueSize", cfg.MaxQueueSize, defaultMaxQueueSize),
		kick:      make(chan struct{}, 1),
		flushes:   make(chan chan struct{}),
		quit:      make(chan struct{}),
		stopped:   make(chan struct{}),
		closeDone: make(chan struct{}),
	}
	if b.maxSize > maxBatchEvents {
		log.Warn("Batch.MaxSize exceeds the 1000-event request cap; clamping", "value", b.maxSize)
		b.maxSize = maxBatchEvents
	}
	b.sendCtx, b.cancelSends = context.WithCancel(context.Background())
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

func (b *Batcher) run() {
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

func (b *Batcher) Enqueue(ev *eventsv1.Event) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		b.log.Warn("client closed; event dropped")
		b.deadLetter(ErrClosed, []*eventsv1.Event{ev})
		return
	}
	var dropped []*eventsv1.Event
	if len(b.buf) >= b.maxQueue {
		dropped = b.dropOldestLocked(len(b.buf) - b.maxQueue + 1)
	}
	b.buf = append(b.buf, ev)
	full := len(b.buf) >= b.maxSize && !b.backoff
	if !full {
		b.armLocked()
	}
	b.mu.Unlock()
	if dropped != nil {
		b.log.Warn("queue full; dropping oldest event")
		b.deadLetter(ErrQueueOverflow, dropped)
	}
	if full {
		b.signal()
	}
}

func (b *Batcher) Flush(ctx context.Context) error {
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

func (b *Batcher) Close(ctx context.Context) error {
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
		b.log.Error("client closed with undelivered events; dropped", "events", len(undelivered))
		b.deadLetter(fmt.Errorf("%w with %d undelivered event(s)", ErrClosed, len(undelivered)), undelivered)
	}
	close(b.closeDone)
	return err
}

// flushOnce stops at the first transient failure, since the rest would likely fail the same
// way. Open, the failed and unsent chunks are re-queued in order for a retry after MaxWait;
// closed, the failed chunk is dead-lettered and the rest left for close to report.
func (b *Batcher) flushOnce() {
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
			b.log.Error("permanent send failure; events dropped", "events", len(chunk), "err", err)
			b.deadLetter(err, chunk)
			continue
		}
		b.mu.Lock()
		closed := b.closed
		b.mu.Unlock()
		if closed {
			b.log.Error("client closed; events dropped after a transient send failure", "events", len(chunk), "err", err)
			b.deadLetter(err, chunk)
			b.requeue(slices.Concat(chunks[i+1:]...), false)
		} else {
			b.log.Warn("transient send failure; will retry", "events", len(chunk), "err", err)
			b.requeue(slices.Concat(chunks[i:]...), true)
		}
		return
	}
	b.mu.Lock()
	b.backoff = false
	b.mu.Unlock()
}

func (b *Batcher) split(batch []*eventsv1.Event) [][]*eventsv1.Event {
	var chunks [][]*eventsv1.Event
	start, size := 0, 0
	for i, ev := range batch {
		n := proto.Size(ev)
		if i > start && (i-start >= b.maxSize || size+n > maxBatchBytes) {
			chunks = append(chunks, batch[start:i:i])
			start, size = i, 0
		}
		size += n
	}
	return append(chunks, batch[start:])
}

func (b *Batcher) sendChunk(chunk []*eventsv1.Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("sending batch panicked: %v", r)
		}
	}()
	ctx, cancel := context.WithTimeout(b.sendCtx, b.timeout)
	defer cancel()
	return b.sink.Send(ctx, chunk)
}

func (b *Batcher) requeue(events []*eventsv1.Event, retry bool) {
	b.mu.Lock()
	b.buf = append(events, b.buf...)
	var dropped []*eventsv1.Event
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
		b.log.Warn("queue full; dropping oldest events", "events", len(dropped))
		b.deadLetter(ErrQueueOverflow, dropped)
	}
}

func (b *Batcher) dropOldestLocked(n int) []*eventsv1.Event {
	dropped := slices.Clone(b.buf[:n])
	clear(b.buf[:n])
	b.buf = b.buf[n:]
	return dropped
}

func (b *Batcher) armLocked() {
	if b.timer != nil || b.closed {
		return
	}
	b.timerGen++
	gen := b.timerGen
	b.timer = time.AfterFunc(b.maxWait, func() { b.timerFired(gen) })
}

func (b *Batcher) disarmLocked() {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
		b.timerGen++
	}
}

func (b *Batcher) timerFired(gen uint64) {
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

func (b *Batcher) signal() {
	select {
	case b.kick <- struct{}{}:
	default:
	}
}

// A panicking callback must not take down the sender, which would strand every later event.
func (b *Batcher) deadLetter(err error, events []*eventsv1.Event) {
	if b.onError == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			b.log.Error("OnError panicked; events may be lost", "events", len(events), "panic", r)
		}
	}()
	b.onError(err, events)
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
