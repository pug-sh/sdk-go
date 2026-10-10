package pug

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/pug-sh/sdk-go/gen/common/v1"
	eventsv1 "github.com/pug-sh/sdk-go/gen/sdk/events/v1"
)

var errUnavailable = connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))

func ev(id int) *eventsv1.Event { return &eventsv1.Event{EventId: proto.String(strconv.Itoa(id))} }

func ids(events []*eventsv1.Event) string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.GetEventId()
	}
	return strings.Join(out, ",")
}

// fail, when set, decides each call's outcome before the batch counts as delivered.
type fakeSink struct {
	mu    sync.Mutex
	calls int
	sent  []string
	fail  func(ctx context.Context, call int, events []*eventsv1.Event) error
}

func (s *fakeSink) send(ctx context.Context, events []*eventsv1.Event) error {
	s.mu.Lock()
	s.calls++
	call, fail := s.calls, s.fail
	s.mu.Unlock()
	if fail != nil {
		if err := fail(ctx, call, events); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, ids(events))
	return nil
}

func (s *fakeSink) batches() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.sent)
}

func (s *fakeSink) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type deadLetter struct {
	err error
	ids string
}

type deadLetters struct {
	mu  sync.Mutex
	got []deadLetter
}

func (d *deadLetters) record(err error, events []*Event) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.got = append(d.got, deadLetter{err, ids(events)})
}

func (d *deadLetters) all() []deadLetter {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.got)
}

var smallBatch = BatchConfig{MaxSize: 3, MaxWait: 10 * time.Second, MaxQueueSize: 5}

func newTestBatcher(s sink, cfg BatchConfig) (*batcher, *deadLetters, *logs) {
	l, log := newLogs()
	dl := &deadLetters{}
	return newBatcher(s, cfg, dl.record, log, 10*time.Second), dl, l
}

func wantBatches(t *testing.T, s *fakeSink, want ...string) {
	t.Helper()
	if got := s.batches(); !slices.Equal(got, want) {
		t.Errorf("sent %q, want %q", got, want)
	}
}

func TestBatchFlushesAtMaxSize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &fakeSink{}
		b, _, _ := newTestBatcher(s, smallBatch)
		defer b.close(t.Context())
		b.enqueue(ev(1))
		b.enqueue(ev(2))
		synctest.Wait()
		wantBatches(t, s)
		b.enqueue(ev(3))
		synctest.Wait()
		wantBatches(t, s, "1,2,3")
	})
}

func TestBatchFlushSendsTheBuffer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &fakeSink{}
		b, _, _ := newTestBatcher(s, smallBatch)
		defer b.close(t.Context())
		b.enqueue(ev(1))
		if err := b.flush(t.Context()); err != nil {
			t.Fatal(err)
		}
		wantBatches(t, s, "1")
	})
}

func TestBatchDeadLettersPermanentErrors(t *testing.T) {
	for _, perm := range []error{errors.New("boom"), connect.NewError(connect.CodeInvalidArgument, errors.New("bad"))} {
		synctest.Test(t, func(t *testing.T) {
			s := &fakeSink{fail: func(context.Context, int, []*eventsv1.Event) error { return perm }}
			b, dl, l := newTestBatcher(s, smallBatch)
			defer b.close(t.Context())
			b.enqueue(ev(1))
			_ = b.flush(t.Context())
			if got := dl.all(); len(got) != 1 || got[0].ids != "1" || !errors.Is(got[0].err, perm) {
				t.Errorf("dead letters = %v", got)
			}
			l.find(t, slog.LevelError, "permanent send failure")
			_ = b.flush(t.Context())
			if n := s.callCount(); n != 1 {
				t.Errorf("sent %d times, want no retry", n)
			}
		})
	}
}

func TestBatchRetriesTransientErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &fakeSink{fail: func(_ context.Context, call int, _ []*eventsv1.Event) error {
			if call == 1 {
				return errUnavailable
			}
			return nil
		}}
		b, dl, l := newTestBatcher(s, BatchConfig{MaxSize: 99, MaxWait: 10 * time.Second})
		defer b.close(t.Context())
		b.enqueue(ev(1))
		_ = b.flush(t.Context())
		wantBatches(t, s)
		if got := dl.all(); len(got) != 0 {
			t.Errorf("dead letters = %v", got)
		}
		l.find(t, slog.LevelWarn, "transient send failure")
		_ = b.flush(t.Context())
		wantBatches(t, s, "1")
	})
}

func TestBatchDropsOldestOnOverflow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &fakeSink{}
		b, dl, l := newTestBatcher(s, BatchConfig{MaxSize: 99, MaxWait: 10 * time.Second, MaxQueueSize: 2})
		defer b.close(t.Context())
		b.enqueue(ev(1))
		b.enqueue(ev(2))
		b.enqueue(ev(3))
		if got := dl.all(); len(got) != 1 || got[0].ids != "1" || !errors.Is(got[0].err, ErrQueueOverflow) {
			t.Errorf("dead letters = %v", got)
		}
		l.find(t, slog.LevelWarn, "queue full")
		_ = b.flush(t.Context())
		wantBatches(t, s, "2,3")
	})
}

func TestBatchCloseDrainsAndRejectsLaterEvents(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &fakeSink{}
		b, dl, _ := newTestBatcher(s, smallBatch)
		b.enqueue(ev(1))
		if err := b.close(t.Context()); err != nil {
			t.Fatal(err)
		}
		wantBatches(t, s, "1")
		b.enqueue(ev(2))
		if got := dl.all(); len(got) != 1 || got[0].ids != "2" || !errors.Is(got[0].err, ErrClosed) {
			t.Errorf("dead letters = %v", got)
		}
	})
}

func TestBatchFlushesOnMaxWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &fakeSink{}
		b, _, _ := newTestBatcher(s, BatchConfig{MaxSize: 99, MaxWait: 50 * time.Millisecond})
		defer b.close(t.Context())
		b.enqueue(ev(1))
		time.Sleep(30 * time.Millisecond)
		b.enqueue(ev(2))
		synctest.Wait()
		wantBatches(t, s)
		// The wait runs from the first buffered event; later ones don't push it back.
		time.Sleep(20 * time.Millisecond)
		synctest.Wait()
		wantBatches(t, s, "1,2")
	})
}

func TestBatchSplitsIntoMaxSizeChunks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &fakeSink{fail: func(_ context.Context, call int, _ []*eventsv1.Event) error {
			if call == 1 {
				return errUnavailable
			}
			return nil
		}}
		b, _, _ := newTestBatcher(s, BatchConfig{MaxSize: 2, MaxWait: 10 * time.Second})
		defer b.close(t.Context())
		b.enqueue(ev(1))
		b.enqueue(ev(2))
		synctest.Wait()
		b.enqueue(ev(3))
		_ = b.flush(t.Context())
		wantBatches(t, s, "1,2", "3")
	})
}

func TestBatchSplitsByBytes(t *testing.T) {
	big := func(id int) *eventsv1.Event {
		e := ev(id)
		e.CustomProperties = map[string]*commonv1.PropertyValue{}
		value := strings.Repeat("x", maxStringBytes)
		for i := range 1500 {
			e.CustomProperties[fmt.Sprintf("p%04d", i)] = stringValue(logger{}, value)
		}
		return e
	}
	b := &batcher{maxSize: 100}
	chunks := b.split([]*eventsv1.Event{big(1), big(2), big(3), ev(4)})
	var got []string
	for _, c := range chunks {
		got = append(got, ids(c))
		size := 0
		for _, e := range c {
			size += proto.Size(e)
		}
		if size > maxBatchBytes {
			t.Errorf("chunk %s is %d bytes", ids(c), size)
		}
	}
	if want := []string{"1,2", "3,4"}; !slices.Equal(got, want) {
		t.Errorf("chunks = %q, want %q", got, want)
	}
}

func TestBatchInvalidConfigFallsBackWithWarning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &fakeSink{}
		b, _, l := newTestBatcher(s, BatchConfig{MaxSize: -1, MaxWait: -time.Second, MaxQueueSize: -5})
		defer b.close(t.Context())
		if b.maxSize != defaultMaxSize || b.maxWait != defaultMaxWait || b.maxQueue != defaultMaxQueueSize {
			t.Errorf("settings = %d %v %d", b.maxSize, b.maxWait, b.maxQueue)
		}
		if n := len(l.warns()); n != 3 {
			t.Errorf("got %d warnings, want 3: %v", n, l.records)
		}
		b.enqueue(ev(1))
		synctest.Wait()
		wantBatches(t, s)
		_ = b.flush(t.Context())
		wantBatches(t, s, "1")
	})
}

func TestBatchClampsMaxSize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, _, l := newTestBatcher(&fakeSink{}, BatchConfig{MaxSize: 5000})
		defer b.close(t.Context())
		if b.maxSize != maxBatchEvents {
			t.Errorf("maxSize = %d", b.maxSize)
		}
		l.find(t, slog.LevelWarn, "clamping")
	})
}

func TestBatchCloseDeadLettersTransientFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &fakeSink{fail: func(context.Context, int, []*eventsv1.Event) error { return errUnavailable }}
		b, dl, l := newTestBatcher(s, BatchConfig{MaxSize: 99, MaxWait: 10 * time.Second})
		b.enqueue(ev(1))
		if err := b.close(t.Context()); err != nil {
			t.Fatal(err)
		}
		if got := dl.all(); len(got) != 1 || got[0].ids != "1" || !errors.Is(got[0].err, errUnavailable) {
			t.Errorf("dead letters = %v", got)
		}
		l.find(t, slog.LevelError, "client closed")
	})
}

func TestBatchPreservesOrderAcrossFailedChunks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var failing sync.Mutex
		failing.Lock()
		down := true
		failing.Unlock()
		s := &fakeSink{fail: func(context.Context, int, []*eventsv1.Event) error {
			failing.Lock()
			defer failing.Unlock()
			if down {
				return errUnavailable
			}
			return nil
		}}
		b, _, _ := newTestBatcher(s, BatchConfig{MaxSize: 2, MaxWait: 10 * time.Second})
		defer b.close(t.Context())
		b.enqueue(ev(1))
		b.enqueue(ev(2))
		_ = b.flush(t.Context())
		b.enqueue(ev(3))
		_ = b.flush(t.Context())
		failing.Lock()
		down = false
		failing.Unlock()
		_ = b.flush(t.Context())
		wantBatches(t, s, "1,2", "3")
	})
}

// gatedSink holds every send until release says how it ends.
func gatedSink() (*fakeSink, chan error) {
	release := make(chan error)
	return &fakeSink{fail: func(ctx context.Context, _ int, _ []*eventsv1.Event) error {
		select {
		case err := <-release:
			return err
		case <-ctx.Done():
			return connect.NewError(connect.CodeCanceled, ctx.Err())
		}
	}}, release
}

func TestBatchStopsAtFirstTransientFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, release := gatedSink()
		b, dl, _ := newTestBatcher(s, BatchConfig{MaxSize: 2, MaxWait: 10 * time.Second})
		defer b.close(t.Context())
		b.enqueue(ev(1))
		b.enqueue(ev(2))
		synctest.Wait()
		b.enqueue(ev(3))
		b.enqueue(ev(4))
		b.enqueue(ev(5))
		release <- nil
		synctest.Wait()
		release <- errUnavailable
		synctest.Wait()
		if n := s.callCount(); n != 2 {
			t.Errorf("calls = %d; the chunk after a transient failure must not be tried", n)
		}
		go func() {
			release <- nil
			release <- nil
		}()
		_ = b.flush(t.Context())
		wantBatches(t, s, "1,2", "3,4", "5")
		if got := dl.all(); len(got) != 0 {
			t.Errorf("dead letters = %v", got)
		}
	})
}

func TestBatchPermanentFailureMovesOnToTheNextChunk(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, release := gatedSink()
		b, dl, _ := newTestBatcher(s, BatchConfig{MaxSize: 2, MaxWait: 10 * time.Second})
		defer b.close(t.Context())
		b.enqueue(ev(1))
		b.enqueue(ev(2))
		synctest.Wait()
		b.enqueue(ev(3))
		b.enqueue(ev(4))
		b.enqueue(ev(5))
		bad := connect.NewError(connect.CodePermissionDenied, errors.New("denied"))
		release <- nil
		synctest.Wait()
		release <- bad
		synctest.Wait()
		release <- nil
		synctest.Wait()
		wantBatches(t, s, "1,2", "5")
		if got := dl.all(); len(got) != 1 || got[0].ids != "3,4" || !errors.Is(got[0].err, bad) {
			t.Errorf("dead letters = %v", got)
		}
	})
}

func TestBatchBacksOffAfterTransientFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &fakeSink{fail: func(_ context.Context, call int, _ []*eventsv1.Event) error {
			if call == 1 {
				return errUnavailable
			}
			return nil
		}}
		b, _, _ := newTestBatcher(s, BatchConfig{MaxSize: 1, MaxWait: 5 * time.Second})
		defer b.close(t.Context())
		b.enqueue(ev(1))
		synctest.Wait()
		b.enqueue(ev(2))
		b.enqueue(ev(3))
		synctest.Wait()
		if n := s.callCount(); n != 1 {
			t.Fatalf("calls = %d; a size trigger during backoff must not retry at once", n)
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		wantBatches(t, s, "1", "2", "3")
	})
}

func TestBatchRequeueRespectsMaxQueueSize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, release := gatedSink()
		b, dl, _ := newTestBatcher(s, BatchConfig{MaxSize: 2, MaxWait: 10 * time.Second, MaxQueueSize: 3})
		defer b.close(t.Context())
		b.enqueue(ev(1))
		b.enqueue(ev(2))
		synctest.Wait()
		b.enqueue(ev(3))
		b.enqueue(ev(4))
		b.enqueue(ev(5))
		release <- errUnavailable
		synctest.Wait()
		if got := dl.all(); len(got) != 1 || got[0].ids != "1,2" || !errors.Is(got[0].err, ErrQueueOverflow) {
			t.Errorf("dead letters = %v", got)
		}
		go func() {
			release <- nil
			release <- nil
		}()
		_ = b.flush(t.Context())
		wantBatches(t, s, "3,4", "5")
	})
}

func TestBatchCloseWithExpiredContextReportsTheRest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _ := gatedSink()
		b, dl, _ := newTestBatcher(s, BatchConfig{MaxSize: 2, MaxWait: 10 * time.Second})
		b.enqueue(ev(1))
		b.enqueue(ev(2))
		synctest.Wait()
		b.enqueue(ev(3))
		b.enqueue(ev(4))
		b.enqueue(ev(5))
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := b.close(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("close = %v, want the deadline", err)
		}
		got := dl.all()
		if len(got) != 2 || got[0].ids != "1,2" || connect.CodeOf(got[0].err) != connect.CodeCanceled ||
			got[1].ids != "3,4,5" || !errors.Is(got[1].err, ErrClosed) {
			t.Errorf("dead letters = %v", got)
		}
	})
}

func TestBatchSendIsBoundedByTheTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _ := gatedSink()
		b, dl, _ := newTestBatcher(s, smallBatch)
		defer b.close(t.Context())
		b.enqueue(ev(1))
		start := time.Now()
		_ = b.flush(t.Context())
		if elapsed := time.Since(start); elapsed != 10*time.Second {
			t.Errorf("flush took %v, want the 10s send timeout", elapsed)
		}
		if got := dl.all(); len(got) != 0 {
			t.Errorf("dead letters = %v; a timed-out send is retried", got)
		}
	})
}

func TestBatchFlushHonorsItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, release := gatedSink()
		b, _, _ := newTestBatcher(s, smallBatch)
		defer b.close(t.Context())
		b.enqueue(ev(1))
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := b.flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("flush = %v", err)
		}
		release <- nil
		synctest.Wait()
		wantBatches(t, s, "1")
	})
}

func TestBatchFlushAndCloseAfterClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, _, _ := newTestBatcher(&fakeSink{}, smallBatch)
		if err := b.close(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := b.flush(t.Context()); err != nil {
			t.Errorf("flush after close = %v", err)
		}
		if err := b.close(t.Context()); err != nil {
			t.Errorf("second close = %v", err)
		}
	})
}

func TestBatchContainsPanickingOnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l, log := newLogs()
		b := newBatcher(&fakeSink{}, BatchConfig{MaxSize: 99, MaxQueueSize: 1}, func(error, []*Event) { panic("callback boom") }, log, time.Second)
		defer b.close(t.Context())
		b.enqueue(ev(1))
		b.enqueue(ev(2))
		l.find(t, slog.LevelError, "OnError panicked")
	})
}

func TestBatchRecoversFromAPanickingSink(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &fakeSink{fail: func(context.Context, int, []*eventsv1.Event) error { panic("sink boom") }}
		b, dl, _ := newTestBatcher(s, smallBatch)
		defer b.close(t.Context())
		b.enqueue(ev(1))
		_ = b.flush(t.Context())
		if got := dl.all(); len(got) != 1 || !strings.Contains(got[0].err.Error(), "sink boom") {
			t.Errorf("dead letters = %v", got)
		}
	})
}

// Every event must end up delivered or dead-lettered, exactly once, under concurrent
// enqueues, flushes, failures and a close. Run with -race.
func TestBatchAccountsForEveryEvent(t *testing.T) {
	s := &fakeSink{fail: func(context.Context, int, []*eventsv1.Event) error {
		time.Sleep(time.Duration(rand.IntN(200)) * time.Microsecond)
		switch n := rand.IntN(10); {
		case n < 2:
			return errUnavailable
		case n < 3:
			return connect.NewError(connect.CodeInvalidArgument, errors.New("bad"))
		}
		return nil
	}}
	l, log := newLogs()
	dl := &deadLetters{}
	b := newBatcher(s, BatchConfig{MaxSize: 50, MaxWait: time.Millisecond, MaxQueueSize: 500}, dl.record, log, time.Second)

	const producers, perProducer = 8, 500
	var wg sync.WaitGroup
	for p := range producers {
		wg.Go(func() {
			for i := range perProducer {
				b.enqueue(ev(p*perProducer + i))
			}
		})
	}
	stop := make(chan struct{})
	flushed := make(chan struct{})
	go func() {
		defer close(flushed)
		for {
			select {
			case <-stop:
				return
			default:
				_ = b.flush(context.Background())
			}
		}
	}()
	wg.Wait()
	close(stop)
	<-flushed
	if err := b.close(context.Background()); err != nil {
		t.Fatal(err)
	}

	seen := map[string]int{}
	for _, batch := range s.batches() {
		for id := range strings.SplitSeq(batch, ",") {
			seen[id]++
		}
	}
	for _, d := range dl.all() {
		for id := range strings.SplitSeq(d.ids, ",") {
			seen[id]++
		}
	}
	for i := range producers * perProducer {
		if n := seen[strconv.Itoa(i)]; n != 1 {
			t.Fatalf("event %d accounted for %d times", i, n)
		}
	}
	if len(s.batches()) == 0 || len(dl.all()) == 0 || len(l.warns()) == 0 {
		t.Errorf("want deliveries, dead letters and retries; got %d, %d, %d", len(s.batches()), len(dl.all()), len(l.warns()))
	}
}

// Close's flush is the final attempt: a Flush queued behind it must not send again, or Close
// would wait out those sends past its ctx. Queued requests are served in order, so Close's is
// queued first; the repeats cover the sender racing Close to the Flush.
func TestBatchCloseDoesNotSendAfterItsFlush(t *testing.T) {
	for range 20 {
		synctest.Test(t, func(t *testing.T) {
			release := make(chan error)
			s := &fakeSink{fail: func(ctx context.Context, call int, _ []*eventsv1.Event) error {
				switch call {
				case 1:
					return <-release
				case 2:
					return errUnavailable
				}
				<-ctx.Done()
				return connect.NewError(connect.CodeCanceled, ctx.Err())
			}}
			b, dl, _ := newTestBatcher(s, BatchConfig{MaxSize: 1, MaxWait: 10 * time.Second})
			b.enqueue(ev(1))
			synctest.Wait()
			b.enqueue(ev(2))
			b.enqueue(ev(3))
			b.enqueue(ev(4))
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			closed := make(chan error, 1)
			go func() { closed <- b.close(ctx) }()
			synctest.Wait()
			go func() { _ = b.flush(context.Background()) }()
			synctest.Wait()
			start := time.Now()
			release <- nil
			<-closed
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("close took %v, past its 1s ctx", elapsed)
			}
			seen := map[string]int{}
			for _, batch := range s.batches() {
				seen[batch]++
			}
			for _, d := range dl.all() {
				for id := range strings.SplitSeq(d.ids, ",") {
					seen[id]++
				}
			}
			for id := range 4 {
				if n := seen[strconv.Itoa(id+1)]; n != 1 {
					t.Fatalf("event %d accounted for %d times", id+1, n)
				}
			}
		})
	}
}

func TestBatchLogsEveryDropWithoutOnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &fakeSink{fail: func(context.Context, int, []*eventsv1.Event) error { return errUnavailable }}
		l, log := newLogs()
		b := newBatcher(s, BatchConfig{MaxSize: 1, MaxWait: 10 * time.Second}, nil, log, time.Second)
		b.enqueue(ev(1))
		synctest.Wait()
		b.enqueue(ev(2))
		b.enqueue(ev(3))
		if err := b.close(t.Context()); err != nil {
			t.Fatal(err)
		}
		if got := l.find(t, slog.LevelError, "events dropped after a transient send failure").attrs["events"]; got != int64(1) {
			t.Errorf("failed chunk logged with events=%v, want 1", got)
		}
		if got := l.find(t, slog.LevelError, "client closed with undelivered events").attrs["events"]; got != int64(2) {
			t.Errorf("leftovers logged with events=%v, want 2", got)
		}
		b.enqueue(ev(4))
		l.find(t, slog.LevelWarn, "client closed; event dropped")
	})
}

// A timer an enqueue armed during the failing send must not become the retry timer: the wait
// runs MaxWait from the failure.
func TestBatchRetryWaitsMaxWaitFromTheFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan error)
		s := &fakeSink{fail: func(_ context.Context, call int, _ []*eventsv1.Event) error {
			if call == 1 {
				return <-release
			}
			return nil
		}}
		b, _, _ := newTestBatcher(s, BatchConfig{MaxSize: 99, MaxWait: 2 * time.Second})
		defer b.close(t.Context())
		b.enqueue(ev(1))
		time.Sleep(2 * time.Second)
		synctest.Wait()
		time.Sleep(50 * time.Millisecond)
		b.enqueue(ev(2))
		time.Sleep(1850 * time.Millisecond)
		release <- errUnavailable
		synctest.Wait()
		time.Sleep(1900 * time.Millisecond)
		synctest.Wait()
		if n := s.callCount(); n != 1 {
			t.Fatalf("calls = %d, want the retry to wait the full 2s from the failure", n)
		}
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		wantBatches(t, s, "1,2")
	})
}
