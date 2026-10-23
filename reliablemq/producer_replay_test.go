package reliablemq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pax-beehive/paxkit/reliablemq"
	"github.com/pax-beehive/paxkit/reliablemq/sqlstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteBehindReplayKeepsFramesFlushedDuringRead(t *testing.T) {
	for _, test := range []struct {
		name   string
		cursor bool
		from   int64
		limit  int
		want   []int64
	}{
		{name: "cursor batch", cursor: true, from: 1, limit: 100, want: []int64{1, 2}},
		{name: "cursor exact head", cursor: true, from: 2, limit: 1, want: []int64{2}},
		{name: "fallback batch", from: 1, limit: 100, want: []int64{1, 2}},
		{name: "fallback exact head", from: 2, limit: 1, want: []int64{2}},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Given
			ctx := replayTestContext(t)
			store, gate := newReplayTestStore(t, test.cursor, false)
			appendReplayFrame(t, store, 1)
			require.NoError(t, store.Flush(ctx))
			appendReplayFrame(t, store, 2)
			gate.armed.Store(true)
			result := startReplayRead(ctx, store, test.from, test.limit)
			awaitReplayBarrier(t, ctx, gate)

			// When
			require.NoError(t, store.Flush(ctx))
			appendReplayFrame(t, store, 3)
			gate.release()

			// Then
			frames := awaitReplayRead(t, ctx, result)
			assert.Equal(t, test.want, replaySeqs(frames))
			next, err := store.ListOutboundReplayFrom(ctx, "queue_1", reliablemq.StreamACP, 3, 100)
			require.NoError(t, err)
			assert.Equal(t, []int64{3}, replaySeqs(next), "new arrivals remain available to the next read")
		})
	}
}

func TestWriteBehindReplayDoesNotResurrectACKedFrames(t *testing.T) {
	for _, test := range []struct {
		name    string
		through int64
		want    []int64
	}{
		{name: "persisted prefix", through: 1, want: []int64{2}},
		{name: "memory snapshot", through: 2, want: []int64{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Given
			ctx := replayTestContext(t)
			store, gate := newReplayTestStore(t, true, false)
			appendReplayFrame(t, store, 1)
			require.NoError(t, store.Flush(ctx))
			appendReplayFrame(t, store, 2)
			gate.armed.Store(true)
			result := startReplayRead(ctx, store, 1, 100)
			awaitReplayBarrier(t, ctx, gate)

			// When
			require.NoError(t, store.AckOutboundThrough(ctx, "queue_1", reliablemq.StreamACP, test.through))
			require.NoError(t, store.Flush(ctx))
			gate.release()

			// Then
			assert.Equal(t, test.want, replaySeqs(awaitReplayRead(t, ctx, result)))
		})
	}
}

func TestProducerKeepsBindingWhenACKAdvancesDuringReplay(t *testing.T) {
	for _, test := range []struct {
		name       string
		beforeRead bool
		through    int64
		want       []int64
	}{
		{name: "ACK before disk snapshot", beforeRead: true, through: 2, want: []int64{3}},
		{name: "ACK after disk snapshot", through: 2, want: []int64{3}},
		{name: "entire tail ACKed before snapshot", beforeRead: true, through: 3, want: []int64{}},
		{name: "entire tail ACKed after snapshot", through: 3, want: []int64{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Given
			ctx := replayTestContext(t)
			store, gate := newReplayTestStore(t, true, test.beforeRead)
			for n := 1; n <= 3; n++ {
				appendReplayFrame(t, store, n)
			}
			require.NoError(t, store.Flush(ctx))
			producer, err := reliablemq.NewProducer(ctx, reliablemq.ProducerConfig{
				QueueID: "queue_1", Stream: reliablemq.StreamACP, CursorBatchSize: 1,
			}, store)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, producer.Close(context.Background())) })
			var mu sync.Mutex
			sent := make([]int64, 0)
			sender := reliablemq.SenderFunc(func(_ context.Context, env reliablemq.Envelope) error {
				mu.Lock()
				defer mu.Unlock()
				sent = append(sent, env.Seq)
				return nil
			})
			gate.armed.Store(true)
			binding, err := producer.Bind(ctx, sender, 0)
			require.NoError(t, err)
			defer binding.Close()
			awaitReplayBarrier(t, ctx, gate)

			// When
			engine := reliablemq.NewEngine(reliablemq.Config{}, store, producer, nil)
			require.NoError(t, engine.Receive(ctx, reliablemq.AckEnvelope("queue_1", reliablemq.StreamACP, test.through)))
			require.Eventually(t, func() bool {
				return producer.Stats().AckedThrough == test.through
			}, time.Second, time.Millisecond)
			require.NoError(t, store.Flush(ctx))
			gate.release()

			// Then
			require.NoError(t, binding.WaitCaughtUp(ctx))
			payload := json.RawMessage(`{"n":4}`)
			require.NoError(t, engine.Send(ctx, reliablemq.OutboundMessage{
				QueueID: "queue_1", Stream: reliablemq.StreamACP, Payload: payload,
			}))
			require.Eventually(t, func() bool {
				stats := producer.Stats()
				return stats.NextToSend == 5 || !stats.Bound
			}, time.Second, time.Millisecond)
			assert.True(t, producer.Stats().Bound)
			mu.Lock()
			assert.Equal(t, append(test.want, 4), sent)
			mu.Unlock()
		})
	}
}

func TestProducerDiscardsReplayPageAcknowledgedDuringRead(t *testing.T) {
	// Given
	ctx := replayTestContext(t)
	store, gate := newReplayTestStore(t, true, false)
	for n := 1; n <= 3; n++ {
		appendReplayFrame(t, store, n)
	}
	require.NoError(t, store.Flush(ctx))
	// Read the journal directly so its captured page can outlive an ACK.
	producer, err := reliablemq.NewProducer(ctx, reliablemq.ProducerConfig{
		QueueID: "queue_1", Stream: reliablemq.StreamACP, CursorBatchSize: 2,
	}, gate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, producer.Close(context.Background())) })
	gate.armed.Store(true)
	sent := make(chan int64, 3)
	binding, err := producer.Bind(ctx, reliablemq.SenderFunc(func(_ context.Context, env reliablemq.Envelope) error {
		sent <- env.Seq
		return nil
	}), 0)
	require.NoError(t, err)
	defer binding.Close()
	awaitReplayBarrier(t, ctx, gate)

	// When
	engine := reliablemq.NewEngine(reliablemq.Config{}, gate, producer, nil)
	require.NoError(t, engine.Receive(ctx, reliablemq.AckEnvelope("queue_1", reliablemq.StreamACP, 2)))
	require.Eventually(t, func() bool {
		return producer.Stats().AckedThrough == 2
	}, time.Second, time.Millisecond)
	gate.release()

	// Then
	require.NoError(t, binding.WaitCaughtUp(ctx))
	assert.Equal(t, int64(0), producer.Stats().HotFrames, "an obsolete page must not restore acknowledged frames")
	assert.True(t, producer.Stats().Bound)
	select {
	case seq := <-sent:
		assert.Equal(t, int64(3), seq)
	default:
		t.Fatal("unacknowledged frame was not sent")
	}
	assert.Empty(t, sent)
}

type replayReadGate struct {
	*sqlstore.Store
	armed      atomic.Bool
	beforeRead bool
	once       sync.Once
	read       chan struct{}
	resume     chan struct{}
	release    func()
}

func (s *replayReadGate) pause(ctx context.Context) {
	if !s.armed.Load() {
		return
	}
	s.once.Do(func() {
		close(s.read)
		select {
		case <-s.resume:
		case <-ctx.Done():
		}
	})
}

func (s *replayReadGate) ListOutboundReplayFrom(ctx context.Context, queue string, stream reliablemq.Stream, from int64, limit int) ([]reliablemq.Frame, error) {
	if s.beforeRead {
		s.pause(ctx)
	}
	frames, err := s.Store.ListOutboundReplayFrom(ctx, queue, stream, from, limit)
	if !s.beforeRead {
		s.pause(ctx)
	}
	return frames, err
}

func (s *replayReadGate) ListOutboundReplay(ctx context.Context, queue string, stream reliablemq.Stream, limit int) ([]reliablemq.Frame, error) {
	if s.beforeRead {
		s.pause(ctx)
	}
	frames, err := s.Store.ListOutboundReplay(ctx, queue, stream, limit)
	if !s.beforeRead {
		s.pause(ctx)
	}
	return frames, err
}

type replaySinkWithoutCursor struct {
	reliablemq.DurableStore
	reliablemq.QueueStateStore
	reliablemq.BatchStore
}

func newReplayTestStore(t *testing.T, cursor, beforeRead bool) (*reliablemq.ProducerWriteBehindStore, *replayReadGate) {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	journal, err := sqlstore.NewSQLite(db)
	require.NoError(t, err)
	gate := &replayReadGate{
		Store: journal, beforeRead: beforeRead, read: make(chan struct{}), resume: make(chan struct{}),
	}
	gate.release = sync.OnceFunc(func() { close(gate.resume) })
	var sink reliablemq.DurableStore = gate
	if !cursor {
		sink = replaySinkWithoutCursor{DurableStore: gate, QueueStateStore: gate, BatchStore: gate}
	}
	store := reliablemq.NewProducerWriteBehindStore(sink,
		reliablemq.WithProducerWriteBehindManualFlush(),
		reliablemq.WithProducerWriteBehindRequireBatchStore(),
	)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	t.Cleanup(gate.release)
	return store, gate
}

func appendReplayFrame(t *testing.T, store *reliablemq.ProducerWriteBehindStore, n int) {
	t.Helper()
	payload, err := json.Marshal(map[string]int{"n": n})
	require.NoError(t, err)
	_, err = store.AppendOutboundData(context.Background(), "queue_1", reliablemq.StreamACP, payload, nil)
	require.NoError(t, err)
}

type replayReadResult struct {
	frames []reliablemq.Frame
	err    error
}

func startReplayRead(ctx context.Context, store *reliablemq.ProducerWriteBehindStore, from int64, limit int) <-chan replayReadResult {
	done := make(chan replayReadResult, 1)
	go func() {
		frames, err := store.ListOutboundReplayFrom(ctx, "queue_1", reliablemq.StreamACP, from, limit)
		done <- replayReadResult{frames: frames, err: err}
	}()
	return done
}

func awaitReplayRead(t *testing.T, ctx context.Context, done <-chan replayReadResult) []reliablemq.Frame {
	t.Helper()
	select {
	case result := <-done:
		require.NoError(t, result.err)
		return result.frames
	case <-ctx.Done():
		t.Fatal("replay did not finish")
		return nil
	}
}

func awaitReplayBarrier(t *testing.T, ctx context.Context, gate *replayReadGate) {
	t.Helper()
	select {
	case <-gate.read:
	case <-ctx.Done():
		t.Fatal("replay did not reach the read barrier")
	}
}

func replaySeqs(frames []reliablemq.Frame) []int64 {
	seqs := make([]int64, 0, len(frames))
	for _, frame := range frames {
		seqs = append(seqs, frame.Key.Seq)
	}
	return seqs
}

func replayTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
