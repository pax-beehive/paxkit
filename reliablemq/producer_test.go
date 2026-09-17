package reliablemq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducerKeepsBindingWhenACKPrecedesWriteCompletion(t *testing.T) {
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducer(t, store)
	defer closeProducer(t, producer)
	sender := newBlockingFirstEnvelopeSender()
	binding, err := producer.Bind(context.Background(), sender, 0)
	require.NoError(t, err)
	defer binding.Close()
	engine := NewEngine(Config{}, store, producer, nil)

	require.NoError(t, engine.Send(context.Background(), outboundMessage(1)))
	select {
	case <-sender.blocked:
	case <-time.After(time.Second):
		t.Fatal("first write did not start")
	}
	// The peer can acknowledge bytes before the local writer reports success.
	require.NoError(t, engine.Receive(context.Background(), AckEnvelope("queue_1", StreamACP, 1)))
	require.Eventually(t, func() bool {
		return producer.Stats().AckedThrough == 1
	}, time.Second, time.Millisecond)
	require.NoError(t, engine.Send(context.Background(), outboundMessage(2)))
	close(sender.release)
	require.Eventually(t, func() bool {
		return sender.dataSeqsEqual([]int64{1, 2})
	}, time.Second, time.Millisecond)
	assert.True(t, producer.Stats().Bound)
	assert.Empty(t, producer.Stats().LastError)
}

func TestProducerSendDoesNotWaitForBlockedJournalOrSocket(t *testing.T) {
	// Given
	sink := newBlockingProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducer(t, store)
	defer closeProducer(t, producer)

	sender := newBlockingEnvelopeSender()
	binding, err := producer.Bind(context.Background(), sender, 0)
	require.NoError(t, err)
	defer binding.Close()
	engine := NewEngine(Config{}, store, producer, nil)

	require.NoError(t, engine.Send(context.Background(), outboundMessage(1)))
	<-sender.blocked
	require.Eventually(t, func() bool {
		return producer.Stats().Tail == 1
	}, time.Second, time.Millisecond)

	flushDone := make(chan error, 1)
	go func() { flushDone <- store.Flush(context.Background()) }()
	<-sink.blocked

	// When
	returned := make(chan error, 1)
	go func() { returned <- engine.Send(context.Background(), outboundMessage(2)) }()

	// Then
	select {
	case sendErr := <-returned:
		require.NoError(t, sendErr)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Send blocked behind journal or socket I/O")
	}
	checkpoint, err := producer.Checkpoint(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(3), checkpoint.ProducerNextSeq)

	close(sink.release)
	require.NoError(t, <-flushDone)
	close(sender.release)
}

func TestProducerConcurrentAcceptanceGetsUniqueContiguousSequence(t *testing.T) {
	// Given
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducer(t, store)
	defer closeProducer(t, producer)
	engine := NewEngine(Config{}, store, producer, nil)

	const count = 2500
	start := make(chan struct{})
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			errs <- engine.Send(context.Background(), outboundMessage(n))
		}(i)
	}

	// When
	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent producers blocked during acceptance")
	}
	close(errs)
	for sendErr := range errs {
		require.NoError(t, sendErr)
	}

	checkpoint, err := producer.Checkpoint(context.Background())
	require.NoError(t, err)

	// Then
	assert.Equal(t, int64(count+1), checkpoint.ProducerNextSeq)
	frames, err := store.ListOutboundReplay(context.Background(), "queue_1", StreamACP, count)
	require.NoError(t, err)
	require.Len(t, frames, count)
	for i, frame := range frames {
		assert.Equal(t, int64(i+1), frame.Key.Seq)
	}
}

func TestProducerNetworkCursorDoesNotWaitForJournalFlush(t *testing.T) {
	// Given
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducer(t, store)
	defer closeProducer(t, producer)
	sender := newRecordingEnvelopeSender()
	binding, err := producer.Bind(context.Background(), sender, 0)
	require.NoError(t, err)
	defer binding.Close()
	engine := NewEngine(Config{}, store, producer, nil)

	// When
	require.NoError(t, engine.Send(context.Background(), outboundMessage(1)))

	// Then
	require.Eventually(t, func() bool {
		return sender.dataSeqsEqual([]int64{1})
	}, time.Second, time.Millisecond)
	assert.Empty(t, sink.batchCalls())
}

func TestProducerEvictsPersistedHotFramesWhileDisconnected(t *testing.T) {
	// Given
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducer(t, store)
	defer closeProducer(t, producer)
	engine := NewEngine(Config{}, store, producer, nil)
	for i := 1; i <= 50; i++ {
		require.NoError(t, engine.Send(context.Background(), outboundMessage(i)))
	}
	checkpoint, err := producer.Checkpoint(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(51), checkpoint.ProducerNextSeq)

	// When
	require.NoError(t, store.Flush(context.Background()))

	// Then
	require.Eventually(t, func() bool {
		stats := producer.Stats()
		return stats.PersistedThrough == 50 && stats.HotFrames == 0
	}, time.Second, time.Millisecond)
}

func TestProducerFailsExplicitlyWhenUnpersistedBytesExceedLimit(t *testing.T) {
	// Given
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	failed := make(chan error, 1)
	producer := newTestProducerWithConfig(t, store, ProducerConfig{
		MaxUnpersistedBytes: 1,
		OnError: func(err error) {
			select {
			case failed <- err:
			default:
			}
		},
	})
	defer closeProducer(t, producer)
	engine := NewEngine(Config{}, store, producer, nil)

	// When
	require.NoError(t, engine.Send(context.Background(), outboundMessage(1)))

	// Then
	select {
	case err := <-failed:
		require.ErrorIs(t, err, ErrProducerJournalLimit)
	case <-time.After(time.Second):
		t.Fatal("producer did not report its journal safety limit")
	}
	require.ErrorIs(t, engine.Send(context.Background(), outboundMessage(2)), ErrProducerNotReady)
}

func TestProducerKeepsSendingWhenUnpersistedAgeExceedsLimit(t *testing.T) {
	// Given
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	var nowNanos atomic.Int64
	nowNanos.Store(time.Now().UnixNano())
	failed := make(chan error, 1)
	producer := newTestProducerWithConfig(t, store, ProducerConfig{
		MaintenanceEvery:  time.Millisecond,
		MaxUnpersistedAge: 10 * time.Millisecond,
		Now: func() time.Time {
			return time.Unix(0, nowNanos.Load())
		},
		OnError: func(err error) {
			select {
			case failed <- err:
			default:
			}
		},
	})
	defer closeProducer(t, producer)
	sender := newRecordingEnvelopeSender()
	binding, err := producer.Bind(context.Background(), sender, 0)
	require.NoError(t, err)
	defer binding.Close()
	engine := NewEngine(Config{}, store, producer, nil)
	require.NoError(t, engine.Send(context.Background(), outboundMessage(1)))
	require.Eventually(t, func() bool {
		return sender.dataSeqsEqual([]int64{1}) &&
			producer.Stats().UnpersistedBytes > 0
	}, time.Second, time.Millisecond)

	// When
	nowNanos.Add(int64(11 * time.Millisecond))

	// Then
	require.Eventually(t, func() bool {
		return producer.Stats().OldestUnpersistedAge > 10*time.Millisecond
	}, time.Second, time.Millisecond)
	require.Never(t, func() bool {
		select {
		case <-binding.Done():
			return true
		default:
			return false
		}
	}, 25*time.Millisecond, time.Millisecond)
	assert.True(t, producer.Stats().Ready)
	assert.True(t, producer.Stats().Bound)
	assert.Empty(t, failed)

	require.NoError(t, engine.Send(context.Background(), outboundMessage(2)))
	require.Eventually(t, func() bool {
		return sender.dataSeqsEqual([]int64{1, 2})
	}, time.Second, time.Millisecond)
	_, err = producer.Checkpoint(context.Background())
	require.NoError(t, err)
}

func TestProducerHeadFailurePreventsLaterFrameBypass(t *testing.T) {
	// Given
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducer(t, store)
	defer closeProducer(t, producer)
	engine := NewEngine(Config{}, store, producer, nil)
	require.NoError(t, engine.Send(context.Background(), outboundMessage(1)))
	require.NoError(t, engine.Send(context.Background(), outboundMessage(2)))
	_, err := producer.Checkpoint(context.Background())
	require.NoError(t, err)

	failing := newRecordingEnvelopeSender()
	failing.failSeq = 1
	binding, err := producer.Bind(context.Background(), failing, 0)
	require.NoError(t, err)

	// When
	err = binding.WaitCaughtUp(context.Background())

	// Then
	require.ErrorIs(t, err, ErrProducerDisconnected)
	assert.Equal(t, []int64{1}, failing.dataSeqs())

	recovered := newRecordingEnvelopeSender()
	rebound, err := producer.Bind(context.Background(), recovered, 0)
	require.NoError(t, err)
	defer rebound.Close()
	require.NoError(t, rebound.WaitCaughtUp(context.Background()))
	assert.Equal(t, []int64{1, 2}, recovered.dataSeqs())
}

func TestProducerBindingReportsRuntimeWriterFailure(t *testing.T) {
	// Given
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducer(t, store)
	defer closeProducer(t, producer)
	sender := newRecordingEnvelopeSender()
	sender.failSeq = 1
	binding, err := producer.Bind(context.Background(), sender, 0)
	require.NoError(t, err)
	defer binding.Close()
	require.NoError(t, binding.WaitCaughtUp(context.Background()))
	engine := NewEngine(Config{}, store, producer, nil)

	// When
	require.NoError(t, engine.Send(context.Background(), outboundMessage(1)))

	// Then
	select {
	case <-binding.Done():
	case <-time.After(time.Second):
		t.Fatal("binding did not report its runtime writer failure")
	}
	require.ErrorIs(t, binding.Err(), ErrProducerDisconnected)
	require.ErrorContains(t, binding.Err(), "socket failed")
	assert.False(t, producer.Stats().Bound)
}

func TestProducerBindingReportsRuntimeCursorFailure(t *testing.T) {
	// Given
	sink := newProducerWriteBehindSink()
	sink.queueState = QueueState{NextOutboundSeq: 3}
	sink.outboundReplay = []Frame{
		outboundProducerFrame(1, StatusPending),
		outboundProducerFrame(2, StatusPending),
	}
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducerWithConfig(t, store, ProducerConfig{CursorBatchSize: 1})
	defer closeProducer(t, producer)
	sender := newBlockingFirstEnvelopeSender()
	binding, err := producer.Bind(context.Background(), sender, 0)
	require.NoError(t, err)
	defer binding.Close()
	select {
	case <-sender.blocked:
	case <-time.After(time.Second):
		t.Fatal("producer did not start the first cursor frame")
	}

	// When
	sink.mu.Lock()
	sink.outboundReplay = sink.outboundReplay[:1]
	sink.mu.Unlock()
	close(sender.release)

	// Then
	select {
	case <-binding.Done():
	case <-time.After(time.Second):
		t.Fatal("binding did not report its runtime cursor failure")
	}
	require.ErrorIs(t, binding.Err(), ErrProducerDisconnected)
	require.ErrorIs(t, binding.Err(), ErrProducerJournalGap)
	assert.Equal(t, []int64{1}, sender.dataSeqs())
}

func TestProducerPersistsCumulativeACKWithoutMarkSentPatch(t *testing.T) {
	// Given
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducer(t, store)
	defer closeProducer(t, producer)
	sender := newRecordingEnvelopeSender()
	binding, err := producer.Bind(context.Background(), sender, 0)
	require.NoError(t, err)
	defer binding.Close()
	engine := NewEngine(Config{}, store, producer, nil)
	require.NoError(t, engine.Send(context.Background(), outboundMessage(1)))
	require.NoError(t, engine.Send(context.Background(), outboundMessage(2)))
	require.NoError(t, binding.WaitCaughtUp(context.Background()))

	// When
	require.NoError(t, engine.Receive(context.Background(), AckEnvelope("queue_1", StreamACP, 2)))
	require.Eventually(t, func() bool {
		return producer.Stats().AckedThrough == 2
	}, time.Second, time.Millisecond)
	require.NoError(t, store.Flush(context.Background()))

	// Then
	require.Len(t, sink.batches, 1)
	for _, frame := range sink.batches[0].Frames {
		assert.NotEqual(t, StatusSent, frame.Status)
	}
	require.Len(t, sink.batches[0].Patches, 1)
	assert.Equal(t, StatusAcked, sink.batches[0].Patches[0].Status)
	assert.Equal(t, int64(2), sink.batches[0].Patches[0].Key.Seq)
}

func TestProducerReplaysMoreThanCursorBatchInOrder(t *testing.T) {
	// Given
	const count = 1505
	sink := newProducerWriteBehindSink()
	sink.queueState = QueueState{NextOutboundSeq: count + 1}
	for seq := int64(1); seq <= count; seq++ {
		sink.outboundReplay = append(sink.outboundReplay, outboundProducerFrame(seq, StatusPending))
	}
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducerWithConfig(t, store, ProducerConfig{CursorBatchSize: 64})
	defer closeProducer(t, producer)
	sender := newRecordingEnvelopeSender()

	// When
	binding, err := producer.Bind(context.Background(), sender, 0)
	require.NoError(t, err)
	defer binding.Close()
	require.NoError(t, binding.WaitCaughtUp(context.Background()))

	// Then
	seqs := sender.dataSeqs()
	require.Len(t, seqs, count)
	for i, seq := range seqs {
		assert.Equal(t, int64(i+1), seq)
	}
}

func TestProducerDisconnectsWhenJournalCursorHasSequenceGap(t *testing.T) {
	// Given
	sink := newProducerWriteBehindSink()
	sink.queueState = QueueState{NextOutboundSeq: 4}
	sink.outboundReplay = []Frame{
		outboundProducerFrame(1, StatusPending),
		outboundProducerFrame(3, StatusPending),
	}
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducer(t, store)
	defer closeProducer(t, producer)
	sender := newRecordingEnvelopeSender()
	binding, err := producer.Bind(context.Background(), sender, 0)
	require.NoError(t, err)
	defer binding.Close()
	waitCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// When
	err = binding.WaitCaughtUp(waitCtx)

	// Then
	require.ErrorIs(t, err, ErrProducerDisconnected)
	assert.Empty(t, sender.dataSeqs())
}

func TestProducerLiveOutputCannotOvertakeReplayBacklog(t *testing.T) {
	// Given
	const backlog = 40
	sink := newProducerWriteBehindSink()
	sink.queueState = QueueState{NextOutboundSeq: backlog + 1}
	for seq := int64(1); seq <= backlog; seq++ {
		sink.outboundReplay = append(sink.outboundReplay, outboundProducerFrame(seq, StatusPending))
	}
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducerWithConfig(t, store, ProducerConfig{CursorBatchSize: 7})
	defer closeProducer(t, producer)
	sender := newBlockingFirstEnvelopeSender()
	binding, err := producer.Bind(context.Background(), sender, 0)
	require.NoError(t, err)
	defer binding.Close()
	<-sender.blocked

	engine := NewEngine(Config{}, store, producer, nil)
	require.NoError(t, engine.Send(context.Background(), outboundMessage(backlog+1)))
	checkpoint, err := producer.Checkpoint(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(backlog+2), checkpoint.ProducerNextSeq)

	// When
	close(sender.release)
	require.Eventually(t, func() bool {
		return len(sender.dataSeqs()) == backlog+1
	}, 3*time.Second, time.Millisecond)

	// Then
	seqs := sender.dataSeqs()
	for i, seq := range seqs {
		assert.Equal(t, int64(i+1), seq)
	}
}

func TestProducerAdvanceNextSequenceReconcilesPeerAheadBeforeAcceptance(t *testing.T) {
	// Given
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducer(t, store)
	defer closeProducer(t, producer)
	engine := NewEngine(Config{}, store, producer, nil)
	require.ErrorIs(t, producer.AdvanceProducerNextSeq(context.Background(), 0), ErrInvalidFrame)

	// When
	require.NoError(t, producer.AdvanceProducerNextSeq(context.Background(), 5))
	require.NoError(t, engine.Send(context.Background(), outboundMessage(1)))
	checkpoint, err := producer.Checkpoint(context.Background())
	require.NoError(t, err)

	// Then
	assert.Equal(t, int64(6), checkpoint.ProducerNextSeq)
	assert.Equal(t, int64(5), checkpoint.ReplayFrom)
	assert.Equal(t, int64(5), checkpoint.ReplayThrough)
	frames, err := store.ListOutboundReplay(context.Background(), "queue_1", StreamACP, 10)
	require.NoError(t, err)
	require.Len(t, frames, 1)
	assert.Equal(t, int64(5), frames[0].Key.Seq)
}

func TestProducerRejectsOperationsAfterClose(t *testing.T) {
	// Given
	store := NewProducerWriteBehindStore(
		newProducerWriteBehindSink(),
		WithProducerWriteBehindManualFlush(),
	)
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducer(t, store)
	engine := NewEngine(Config{}, store, producer, nil)
	require.NoError(t, producer.Close(nil))

	// When / Then
	require.ErrorIs(t, engine.Send(context.Background(), outboundMessage(1)), ErrProducerClosed)
	binding, err := producer.Bind(context.Background(), newRecordingEnvelopeSender(), 0)
	require.ErrorIs(t, err, ErrProducerClosed)
	require.Nil(t, binding)
	_, err = producer.Checkpoint(context.Background())
	require.ErrorIs(t, err, ErrProducerClosed)
	require.ErrorIs(t, producer.AdvanceProducerNextSeq(context.Background(), 2), ErrProducerClosed)
	require.ErrorIs(
		t,
		engine.Receive(context.Background(), AckEnvelope("queue_1", StreamACP, 1)),
		ErrProducerClosed,
	)
	require.NoError(t, producer.Close(context.Background()))
}

func TestProducerSerializesDataAndACKOnOneWriter(t *testing.T) {
	// Given
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newTestProducer(t, store)
	defer closeProducer(t, producer)
	sender := newRecordingEnvelopeSender()
	sender.delay = time.Millisecond
	binding, err := producer.Bind(context.Background(), sender, 0)
	require.NoError(t, err)
	defer binding.Close()
	engine := NewEngine(Config{}, store, producer, DispatcherFunc(func(context.Context, Frame) error { return nil }))

	// When
	for i := 1; i <= 30; i++ {
		require.NoError(t, engine.Send(context.Background(), outboundMessage(i)))
		require.NoError(t, engine.Receive(context.Background(), Envelope{
			Type:    EnvelopeTypeData,
			QueueID: "queue_1",
			Stream:  StreamACP,
			Seq:     int64(i),
			Payload: json.RawMessage(`{"inbound":true}`),
		}))
	}
	require.Eventually(t, func() bool {
		return len(sender.dataSeqs()) == 30
	}, 3*time.Second, time.Millisecond, "outbound data did not drain through the writer")
	require.Eventually(t, func() bool {
		return sender.hasACKThrough(30)
	}, 3*time.Second, time.Millisecond, "cumulative inbound ACK did not drain through the writer")

	// Then
	assert.Equal(t, 1, sender.maxConcurrent())
}

func TestProducerResendsCumulativeACKForDuplicateInbound(t *testing.T) {
	// Given
	store := newSpyStore()
	producer := newTestProducer(t, store)
	defer closeProducer(t, producer)
	sender := newRecordingEnvelopeSender()
	binding, err := producer.Bind(context.Background(), sender, 0)
	require.NoError(t, err)
	defer binding.Close()
	engine := NewEngine(
		Config{},
		store,
		producer,
		DispatcherFunc(func(context.Context, Frame) error { return nil }),
	)
	env := Envelope{
		Type:    EnvelopeTypeData,
		QueueID: "queue_1",
		Stream:  StreamACP,
		Seq:     1,
		Payload: json.RawMessage(`{"inbound":true}`),
	}
	require.NoError(t, engine.Receive(context.Background(), env))
	require.Eventually(t, func() bool {
		return len(sender.ackSeqs()) == 1
	}, time.Second, time.Millisecond)
	store.saveInboundInserted = false

	// When
	require.NoError(t, engine.Receive(context.Background(), env))

	// Then
	require.Eventually(t, func() bool {
		return len(sender.ackSeqs()) == 2
	}, time.Second, time.Millisecond)
	assert.Equal(t, []int64{1, 1}, sender.ackSeqs())
}

func newTestProducer(t *testing.T, store DurableStore) *Producer {
	t.Helper()
	return newTestProducerWithConfig(t, store, ProducerConfig{})
}

func newTestProducerWithConfig(t *testing.T, store DurableStore, config ProducerConfig) *Producer {
	t.Helper()
	config.QueueID = "queue_1"
	config.Stream = StreamACP
	producer, err := NewProducer(context.Background(), config, store)
	require.NoError(t, err)
	return producer
}

func closeProducer(t *testing.T, producer *Producer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, producer.Close(ctx))
}

func outboundMessage(n int) OutboundMessage {
	payload, _ := json.Marshal(map[string]int{"n": n})
	return OutboundMessage{QueueID: "queue_1", Stream: StreamACP, Payload: payload}
}

type recordingEnvelopeSender struct {
	mu          sync.Mutex
	envelopes   []Envelope
	failSeq     int64
	delay       time.Duration
	inFlight    int
	maxInFlight int
}

func newRecordingEnvelopeSender() *recordingEnvelopeSender {
	return &recordingEnvelopeSender{}
}

func (s *recordingEnvelopeSender) Send(ctx context.Context, env Envelope) error {
	s.mu.Lock()
	s.inFlight++
	if s.inFlight > s.maxInFlight {
		s.maxInFlight = s.inFlight
	}
	s.envelopes = append(s.envelopes, env)
	s.mu.Unlock()

	if s.delay > 0 {
		select {
		case <-ctx.Done():
		case <-time.After(s.delay):
		}
	}

	s.mu.Lock()
	s.inFlight--
	s.mu.Unlock()
	if env.Type == EnvelopeTypeData && env.Seq == s.failSeq {
		return errors.New("socket failed")
	}
	return nil
}

func (s *recordingEnvelopeSender) dataSeqs() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	seqs := make([]int64, 0, len(s.envelopes))
	for _, env := range s.envelopes {
		if env.Type == EnvelopeTypeData || env.Type == EnvelopeTypeTombstone {
			seqs = append(seqs, env.Seq)
		}
	}
	return seqs
}

func (s *recordingEnvelopeSender) ackSeqs() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	seqs := make([]int64, 0, len(s.envelopes))
	for _, env := range s.envelopes {
		if env.Type == EnvelopeTypeAck {
			seqs = append(seqs, env.Seq)
		}
	}
	return seqs
}

func (s *recordingEnvelopeSender) dataSeqsEqual(want []int64) bool {
	return assert.ObjectsAreEqual(want, s.dataSeqs())
}

func (s *recordingEnvelopeSender) hasACKThrough(through int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, env := range s.envelopes {
		if env.Type == EnvelopeTypeAck && env.Seq >= through {
			return true
		}
	}
	return false
}

func (s *recordingEnvelopeSender) maxConcurrent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxInFlight
}

type blockingEnvelopeSender struct {
	blocked chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingEnvelopeSender() *blockingEnvelopeSender {
	return &blockingEnvelopeSender{blocked: make(chan struct{}), release: make(chan struct{})}
}

func (s *blockingEnvelopeSender) Send(ctx context.Context, env Envelope) error {
	if env.Type != EnvelopeTypeData && env.Type != EnvelopeTypeTombstone {
		return nil
	}
	s.once.Do(func() { close(s.blocked) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return nil
	}
}

type blockingFirstEnvelopeSender struct {
	*recordingEnvelopeSender
	blocked chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingFirstEnvelopeSender() *blockingFirstEnvelopeSender {
	return &blockingFirstEnvelopeSender{
		recordingEnvelopeSender: newRecordingEnvelopeSender(),
		blocked:                 make(chan struct{}),
		release:                 make(chan struct{}),
	}
}

func (s *blockingFirstEnvelopeSender) Send(ctx context.Context, env Envelope) error {
	if env.Type == EnvelopeTypeData || env.Type == EnvelopeTypeTombstone {
		blocked := false
		s.once.Do(func() {
			blocked = true
			close(s.blocked)
		})
		if blocked {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.release:
			}
		}
	}
	return s.recordingEnvelopeSender.Send(ctx, env)
}
