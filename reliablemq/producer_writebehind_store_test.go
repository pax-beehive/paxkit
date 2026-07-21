package reliablemq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducerWriteBehindStoreBootstrapsSeqAndDefersOutboundPersistence(t *testing.T) {
	sink := newProducerWriteBehindSink()
	sink.queueState = QueueState{NextOutboundSeq: 42}
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)

	frame, err := store.AppendOutboundData(
		context.Background(),
		"queue_1",
		StreamACP,
		json.RawMessage(`{"n":1}`),
		Metadata{"agent_id": "agent_1"},
	)
	require.NoError(t, err)
	require.Equal(t, int64(42), frame.Key.Seq)
	require.NoError(t, store.MarkSent(context.Background(), frame.Key))

	assert.Equal(t, []string{"LoadQueueState:queue_1:acp"}, sink.calls)
	assert.Equal(t, 1, store.Stats().DirtyFrames)

	require.NoError(t, store.Flush(context.Background()))
	assert.Equal(t, []string{"LoadQueueState:queue_1:acp", "ApplyBatch:1:0"}, sink.calls)
	require.Len(t, sink.batches, 1)
	require.Len(t, sink.batches[0].Frames, 1)
	assert.Equal(t, int64(42), sink.batches[0].Frames[0].Key.Seq)
	assert.Equal(t, StatusSent, sink.batches[0].Frames[0].Status)
}

func TestProducerWriteBehindStoreKeepsInboundConsumerOperationsSynchronous(t *testing.T) {
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)

	inbound := Frame{
		Key:     FrameKey{QueueID: "queue_1", Stream: StreamACP, Seq: 1, Direction: DirectionInbound},
		Kind:    FrameKindData,
		Payload: json.RawMessage(`{"method":"initialize"}`),
		Status:  StatusReceived,
	}
	_, _, err := store.SaveInboundIfAbsent(context.Background(), inbound)
	require.NoError(t, err)
	require.NoError(t, store.MarkApplied(context.Background(), inbound.Key))

	assert.Equal(t, []string{"SaveInboundIfAbsent:1", "MarkApplied:1"}, sink.calls)
}

func TestProducerWriteBehindStorePassesConsumerCheckpointThrough(t *testing.T) {
	sink := newProducerWriteBehindSink()
	sink.consumerAckedThrough = 54
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)

	through, err := store.ConsumerAckedThrough(context.Background(), "queue_1", StreamACP)
	require.NoError(t, err)
	assert.Equal(t, int64(54), through)
	assert.Equal(t, []string{"ConsumerAckedThrough:queue_1:acp"}, sink.calls)
}

func TestProducerWriteBehindStoreBatchesAckThrough(t *testing.T) {
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)

	first, err := store.AppendOutboundData(context.Background(), "queue_1", StreamACP, json.RawMessage(`{"n":1}`), nil)
	require.NoError(t, err)
	_, err = store.AppendOutboundData(context.Background(), "queue_1", StreamACP, json.RawMessage(`{"n":2}`), nil)
	require.NoError(t, err)
	require.NoError(t, store.MarkSent(context.Background(), first.Key))
	require.NoError(t, store.AckOutboundThrough(context.Background(), "queue_1", StreamACP, 2))

	require.NoError(t, store.Flush(context.Background()))
	require.Len(t, sink.batches, 1)
	assert.Len(t, sink.batches[0].Frames, 2)
	require.Len(t, sink.batches[0].Patches, 1)
	assert.Equal(t, StatusAcked, sink.batches[0].Patches[0].Status)
	assert.Equal(t, int64(2), sink.batches[0].Patches[0].Key.Seq)
}

func TestProducerWriteBehindStoreCoalescesCumulativeAcksPerQueue(t *testing.T) {
	// Given
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	for i := 0; i < 3; i++ {
		_, err := store.AppendOutboundData(context.Background(), "queue_1", StreamACP, json.RawMessage(`{"n":1}`), nil)
		require.NoError(t, err)
	}

	// When
	require.NoError(t, store.AckOutboundThrough(context.Background(), "queue_1", StreamACP, 1))
	require.NoError(t, store.AckOutboundThrough(context.Background(), "queue_1", StreamACP, 3))
	require.NoError(t, store.AckOutboundThrough(context.Background(), "queue_1", StreamACP, 2))
	require.NoError(t, store.AckOutboundThrough(context.Background(), "queue_1", StreamACP, 3))
	require.NoError(t, store.Flush(context.Background()))

	// Then
	require.Len(t, sink.batches, 1)
	require.Len(t, sink.batches[0].Patches, 1)
	require.Equal(t, StatusAcked, sink.batches[0].Patches[0].Status)
	require.Equal(t, int64(3), sink.batches[0].Patches[0].Key.Seq)
}

func TestProducerWriteBehindStoreDoesNotBlockOutboundWhenFlushIsBlocked(t *testing.T) {
	sink := newBlockingProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)

	_, err := store.AppendOutboundData(context.Background(), "queue_1", StreamACP, json.RawMessage(`{"n":1}`), nil)
	require.NoError(t, err)

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- store.Flush(context.Background())
	}()
	<-sink.blocked

	returned := make(chan error, 1)
	go func() {
		_, err := store.AppendOutboundData(context.Background(), "queue_1", StreamACP, json.RawMessage(`{"n":2}`), nil)
		returned <- err
	}()

	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("AppendOutboundData blocked behind flush")
	}

	close(sink.release)
	require.NoError(t, <-flushDone)
	require.NoError(t, store.Flush(context.Background()))
	assert.Equal(t, []string{"ApplyBatch:1:0", "ApplyBatch:1:0"}, sink.batchCalls())
}

func TestProducerWriteBehindStoreRecordsFlushFailureWithoutLosingFrames(t *testing.T) {
	sink := newProducerWriteBehindSink()
	sink.applyErr = errors.New("db unavailable")
	var gotStats ProducerWriteBehindStats
	store := NewProducerWriteBehindStore(
		sink,
		WithProducerWriteBehindManualFlush(),
		WithProducerWriteBehindFlushFailureHandler(func(err error, stats ProducerWriteBehindStats) {
			gotStats = stats
		}),
	)
	defer closeProducerWriteBehindStore(t, store)

	_, err := store.AppendOutboundData(context.Background(), "queue_1", StreamACP, json.RawMessage(`{"n":1}`), nil)
	require.NoError(t, err)
	err = store.Flush(context.Background())
	require.Error(t, err)
	assert.True(t, store.Stats().Degraded)
	assert.Equal(t, 1, store.Stats().DirtyFrames)
	assert.Equal(t, 1, gotStats.DirtyFrames)

	sink.applyErr = nil
	require.NoError(t, store.Flush(context.Background()))
	assert.False(t, store.Stats().Degraded)
	assert.Equal(t, 0, store.Stats().DirtyFrames)
}

func TestProducerWriteBehindStoreMergesMemoryFramesIntoOutboundReplay(t *testing.T) {
	sink := newProducerWriteBehindSink()
	sink.outboundReplay = []Frame{outboundProducerFrame(1, StatusSent)}
	sink.queueState = QueueState{NextOutboundSeq: 2}
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)

	_, err := store.AppendOutboundData(context.Background(), "queue_1", StreamACP, json.RawMessage(`{"n":2}`), nil)
	require.NoError(t, err)

	frames, err := store.ListOutboundReplay(context.Background(), "queue_1", StreamACP, 10)
	require.NoError(t, err)
	require.Len(t, frames, 2)
	assert.Equal(t, int64(1), frames[0].Key.Seq)
	assert.Equal(t, int64(2), frames[1].Key.Seq)
}

func TestProducerWriteBehindStoreBuildsReconcileCheckpointFromSinkAndMemory(t *testing.T) {
	sink := newProducerWriteBehindSink()
	sink.queueState = QueueState{NextOutboundSeq: 2}
	sink.outboundReplay = []Frame{outboundProducerFrame(1, StatusSent)}
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)

	_, err := store.AppendOutboundData(context.Background(), "queue_1", StreamACP, json.RawMessage(`{"n":2}`), nil)
	require.NoError(t, err)

	checkpoint, err := store.LoadProducerReconcileCheckpoint(context.Background(), "queue_1", StreamACP)
	require.NoError(t, err)

	assert.Equal(t, "queue_1", checkpoint.QueueID)
	assert.Equal(t, StreamACP, checkpoint.Stream)
	assert.Equal(t, int64(3), checkpoint.ProducerNextSeq)
	assert.Equal(t, int64(1), checkpoint.ReplayFrom)
	assert.Equal(t, int64(2), checkpoint.ReplayThrough)
}

func TestProducerWriteBehindStoreAdvanceProducerNextSeqSkipsAlreadyAcceptedFrames(t *testing.T) {
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)

	first, err := store.AppendOutboundData(context.Background(), "queue_1", StreamACP, json.RawMessage(`{"n":1}`), nil)
	require.NoError(t, err)
	second, err := store.AppendOutboundData(context.Background(), "queue_1", StreamACP, json.RawMessage(`{"n":2}`), nil)
	require.NoError(t, err)

	require.NoError(t, store.AdvanceProducerNextSeq(context.Background(), "queue_1", StreamACP, 5))
	replay, err := store.ListOutboundReplay(context.Background(), "queue_1", StreamACP, 10)
	require.NoError(t, err)
	require.Empty(t, replay)

	next, err := store.AppendOutboundData(context.Background(), "queue_1", StreamACP, json.RawMessage(`{"n":5}`), nil)
	require.NoError(t, err)
	assert.Equal(t, int64(5), next.Key.Seq)
	require.NoError(t, store.Flush(context.Background()))
	require.Len(t, sink.batches, 1)
	assert.Equal(t, StatusAcked, sink.batches[0].Frames[0].Status)
	assert.Equal(t, StatusAcked, sink.batches[0].Frames[1].Status)
	assert.Equal(t, first.Key, sink.batches[0].Frames[0].Key)
	assert.Equal(t, second.Key, sink.batches[0].Frames[1].Key)
}

func TestProducerWriteBehindStoreFallsBackToSinkMethodsWithoutBatchStore(t *testing.T) {
	sink := newProducerWriteBehindSinkWithoutBatch()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)

	data, err := store.AppendOutboundData(
		context.Background(),
		"queue_1",
		StreamACP,
		json.RawMessage(`{"n":1}`),
		Metadata{"agent_id": "agent_1"},
	)
	require.NoError(t, err)
	require.NoError(t, store.UpdateMetadata(context.Background(), data.Key, Metadata{"phase": "sent"}))
	require.NoError(t, store.MarkSent(context.Background(), data.Key))

	tombstone, err := store.AppendOutboundTombstone(context.Background(), "queue_1", StreamACP, "blocked", nil)
	require.NoError(t, err)
	require.NoError(t, store.RecordSendFailure(context.Background(), tombstone.Key, "socket closed"))

	require.NoError(t, store.Flush(context.Background()))
	assert.Equal(t, []string{
		"LoadQueueState:queue_1:acp",
		"AppendOutboundData:queue_1:acp",
		"UpdateMetadata:1",
		"MarkSent:1",
		"AppendOutboundTombstone:queue_1:acp",
		"RecordSendFailure:2",
	}, sink.calls)
}

func TestProducerWriteBehindStorePassesInboundPatchOperationsThrough(t *testing.T) {
	sink := newProducerWriteBehindSink()
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)

	key := FrameKey{QueueID: "queue_1", Stream: StreamACP, Seq: 8, Direction: DirectionInbound}
	require.NoError(t, store.UpdateMetadata(context.Background(), key, Metadata{"handled": "yes"}))
	require.NoError(t, store.MarkRejected(context.Background(), key, "denied"))
	require.NoError(t, store.RecordDispatchFailure(context.Background(), key, "stdin closed"))

	assert.Equal(t, []string{"UpdateMetadata:8", "MarkRejected:8", "RecordDispatchFailure:8"}, sink.calls)
}

func TestProducerWriteBehindStoreRequiresBatchStoreWhenConfigured(t *testing.T) {
	sink := newProducerWriteBehindSinkWithoutBatch()
	store := NewProducerWriteBehindStore(
		sink,
		WithProducerWriteBehindManualFlush(),
		WithProducerWriteBehindRequireBatchStore(),
	)

	_, err := store.AppendOutboundData(context.Background(), "queue_1", StreamACP, json.RawMessage(`{"n":1}`), nil)
	require.NoError(t, err)
	err = store.Flush(context.Background())
	require.ErrorIs(t, err, ErrProducerBatchStoreRequired)
}

func TestProducerWriteBehindStoreBackgroundWorkerFlushesOutbound(t *testing.T) {
	sink := newProducerWriteBehindSink()
	now := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	store := NewProducerWriteBehindStore(
		sink,
		WithProducerWriteBehindFlushInterval(time.Millisecond),
		WithProducerWriteBehindClock(func() time.Time { return now }),
	)
	defer closeProducerWriteBehindStore(t, store)

	_, err := store.AppendOutboundData(context.Background(), "queue_1", StreamACP, json.RawMessage(`{"n":1}`), nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(sink.batchCalls()) > 0
	}, time.Second, time.Millisecond)
	assert.Equal(t, 0, store.Stats().DirtyFrames)
	assert.Equal(t, now, sink.batches[0].Frames[0].CreatedAt)
}

func outboundProducerFrame(seq int64, status Status) Frame {
	return Frame{
		Key: FrameKey{
			QueueID:   "queue_1",
			Stream:    StreamACP,
			Seq:       seq,
			Direction: DirectionOutbound,
		},
		Kind:    FrameKindData,
		Payload: json.RawMessage(`{"event":"delta"}`),
		Status:  status,
	}
}

func closeProducerWriteBehindStore(t *testing.T, store *ProducerWriteBehindStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, store.Close(ctx))
}

type producerWriteBehindSinkWithoutBatch struct {
	mu                   sync.Mutex
	calls                []string
	next                 int64
	queueState           QueueState
	consumerAckedThrough int64
	inboundFrames        map[FrameKey]Frame
	outboundReplay       []Frame
}

func newProducerWriteBehindSinkWithoutBatch() *producerWriteBehindSinkWithoutBatch {
	return &producerWriteBehindSinkWithoutBatch{
		next:          1,
		queueState:    QueueState{NextOutboundSeq: 1},
		inboundFrames: make(map[FrameKey]Frame),
	}
}

func (s *producerWriteBehindSinkWithoutBatch) LoadQueueState(
	ctx context.Context,
	queueID string,
	stream Stream,
) (QueueState, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "LoadQueueState:"+queueID+":"+string(stream))
	return s.queueState, nil
}

func (s *producerWriteBehindSinkWithoutBatch) AppendOutboundData(
	ctx context.Context,
	queueID string,
	stream Stream,
	payload json.RawMessage,
	metadata Metadata,
) (Frame, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "AppendOutboundData:"+queueID+":"+string(stream))
	frame := outboundProducerFrame(s.next, StatusPending)
	frame.Key.QueueID = queueID
	frame.Key.Stream = stream
	frame.Payload = append(json.RawMessage(nil), payload...)
	frame.Metadata = metadata.Clone()
	s.next++
	return frame, nil
}

func (s *producerWriteBehindSinkWithoutBatch) AppendOutboundTombstone(
	ctx context.Context,
	queueID string,
	stream Stream,
	errorMessage string,
	metadata Metadata,
) (Frame, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "AppendOutboundTombstone:"+queueID+":"+string(stream))
	frame := outboundProducerFrame(s.next, StatusPending)
	frame.Key.QueueID = queueID
	frame.Key.Stream = stream
	frame.Kind = FrameKindTombstone
	frame.Payload = nil
	frame.Metadata = metadata.Clone()
	frame.ErrorMessage = errorMessage
	s.next++
	return frame, nil
}

func (s *producerWriteBehindSinkWithoutBatch) SaveInboundIfAbsent(
	ctx context.Context,
	frame Frame,
) (bool, Frame, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "SaveInboundIfAbsent:"+itoa(frame.Key.Seq))
	if stored, ok := s.inboundFrames[frame.Key]; ok {
		return false, stored.Clone(), nil
	}
	stored := frame.Clone()
	s.inboundFrames[frame.Key] = stored
	for {
		next := s.consumerAckedThrough + 1
		key := FrameKey{
			QueueID:   frame.Key.QueueID,
			Stream:    frame.Key.Stream,
			Seq:       next,
			Direction: DirectionInbound,
		}
		if _, ok := s.inboundFrames[key]; !ok {
			break
		}
		s.consumerAckedThrough = next
	}
	return true, stored.Clone(), nil
}

func (s *producerWriteBehindSinkWithoutBatch) ListOutboundReplay(
	ctx context.Context,
	queueID string,
	stream Stream,
	limit int,
) ([]Frame, error) {
	_ = ctx
	_ = limit
	s.mu.Lock()
	defer s.mu.Unlock()
	frames := make([]Frame, 0, len(s.outboundReplay))
	for _, frame := range s.outboundReplay {
		if frame.Key.QueueID == queueID && frame.Key.Stream == stream {
			frames = append(frames, frame.Clone())
		}
	}
	return frames, nil
}

func (s *producerWriteBehindSinkWithoutBatch) ListInboundReplay(
	ctx context.Context,
	queueID string,
	stream Stream,
	limit int,
) ([]Frame, error) {
	_ = ctx
	_ = queueID
	_ = stream
	_ = limit
	return nil, nil
}

func (s *producerWriteBehindSinkWithoutBatch) ConsumerAckedThrough(
	ctx context.Context,
	queueID string,
	stream Stream,
) (int64, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "ConsumerAckedThrough:"+queueID+":"+string(stream))
	return s.consumerAckedThrough, nil
}

func (s *producerWriteBehindSinkWithoutBatch) MarkSent(ctx context.Context, key FrameKey) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "MarkSent:"+itoa(key.Seq))
	return nil
}

func (s *producerWriteBehindSinkWithoutBatch) AckOutboundThrough(
	ctx context.Context,
	queueID string,
	stream Stream,
	throughSeq int64,
) error {
	_ = ctx
	_ = queueID
	_ = stream
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "AckOutboundThrough:"+itoa(throughSeq))
	return nil
}

func (s *producerWriteBehindSinkWithoutBatch) MarkApplied(ctx context.Context, key FrameKey) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "MarkApplied:"+itoa(key.Seq))
	return nil
}

func (s *producerWriteBehindSinkWithoutBatch) MarkRejected(
	ctx context.Context,
	key FrameKey,
	errorMessage string,
) error {
	_ = ctx
	_ = errorMessage
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "MarkRejected:"+itoa(key.Seq))
	return nil
}

func (s *producerWriteBehindSinkWithoutBatch) RecordSendFailure(
	ctx context.Context,
	key FrameKey,
	errorMessage string,
) error {
	_ = ctx
	_ = errorMessage
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "RecordSendFailure:"+itoa(key.Seq))
	return nil
}

func (s *producerWriteBehindSinkWithoutBatch) RecordDispatchFailure(
	ctx context.Context,
	key FrameKey,
	errorMessage string,
) error {
	_ = ctx
	_ = errorMessage
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "RecordDispatchFailure:"+itoa(key.Seq))
	return nil
}

func (s *producerWriteBehindSinkWithoutBatch) UpdateMetadata(
	ctx context.Context,
	key FrameKey,
	metadata Metadata,
) error {
	_ = ctx
	_ = metadata
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "UpdateMetadata:"+itoa(key.Seq))
	return nil
}

type producerWriteBehindSink struct {
	*producerWriteBehindSinkWithoutBatch

	applyErr error
	batches  []StoreBatch
}

func newProducerWriteBehindSink() *producerWriteBehindSink {
	return &producerWriteBehindSink{
		producerWriteBehindSinkWithoutBatch: newProducerWriteBehindSinkWithoutBatch(),
	}
}

func (s *producerWriteBehindSink) ApplyBatch(ctx context.Context, batch StoreBatch) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "ApplyBatch:"+itoa(int64(len(batch.Frames)))+":"+itoa(int64(len(batch.Patches))))
	if s.applyErr != nil {
		return s.applyErr
	}
	s.batches = append(s.batches, cloneStoreBatch(batch))
	return nil
}

func (s *producerWriteBehindSink) batchCalls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var calls []string
	for _, call := range s.calls {
		if len(call) >= len("ApplyBatch") && call[:len("ApplyBatch")] == "ApplyBatch" {
			calls = append(calls, call)
		}
	}
	return calls
}

type blockingProducerWriteBehindSink struct {
	*producerWriteBehindSink

	blocked chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingProducerWriteBehindSink() *blockingProducerWriteBehindSink {
	return &blockingProducerWriteBehindSink{
		producerWriteBehindSink: newProducerWriteBehindSink(),
		blocked:                 make(chan struct{}),
		release:                 make(chan struct{}),
	}
}

func (s *blockingProducerWriteBehindSink) ApplyBatch(ctx context.Context, batch StoreBatch) error {
	s.once.Do(func() {
		close(s.blocked)
		<-s.release
	})
	return s.producerWriteBehindSink.ApplyBatch(ctx, batch)
}
