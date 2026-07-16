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

func TestConsumerWriteBehindStorePassesProducerOperationsThroughSynchronously(t *testing.T) {
	sink := newConsumerWriteBehindSink()
	store := NewConsumerWriteBehindStore(sink, WithConsumerWriteBehindManualFlush())
	defer closeConsumerWriteBehindStore(t, store)

	frame, err := store.AppendOutboundData(
		context.Background(),
		"queue_1",
		StreamACP,
		json.RawMessage(`{"n":1}`),
		Metadata{"request_id": "req_1"},
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), frame.Key.Seq)
	require.NoError(t, store.MarkSent(context.Background(), frame.Key))
	require.NoError(t, store.AckOutboundThrough(context.Background(), "queue_1", StreamACP, 1))

	assert.Equal(t, []string{
		"AppendOutboundData:queue_1:acp",
		"MarkSent:1",
		"AckOutboundThrough:1",
	}, sink.calls)
}

func TestConsumerWriteBehindStoreAcceptsInboundWithoutTouchingSinkUntilFlush(t *testing.T) {
	sink := newConsumerWriteBehindSink()
	store := NewConsumerWriteBehindStore(sink, WithConsumerWriteBehindManualFlush())
	defer closeConsumerWriteBehindStore(t, store)

	frame := inboundConsumerFrame(7)
	inserted, stored, err := store.SaveInboundIfAbsent(context.Background(), frame)
	require.NoError(t, err)
	require.True(t, inserted)
	require.Equal(t, int64(7), stored.Key.Seq)
	require.NoError(t, store.UpdateMetadata(context.Background(), frame.Key, Metadata{"handled": "yes"}))
	require.NoError(t, store.MarkApplied(context.Background(), frame.Key))

	assert.Empty(t, sink.calls)
	stats := store.Stats()
	assert.Equal(t, 1, stats.DirtyFrames)
	assert.Equal(t, 0, stats.DirtyPatches)

	require.NoError(t, store.Flush(context.Background()))
	assert.Equal(t, []string{"ApplyBatch:1:0"}, sink.calls)
	require.Len(t, sink.batches, 1)
	require.Len(t, sink.batches[0].Frames, 1)
	flushed := sink.batches[0].Frames[0]
	assert.Equal(t, StatusApplied, flushed.Status)
	assert.Equal(t, Metadata{"handled": "yes"}, flushed.Metadata)
	assert.False(t, store.Stats().Degraded)
	assert.Equal(t, 0, store.Stats().DirtyFrames)
}

func TestConsumerWriteBehindStoreDeduplicatesInboundInMemory(t *testing.T) {
	sink := newConsumerWriteBehindSink()
	store := NewConsumerWriteBehindStore(sink, WithConsumerWriteBehindManualFlush())
	defer closeConsumerWriteBehindStore(t, store)

	frame := inboundConsumerFrame(3)
	inserted, stored, err := store.SaveInboundIfAbsent(context.Background(), frame)
	require.NoError(t, err)
	require.True(t, inserted)

	insertedAgain, duplicate, err := store.SaveInboundIfAbsent(context.Background(), frame)
	require.NoError(t, err)
	require.False(t, insertedAgain)
	assert.Equal(t, stored, duplicate)

	require.NoError(t, store.Flush(context.Background()))
	require.Len(t, sink.batches, 1)
	assert.Len(t, sink.batches[0].Frames, 1)
}

func TestConsumerWriteBehindStoreDoesNotBlockInboundWhenFlushIsBlocked(t *testing.T) {
	sink := newBlockingConsumerWriteBehindSink()
	store := NewConsumerWriteBehindStore(sink, WithConsumerWriteBehindManualFlush())
	defer closeConsumerWriteBehindStore(t, store)

	_, _, err := store.SaveInboundIfAbsent(context.Background(), inboundConsumerFrame(1))
	require.NoError(t, err)

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- store.Flush(context.Background())
	}()
	<-sink.blocked

	returned := make(chan error, 1)
	go func() {
		_, _, err := store.SaveInboundIfAbsent(context.Background(), inboundConsumerFrame(2))
		returned <- err
	}()

	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("SaveInboundIfAbsent blocked behind flush")
	}

	close(sink.release)
	require.NoError(t, <-flushDone)
	require.NoError(t, store.Flush(context.Background()))
	assert.Equal(t, []string{"ApplyBatch:1:0", "ApplyBatch:1:0"}, sink.calls)
}

func TestConsumerWriteBehindStoreRecordsFlushFailureWithoutLosingDirtyFrames(t *testing.T) {
	sink := newConsumerWriteBehindSink()
	sink.applyErr = errors.New("db unavailable")
	var gotStats ConsumerWriteBehindStats
	store := NewConsumerWriteBehindStore(
		sink,
		WithConsumerWriteBehindManualFlush(),
		WithConsumerWriteBehindFlushFailureHandler(func(err error, stats ConsumerWriteBehindStats) {
			gotStats = stats
		}),
	)
	defer closeConsumerWriteBehindStore(t, store)

	_, _, err := store.SaveInboundIfAbsent(context.Background(), inboundConsumerFrame(4))
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

func TestConsumerWriteBehindStoreFallsBackToSinkMethodsWithoutBatchStore(t *testing.T) {
	sink := newConsumerWriteBehindSinkWithoutBatch()
	store := NewConsumerWriteBehindStore(sink, WithConsumerWriteBehindManualFlush())
	defer closeConsumerWriteBehindStore(t, store)

	applied := inboundConsumerFrame(1)
	_, _, err := store.SaveInboundIfAbsent(context.Background(), applied)
	require.NoError(t, err)
	require.NoError(t, store.UpdateMetadata(context.Background(), applied.Key, Metadata{"phase": "applied"}))
	require.NoError(t, store.MarkApplied(context.Background(), applied.Key))

	rejected := inboundConsumerFrame(2)
	_, _, err = store.SaveInboundIfAbsent(context.Background(), rejected)
	require.NoError(t, err)
	require.NoError(t, store.MarkRejected(context.Background(), rejected.Key, "denied"))

	require.NoError(t, store.Flush(context.Background()))
	assert.Equal(t, []string{
		"SaveInboundIfAbsent:1",
		"UpdateMetadata:1",
		"MarkApplied:1",
		"SaveInboundIfAbsent:2",
		"MarkRejected:2",
	}, sink.calls)
}

func TestConsumerWriteBehindStoreFlushesPatchesAfterFrameWasFlushed(t *testing.T) {
	sink := newConsumerWriteBehindSinkWithoutBatch()
	store := NewConsumerWriteBehindStore(sink, WithConsumerWriteBehindManualFlush())
	defer closeConsumerWriteBehindStore(t, store)

	frame := inboundConsumerFrame(5)
	_, _, err := store.SaveInboundIfAbsent(context.Background(), frame)
	require.NoError(t, err)
	require.NoError(t, store.Flush(context.Background()))

	require.NoError(t, store.UpdateMetadata(context.Background(), frame.Key, Metadata{"phase": "patched"}))
	require.NoError(t, store.RecordDispatchFailure(context.Background(), frame.Key, "handler failed"))
	require.NoError(t, store.Flush(context.Background()))

	assert.Equal(t, []string{
		"SaveInboundIfAbsent:5",
		"UpdateMetadata:5",
		"RecordDispatchFailure:5",
	}, sink.calls)
}

func TestConsumerWriteBehindStorePassesOutboundErrorOperationsThrough(t *testing.T) {
	sink := newConsumerWriteBehindSink()
	store := NewConsumerWriteBehindStore(sink, WithConsumerWriteBehindManualFlush())
	defer closeConsumerWriteBehindStore(t, store)

	tombstone, err := store.AppendOutboundTombstone(
		context.Background(),
		"queue_1",
		StreamACP,
		"blocked",
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.RecordSendFailure(context.Background(), tombstone.Key, "socket closed"))

	assert.Equal(t, []string{
		"AppendOutboundTombstone:queue_1:acp",
		"RecordSendFailure:1",
	}, sink.calls)
}

func TestConsumerWriteBehindStoreMergesMemoryFramesIntoInboundReplay(t *testing.T) {
	sink := newConsumerWriteBehindSink()
	sink.inboundReplay = []Frame{inboundConsumerFrame(1)}
	store := NewConsumerWriteBehindStore(sink, WithConsumerWriteBehindManualFlush())
	defer closeConsumerWriteBehindStore(t, store)

	_, _, err := store.SaveInboundIfAbsent(context.Background(), inboundConsumerFrame(2))
	require.NoError(t, err)
	_, _, err = store.SaveInboundIfAbsent(context.Background(), inboundConsumerFrame(3))
	require.NoError(t, err)
	require.NoError(t, store.MarkApplied(context.Background(), inboundConsumerFrame(3).Key))

	frames, err := store.ListInboundReplay(context.Background(), "queue_1", StreamACP, 10)
	require.NoError(t, err)
	require.Len(t, frames, 2)
	assert.Equal(t, int64(1), frames[0].Key.Seq)
	assert.Equal(t, int64(2), frames[1].Key.Seq)
}

func TestConsumerWriteBehindStoreReportsAckedThroughFromSinkAndMemory(t *testing.T) {
	sink := newConsumerWriteBehindSink()
	sink.consumerAckedThrough = 1
	store := NewConsumerWriteBehindStore(sink, WithConsumerWriteBehindManualFlush())
	defer closeConsumerWriteBehindStore(t, store)

	_, _, err := store.SaveInboundIfAbsent(context.Background(), inboundConsumerFrame(2))
	require.NoError(t, err)
	_, _, err = store.SaveInboundIfAbsent(context.Background(), inboundConsumerFrame(4))
	require.NoError(t, err)

	through, err := store.ConsumerAckedThrough(context.Background(), "queue_1", StreamACP)
	require.NoError(t, err)
	assert.Equal(t, int64(2), through)
}

func TestConsumerWriteBehindStoreBackgroundWorkerFlushesInbound(t *testing.T) {
	sink := newConsumerWriteBehindSink()
	store := NewConsumerWriteBehindStore(
		sink,
		WithConsumerWriteBehindFlushInterval(time.Millisecond),
		WithConsumerWriteBehindClock(func() time.Time {
			return time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
		}),
	)
	defer closeConsumerWriteBehindStore(t, store)

	_, _, err := store.SaveInboundIfAbsent(context.Background(), inboundConsumerFrame(9))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(sink.batchCalls()) > 0
	}, time.Second, time.Millisecond)
	assert.Equal(t, 0, store.Stats().DirtyFrames)
	batches := sink.batchCalls()
	require.NotEmpty(t, batches)
	assert.Equal(t, time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC), batches[0].Frames[0].CreatedAt)
}

func TestConsumerWriteBehindStoreRejectsInvalidOrClosedInbound(t *testing.T) {
	sink := newConsumerWriteBehindSink()
	store := NewConsumerWriteBehindStore(sink, WithConsumerWriteBehindManualFlush())

	_, _, err := store.SaveInboundIfAbsent(context.Background(), Frame{})
	require.ErrorIs(t, err, ErrInvalidFrame)

	require.NoError(t, store.Close(context.Background()))
	_, _, err = store.SaveInboundIfAbsent(context.Background(), inboundConsumerFrame(1))
	require.ErrorIs(t, err, ErrConsumerStoreClosed)
}

func TestConsumerWriteBehindStoreRequiresBatchStoreWhenConfigured(t *testing.T) {
	sink := newConsumerWriteBehindSinkWithoutBatch()
	store := NewConsumerWriteBehindStore(
		sink,
		WithConsumerWriteBehindManualFlush(),
		WithConsumerWriteBehindRequireBatchStore(),
	)

	_, _, err := store.SaveInboundIfAbsent(context.Background(), inboundConsumerFrame(1))
	require.NoError(t, err)
	err = store.Flush(context.Background())
	require.ErrorIs(t, err, ErrConsumerBatchStoreRequired)
	assert.Empty(t, sink.calls)
}

func inboundConsumerFrame(seq int64) Frame {
	return Frame{
		Key: FrameKey{
			QueueID:   "queue_1",
			Stream:    StreamACP,
			Seq:       seq,
			Direction: DirectionInbound,
		},
		Kind:    FrameKindData,
		Payload: json.RawMessage(`{"event":"delta"}`),
		Status:  StatusReceived,
	}
}

func closeConsumerWriteBehindStore(t *testing.T, store *ConsumerWriteBehindStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, store.Close(ctx))
}

type consumerWriteBehindSinkWithoutBatch struct {
	mu    sync.Mutex
	calls []string
	next  int64
}

func newConsumerWriteBehindSinkWithoutBatch() *consumerWriteBehindSinkWithoutBatch {
	return &consumerWriteBehindSinkWithoutBatch{next: 1}
}

func (s *consumerWriteBehindSinkWithoutBatch) AppendOutboundData(
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
	frame := Frame{
		Key: FrameKey{
			QueueID:   queueID,
			Stream:    stream,
			Seq:       s.next,
			Direction: DirectionOutbound,
		},
		Kind:     FrameKindData,
		Payload:  append(json.RawMessage(nil), payload...),
		Metadata: metadata.Clone(),
		Status:   StatusPending,
	}
	s.next++
	return frame, nil
}

func (s *consumerWriteBehindSinkWithoutBatch) AppendOutboundTombstone(
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
	frame := Frame{
		Key: FrameKey{
			QueueID:   queueID,
			Stream:    stream,
			Seq:       s.next,
			Direction: DirectionOutbound,
		},
		Kind:         FrameKindTombstone,
		Metadata:     metadata.Clone(),
		Status:       StatusPending,
		ErrorMessage: errorMessage,
	}
	s.next++
	return frame, nil
}

func (s *consumerWriteBehindSinkWithoutBatch) SaveInboundIfAbsent(
	ctx context.Context,
	frame Frame,
) (bool, Frame, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "SaveInboundIfAbsent:"+itoa(frame.Key.Seq))
	return true, frame.Clone(), nil
}

func (s *consumerWriteBehindSinkWithoutBatch) ListOutboundReplay(
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

func (s *consumerWriteBehindSinkWithoutBatch) ListInboundReplay(
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

func (s *consumerWriteBehindSinkWithoutBatch) MarkSent(ctx context.Context, key FrameKey) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "MarkSent:"+itoa(key.Seq))
	return nil
}

func (s *consumerWriteBehindSinkWithoutBatch) AckOutboundThrough(
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

func (s *consumerWriteBehindSinkWithoutBatch) MarkApplied(ctx context.Context, key FrameKey) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "MarkApplied:"+itoa(key.Seq))
	return nil
}

func (s *consumerWriteBehindSinkWithoutBatch) MarkRejected(
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

func (s *consumerWriteBehindSinkWithoutBatch) RecordSendFailure(
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

func (s *consumerWriteBehindSinkWithoutBatch) RecordDispatchFailure(
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

func (s *consumerWriteBehindSinkWithoutBatch) UpdateMetadata(
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

type consumerWriteBehindSink struct {
	*consumerWriteBehindSinkWithoutBatch

	applyErr             error
	batches              []StoreBatch
	inboundReplay        []Frame
	consumerAckedThrough int64
}

func newConsumerWriteBehindSink() *consumerWriteBehindSink {
	return &consumerWriteBehindSink{
		consumerWriteBehindSinkWithoutBatch: newConsumerWriteBehindSinkWithoutBatch(),
	}
}

func (s *consumerWriteBehindSink) ApplyBatch(ctx context.Context, batch StoreBatch) error {
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

func (s *consumerWriteBehindSink) batchCalls() []StoreBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	batches := make([]StoreBatch, 0, len(s.batches))
	for _, batch := range s.batches {
		batches = append(batches, cloneStoreBatch(batch))
	}
	return batches
}

func (s *consumerWriteBehindSink) ListInboundReplay(
	ctx context.Context,
	queueID string,
	stream Stream,
	limit int,
) ([]Frame, error) {
	_ = ctx
	_ = limit
	s.mu.Lock()
	defer s.mu.Unlock()
	frames := make([]Frame, 0, len(s.inboundReplay))
	for _, frame := range s.inboundReplay {
		if frame.Key.QueueID == queueID && frame.Key.Stream == stream {
			frames = append(frames, frame.Clone())
		}
	}
	return frames, nil
}

func (s *consumerWriteBehindSink) ConsumerAckedThrough(
	ctx context.Context,
	queueID string,
	stream Stream,
) (int64, error) {
	_ = ctx
	_ = queueID
	_ = stream
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.consumerAckedThrough, nil
}

type blockingConsumerWriteBehindSink struct {
	*consumerWriteBehindSink

	blocked chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingConsumerWriteBehindSink() *blockingConsumerWriteBehindSink {
	return &blockingConsumerWriteBehindSink{
		consumerWriteBehindSink: newConsumerWriteBehindSink(),
		blocked:                 make(chan struct{}),
		release:                 make(chan struct{}),
	}
}

func (s *blockingConsumerWriteBehindSink) ApplyBatch(ctx context.Context, batch StoreBatch) error {
	s.once.Do(func() {
		close(s.blocked)
		<-s.release
	})
	return s.consumerWriteBehindSink.ApplyBatch(ctx, batch)
}

func cloneStoreBatch(batch StoreBatch) StoreBatch {
	cloned := StoreBatch{
		Frames:  make([]Frame, 0, len(batch.Frames)),
		Patches: make([]StorePatch, 0, len(batch.Patches)),
	}
	for _, frame := range batch.Frames {
		cloned.Frames = append(cloned.Frames, frame.Clone())
	}
	for _, patch := range batch.Patches {
		patch.Metadata = patch.Metadata.Clone()
		cloned.Patches = append(cloned.Patches, patch)
	}
	return cloned
}
