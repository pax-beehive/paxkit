package reliablemq

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInboundReplayStopsAtFailedDispatchBeforeLaterFrames(t *testing.T) {
	for _, failHook := range []bool{false, true} {
		t.Run(map[bool]string{false: "dispatch", true: "hook"}[failHook], func(t *testing.T) {
			// Given a full replay batch whose first frame cannot be applied.
			store := &boundedReplayStore{spyStore: newSpyStore()}
			for seq := int64(1); seq <= 1010; seq++ {
				store.inboundReplay = append(store.inboundReplay, testFrame(seq, DirectionInbound, FrameKindData))
			}
			failure := errors.New("session not found")
			var dispatched []int64
			var opts []Option
			if failHook {
				opts = append(opts, WithInboundMiddleware(inboundErrorMiddleware(failure)))
			}
			engine := NewEngine(Config{}, store, nil, DispatcherFunc(func(_ context.Context, frame Frame) error {
				dispatched = append(dispatched, frame.Key.Seq)
				return failure
			}), opts...)

			// When replay encounters the failed frame.
			err := engine.ReplayInbound(context.Background(), "conn_1", StreamACP, 1000)

			// Then no later frame is dispatched and replay does not restart the batch.
			assert.ErrorIs(t, err, failure)
			assert.Equal(t, 1, store.listCalls)
			if failHook {
				assert.Empty(t, dispatched)
			} else {
				assert.Equal(t, []int64{1}, dispatched)
			}
			assert.NotContains(t, store.callSnapshot(), "MarkApplied:1")
		})
	}
}

func TestInboundReplayPreservesDispatchAndFailureRecordingErrors(t *testing.T) {
	// Given a dispatch failure and a separate failure to persist that error.
	dispatchErr, recordErr := errors.New("session missing"), errors.New("database unavailable")
	store := &boundedReplayStore{spyStore: newSpyStore(), recordErr: recordErr}
	store.inboundReplay = []Frame{testFrame(1, DirectionInbound, FrameKindData)}
	engine := NewEngine(Config{}, store, nil, DispatcherFunc(func(context.Context, Frame) error { return dispatchErr }))

	// When replay records the failure.
	err := engine.ReplayInbound(context.Background(), "conn_1", StreamACP, 1000)

	// Then both causes remain available to retry and diagnostic policies.
	assert.ErrorIs(t, err, dispatchErr)
	assert.ErrorIs(t, err, recordErr)
}

func TestInboundReplayUsesDurableWatermarkThroughProducerWriteBehind(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(map[bool]string{false: "single_wrapper", true: "nested_wrappers"}[nested], func(t *testing.T) {
			// Given a durable watermark ahead of a full batch of old failed rows.
			sink := &orderedReplayStore{boundedReplayStore: boundedReplayStore{spyStore: newSpyStore()}, appliedThrough: 13443}
			for seq := int64(1); seq <= 1010; seq++ {
				sink.inboundReplay = append(sink.inboundReplay, testFrame(seq, DirectionInbound, FrameKindData))
			}
			store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
			defer closeProducerWriteBehindStore(t, store)
			if nested {
				store = NewProducerWriteBehindStore(store, WithProducerWriteBehindManualFlush())
				defer closeProducerWriteBehindStore(t, store)
			}
			calls := 0
			engine := NewEngine(Config{}, store, nil, DispatcherFunc(func(context.Context, Frame) error {
				calls++
				return errors.New("must not dispatch stale rows")
			}))

			// When recovery reads through the write-behind wrapper.
			err := engine.ReplayInbound(context.Background(), "conn_1", StreamACP, 1000)

			// Then the durable inbound watermark is honored without reapplying history.
			require.NoError(t, err)
			assert.Equal(t, 0, calls)
			assert.Equal(t, 1, sink.stateLoads)
			assert.Equal(t, 1, sink.listCalls)
		})
	}
}

func TestInboundReplayKeepsLegacyStoreSemanticsThroughProducerWriteBehind(t *testing.T) {
	// Given an unordered legacy sink whose first replay frame is not sequence one.
	sink := &boundedReplayStore{spyStore: newSpyStore()}
	sink.inboundReplay = []Frame{testFrame(9, DirectionInbound, FrameKindData)}
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	var dispatched []int64
	engine := NewEngine(Config{}, store, nil, DispatcherFunc(func(_ context.Context, frame Frame) error {
		dispatched = append(dispatched, frame.Key.Seq)
		return nil
	}))

	// When replay uses the optional watermark capability.
	err := engine.ReplayInbound(context.Background(), "conn_1", StreamACP, 1000)

	// Then an unavailable watermark does not invent a gap before the first frame.
	require.NoError(t, err)
	assert.Equal(t, []int64{9}, dispatched)
}

func TestInboundReceiveUsesOrderedSinkThroughProducerWriteBehind(t *testing.T) {
	// Given a durable sink missing the first inbound sequence.
	sink := &orderedReplayStore{boundedReplayStore: boundedReplayStore{spyStore: newSpyStore()}}
	sink.inboundReplay = []Frame{testFrame(2, DirectionInbound, FrameKindData)}
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	producer := newEngineTestProducer(t, store)
	sink.stateLoads = 0
	dispatched := false
	engine := NewEngine(Config{}, store, producer, DispatcherFunc(func(context.Context, Frame) error {
		dispatched = true
		return nil
	}))

	// When sequence two arrives before sequence one.
	err := engine.Receive(context.Background(), EnvelopeFromFrame(testFrame(2, DirectionInbound, FrameKindData)))

	// Then it remains unapplied until the durable gap is filled.
	require.NoError(t, err)
	assert.False(t, dispatched)
	assert.Equal(t, 1, sink.stateLoads)
}

func TestInboundReplayReturnsDurableWatermarkReadError(t *testing.T) {
	// Given a wrapped sink whose watermark cannot be loaded.
	stateErr := errors.New("watermark read failed")
	sink := &orderedReplayStore{boundedReplayStore: boundedReplayStore{spyStore: newSpyStore()}, stateErr: stateErr}
	store := NewProducerWriteBehindStore(sink, WithProducerWriteBehindManualFlush())
	defer closeProducerWriteBehindStore(t, store)
	engine := NewEngine(Config{}, store, nil, DispatcherFunc(func(context.Context, Frame) error { return nil }))

	// When recovery tries to establish ordered progress.
	err := engine.ReplayInbound(context.Background(), "conn_1", StreamACP, 1000)

	// Then it stops without reading or dispatching journal rows.
	assert.ErrorIs(t, err, stateErr)
	assert.Zero(t, sink.listCalls)
}

type boundedReplayStore struct {
	*spyStore
	listCalls int
	recordErr error
}

func (s *boundedReplayStore) ListInboundReplay(_ context.Context, _ string, _ Stream, limit int) ([]Frame, error) {
	s.listCalls++
	if s.listCalls > 2 {
		return nil, errors.New("replay repeated unchanged full batch")
	}
	frames := s.inboundReplay
	if len(frames) > limit {
		frames = frames[:limit]
	}
	return frames, nil
}

func (s *boundedReplayStore) RecordDispatchFailure(ctx context.Context, key FrameKey, message string) error {
	_ = s.spyStore.RecordDispatchFailure(ctx, key, message)
	return s.recordErr
}

type orderedReplayStore struct {
	boundedReplayStore
	appliedThrough int64
	stateLoads     int
	stateErr       error
}

func (s *orderedReplayStore) LoadQueueState(context.Context, string, Stream) (QueueState, error) {
	s.stateLoads++
	return QueueState{NextOutboundSeq: 1, InboundAppliedThrough: s.appliedThrough}, s.stateErr
}
