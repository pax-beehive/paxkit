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

func TestInboundReplayReportsUnsafeHistoryThroughProducerWriteBehind(t *testing.T) {
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

			// Then recovery fails closed instead of silently dropping unexecuted requests.
			require.ErrorIs(t, err, ErrInboundHistoryInconsistent)
			assert.Equal(t, 0, calls)
			assert.Equal(t, 1, sink.stateLoads)
			assert.Equal(t, 1, sink.listCalls)
		})
	}
}

func TestInboundReplayGivenStaleFullPageThenDoesNotHideLaterWorkAsSuccessfulRecovery(t *testing.T) {
	// Given stale received rows left behind by an older cursor that crossed failures.
	sink := &orderedReplayStore{boundedReplayStore: boundedReplayStore{spyStore: newSpyStore()}, appliedThrough: 1010}
	for seq := int64(1); seq <= 1011; seq++ {
		frame := testFrame(seq, DirectionInbound, FrameKindData)
		frame.Payload = []byte(`{"private":"do not include payload in diagnostics"}`)
		sink.inboundReplay = append(sink.inboundReplay, frame)
	}
	original := append([]Frame(nil), sink.inboundReplay...)
	dispatched := false
	engine := NewEngine(Config{}, sink, nil, DispatcherFunc(func(context.Context, Frame) error {
		dispatched = true
		return nil
	}))

	// When the stale prefix fills the entire first page ahead of live sequence 1011.
	err := engine.ReplayInbound(context.Background(), "conn_1", StreamACP, 1000)

	// Then the caller must reconcile the history before claiming recovery succeeded.
	require.ErrorIs(t, err, ErrInboundHistoryInconsistent)
	var detail *InboundHistoryError
	require.ErrorAs(t, err, &detail)
	assert.Equal(t, "conn_1", detail.QueueID)
	assert.Equal(t, StreamACP, detail.Stream)
	assert.Equal(t, int64(1), detail.Seq)
	assert.Equal(t, int64(1010), detail.AppliedThrough)
	assert.Contains(t, err.Error(), "preserve the journal")
	assert.False(t, dispatched)
	assert.Equal(t, 1, sink.listCalls)
	assert.Equal(t, original, sink.inboundReplay)
	assert.Empty(t, sink.callSnapshot())
	assert.NotContains(t, err.Error(), "do not include payload")
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

func TestInboundReceiveCheckpointErrorDoesNotSubmitACK(t *testing.T) {
	// Given an atomic journal guard rejects the inconsistent checkpoint.
	store := &checkpointErrorStore{spyStore: newSpyStore()}
	producer := newEngineTestProducer(t, store)
	store.resetCalls()
	beforeACK, beforeSignal := producer.inboundACK.Load(), producer.ackSignal.Load()
	engine := NewEngine(Config{}, store, producer, DispatcherFunc(func(context.Context, Frame) error {
		t.Fatal("an inconsistent checkpoint must not dispatch")
		return nil
	}))

	// When a peer retries a frame at or below the applied watermark.
	err := engine.Receive(context.Background(), EnvelopeFromFrame(testFrame(1, DirectionInbound, FrameKindData)))

	// Then receipt fails before the producer can submit any cumulative ACK.
	require.ErrorIs(t, err, ErrInboundHistoryInconsistent)
	assert.Equal(t, beforeACK, producer.inboundACK.Load())
	assert.Equal(t, beforeSignal, producer.ackSignal.Load())
	assert.Equal(t, []string{"SaveInboundIfAbsent:1"}, store.callSnapshot())
}

type checkpointErrorStore struct{ *spyStore }

func (s *checkpointErrorStore) SaveInboundIfAbsent(_ context.Context, frame Frame) (bool, Frame, error) {
	s.record("SaveInboundIfAbsent:" + itoa(frame.Key.Seq))
	return false, Frame{}, &InboundHistoryError{
		QueueID: frame.Key.QueueID, Stream: frame.Key.Stream,
		AppliedThrough: 2, AckedThrough: 0,
	}
}

func TestInboundReplayRechecksAcknowledgedGapBeforeReportingInconsistentHistory(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fresh     QueueState
		readErr   error
		wantErr   error
		wantCalls []int64
		wantPages int
	}{
		{name: "concurrent dispatcher advanced", fresh: QueueState{InboundAppliedThrough: 1, InboundAckedThrough: 2}, wantCalls: []int64{2}, wantPages: 2},
		{name: "receipt cursor was reconciled", fresh: QueueState{}, wantPages: 1},
		{name: "checkpoint read failed", readErr: errors.New("checkpoint unavailable"), wantPages: 1},
		{name: "checkpoint became inconsistent", fresh: QueueState{InboundAppliedThrough: 2, InboundAckedThrough: 1}, wantErr: ErrInboundHistoryInconsistent, wantPages: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a gap observed using an earlier queue-state snapshot.
			store := &changingReplayStore{
				orderedReplayStore: orderedReplayStore{boundedReplayStore: boundedReplayStore{spyStore: newSpyStore()}},
				fresh:              tc.fresh, readErr: tc.readErr,
			}
			store.inboundReplay = []Frame{testFrame(2, DirectionInbound, FrameKindData)}
			var dispatched []int64
			engine := NewEngine(Config{}, store, nil, DispatcherFunc(func(_ context.Context, frame Frame) error {
				dispatched = append(dispatched, frame.Key.Seq)
				return nil
			}))

			// When recovery reloads the checkpoint before classifying the gap.
			err := engine.ReplayInbound(context.Background(), "conn_1", StreamACP, 1000)

			// Then current durable state governs recovery without a false corruption error.
			if tc.readErr != nil {
				assert.ErrorIs(t, err, tc.readErr)
			} else if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantCalls, dispatched)
			assert.Equal(t, tc.wantPages, store.listCalls)
			assert.Equal(t, 2, store.stateLoads)
			assert.NotContains(t, store.callSnapshot(), "MarkApplied:1")
		})
	}
}

type changingReplayStore struct {
	orderedReplayStore
	fresh   QueueState
	readErr error
}

func TestInboundReplayRechecksEmptyTailAfterConcurrentDispatcherProgress(t *testing.T) {
	// Given another dispatcher completed the acknowledged tail after the first read.
	store := &changingReplayStore{
		orderedReplayStore: orderedReplayStore{boundedReplayStore: boundedReplayStore{spyStore: newSpyStore()}},
		fresh:              QueueState{InboundAppliedThrough: 2, InboundAckedThrough: 2},
	}
	engine := NewEngine(Config{}, store, nil, DispatcherFunc(func(context.Context, Frame) error {
		t.Fatal("concurrently applied frames must not dispatch again")
		return nil
	}))

	// When the received-row query observes the already completed history.
	err := engine.ReplayInbound(context.Background(), "conn_1", StreamACP, 1000)

	// Then a fresh checkpoint prevents a false missing-tail diagnosis.
	require.NoError(t, err)
	assert.Equal(t, 2, store.stateLoads)
	assert.Empty(t, store.callSnapshot())
}

func (s *changingReplayStore) LoadQueueState(context.Context, string, Stream) (QueueState, error) {
	s.stateLoads++
	if s.stateLoads == 1 {
		return QueueState{InboundAckedThrough: 2}, nil
	}
	return s.fresh, s.readErr
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
	return QueueState{NextOutboundSeq: 1, InboundAppliedThrough: s.appliedThrough, InboundAckedThrough: s.appliedThrough}, s.stateErr
}
