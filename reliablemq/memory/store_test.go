package memory

import (
	"context"
	"testing"

	"github.com/pax-beehive/paxkit/reliablemq"
	"github.com/stretchr/testify/require"
)

func TestStoreOutboundSeqMonotonic(t *testing.T) {
	// Given
	store := New()
	ctx := context.Background()

	// When
	first, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	require.NoError(t, err)
	second, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	require.NoError(t, err)
	third, err := store.AppendOutboundTombstone(ctx, "conn_1", reliablemq.StreamACP, "blocked", nil)
	require.NoError(t, err)

	// Then
	require.Equal(t, int64(1), first.Key.Seq)
	require.Equal(t, int64(2), second.Key.Seq)
	require.Equal(t, int64(3), third.Key.Seq)
}

func TestStoreSeqScopesIndependent(t *testing.T) {
	// Given
	store := New()
	ctx := context.Background()

	// When
	queueOne, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	require.NoError(t, err)
	queueTwo, err := store.AppendOutboundData(ctx, "conn_2", reliablemq.StreamACP, []byte(`{}`), nil)
	require.NoError(t, err)
	control, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamControl, []byte(`{}`), nil)
	require.NoError(t, err)
	inserted, inbound, err := store.SaveInboundIfAbsent(ctx, reliablemq.Frame{
		Key:     reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 1, Direction: reliablemq.DirectionInbound},
		Kind:    reliablemq.FrameKindData,
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)

	// Then
	require.True(t, inserted)
	require.Equal(t, int64(1), queueOne.Key.Seq)
	require.Equal(t, int64(1), queueTwo.Key.Seq)
	require.Equal(t, int64(1), control.Key.Seq)
	require.Equal(t, int64(1), inbound.Key.Seq)
}

func TestStoreInboundDuplicate(t *testing.T) {
	// Given
	store := New()
	ctx := context.Background()
	original := reliablemq.Frame{
		Key:      reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 1, Direction: reliablemq.DirectionInbound},
		Kind:     reliablemq.FrameKindData,
		Payload:  []byte(`{"first":true}`),
		Metadata: reliablemq.Metadata{"source": "first"},
	}
	duplicate := reliablemq.Frame{
		Key:      original.Key,
		Kind:     reliablemq.FrameKindData,
		Payload:  []byte(`{"second":true}`),
		Metadata: reliablemq.Metadata{"source": "second"},
	}

	// When
	inserted, _, err := store.SaveInboundIfAbsent(ctx, original)
	require.NoError(t, err)
	insertedAgain, stored, err := store.SaveInboundIfAbsent(ctx, duplicate)
	require.NoError(t, err)

	// Then
	require.True(t, inserted)
	require.False(t, insertedAgain)
	require.JSONEq(t, string(original.Payload), string(stored.Payload))
	require.Equal(t, "first", stored.Metadata["source"])
}

func TestStoreInboundConsumerACKWatermarkAdvancesAcrossFilledGap(t *testing.T) {
	// Given
	store := New()
	ctx := context.Background()

	// When
	saveInboundFrame := func(seq int64) {
		t.Helper()
		inserted, _, err := store.SaveInboundIfAbsent(ctx, reliablemq.Frame{
			Key: reliablemq.FrameKey{
				QueueID:   "conn_1",
				Stream:    reliablemq.StreamACP,
				Seq:       seq,
				Direction: reliablemq.DirectionInbound,
			},
			Kind:    reliablemq.FrameKindData,
			Payload: []byte(`{}`),
		})
		require.NoError(t, err)
		require.True(t, inserted)
	}
	saveInboundFrame(1)
	saveInboundFrame(3)

	// Then
	through, err := store.ConsumerAckedThrough(ctx, "conn_1", reliablemq.StreamACP)
	require.NoError(t, err)
	require.Equal(t, int64(1), through)

	// When
	saveInboundFrame(2)

	// Then
	through, err = store.ConsumerAckedThrough(ctx, "conn_1", reliablemq.StreamACP)
	require.NoError(t, err)
	require.Equal(t, int64(3), through)
	state, err := store.LoadQueueState(ctx, "conn_1", reliablemq.StreamACP)
	require.NoError(t, err)
	require.Equal(t, int64(3), state.InboundAckedThrough)
}

func TestStoreInboundAppliedCursorDoesNotReplaceConsumerACKWatermark(t *testing.T) {
	// Given
	store := New()
	ctx := context.Background()
	inserted, frame, err := store.SaveInboundIfAbsent(ctx, reliablemq.Frame{
		Key: reliablemq.FrameKey{
			QueueID:   "conn_1",
			Stream:    reliablemq.StreamACP,
			Seq:       2,
			Direction: reliablemq.DirectionInbound,
		},
		Kind:    reliablemq.FrameKindData,
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	require.True(t, inserted)

	// When
	require.NoError(t, store.MarkApplied(ctx, frame.Key))

	// Then
	state, err := store.LoadQueueState(ctx, "conn_1", reliablemq.StreamACP)
	require.NoError(t, err)
	require.Equal(t, int64(2), state.InboundAppliedThrough)
	require.Zero(t, state.InboundAckedThrough)
	through, err := store.ConsumerAckedThrough(ctx, "conn_1", reliablemq.StreamACP)
	require.NoError(t, err)
	require.Zero(t, through)
}

func TestStoreAppliedSweptRetriesRebuildConsumerACKWithoutSkippingGap(t *testing.T) {
	// Given
	store := New()
	ctx := context.Background()
	key := queueKey{queueID: "conn_1", stream: reliablemq.StreamACP}
	store.inboundApplied[key] = 2
	save := func(seq int64) {
		t.Helper()
		inserted, stored, err := store.SaveInboundIfAbsent(ctx, reliablemq.Frame{
			Key: reliablemq.FrameKey{
				QueueID:   "conn_1",
				Stream:    reliablemq.StreamACP,
				Seq:       seq,
				Direction: reliablemq.DirectionInbound,
			},
			Kind:    reliablemq.FrameKindData,
			Payload: []byte(`{}`),
		})
		require.NoError(t, err)
		require.False(t, inserted)
		require.Equal(t, reliablemq.StatusApplied, stored.Status)
	}

	// When / Then
	save(2)
	through, err := store.ConsumerAckedThrough(ctx, "conn_1", reliablemq.StreamACP)
	require.NoError(t, err)
	require.Zero(t, through)

	save(1)
	through, err = store.ConsumerAckedThrough(ctx, "conn_1", reliablemq.StreamACP)
	require.NoError(t, err)
	require.Equal(t, int64(1), through)

	save(2)
	through, err = store.ConsumerAckedThrough(ctx, "conn_1", reliablemq.StreamACP)
	require.NoError(t, err)
	require.Equal(t, int64(2), through)
}

func TestStoreCumulativeACK(t *testing.T) {
	// Given
	store := New()
	ctx := context.Background()
	first, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	second, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	third, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)

	// When
	require.NoError(t, store.AckOutboundThrough(ctx, "conn_1", reliablemq.StreamACP, 2))

	// Then
	gotFirst, _ := store.Get(first.Key)
	gotSecond, _ := store.Get(second.Key)
	gotThird, _ := store.Get(third.Key)
	require.Equal(t, reliablemq.StatusAcked, gotFirst.Status)
	require.Equal(t, reliablemq.StatusAcked, gotSecond.Status)
	require.NotEqual(t, reliablemq.StatusAcked, gotThird.Status)
}

func TestStoreReplayFilters(t *testing.T) {
	// Given
	store := New()
	ctx := context.Background()
	pending, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	sent, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	acked, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	_ = store.MarkSent(ctx, sent.Key)
	_ = store.AckOutboundThrough(ctx, "conn_1", reliablemq.StreamACP, acked.Key.Seq)
	_ = store.MarkRejected(ctx, pending.Key, "not actually pending anymore")

	received := reliablemq.Frame{Key: reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 1, Direction: reliablemq.DirectionInbound}, Kind: reliablemq.FrameKindData, Payload: []byte(`{}`)}
	applied := reliablemq.Frame{Key: reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 2, Direction: reliablemq.DirectionInbound}, Kind: reliablemq.FrameKindData, Payload: []byte(`{}`)}
	rejected := reliablemq.Frame{Key: reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 3, Direction: reliablemq.DirectionInbound}, Kind: reliablemq.FrameKindData, Payload: []byte(`{}`)}
	_, receivedStored, _ := store.SaveInboundIfAbsent(ctx, received)
	_, appliedStored, _ := store.SaveInboundIfAbsent(ctx, applied)
	_, rejectedStored, _ := store.SaveInboundIfAbsent(ctx, rejected)
	_ = store.MarkApplied(ctx, appliedStored.Key)
	_ = store.MarkRejected(ctx, rejectedStored.Key, "denied")

	// When
	outbound, err := store.ListOutboundReplay(ctx, "conn_1", reliablemq.StreamACP, 100)
	require.NoError(t, err)
	inbound, err := store.ListInboundReplay(ctx, "conn_1", reliablemq.StreamACP, 100)
	require.NoError(t, err)

	// Then
	require.Empty(t, outbound)
	require.Len(t, inbound, 1)
	require.Equal(t, receivedStored.Key, inbound[0].Key)
}

func TestStoreFailureAndMetadataUpdates(t *testing.T) {
	// Given
	store := New()
	ctx := context.Background()
	outbound, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), reliablemq.Metadata{"before": "true"})
	require.NoError(t, err)
	inboundFrame := reliablemq.Frame{
		Key:     reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 1, Direction: reliablemq.DirectionInbound},
		Kind:    reliablemq.FrameKindData,
		Payload: []byte(`{}`),
	}
	_, inbound, err := store.SaveInboundIfAbsent(ctx, inboundFrame)
	require.NoError(t, err)

	// When
	require.NoError(t, store.RecordSendFailure(ctx, outbound.Key, "socket closed"))
	require.NoError(t, store.RecordDispatchFailure(ctx, inbound.Key, "stdin closed"))
	require.NoError(t, store.UpdateMetadata(ctx, inbound.Key, reliablemq.Metadata{"after": "true"}))

	// Then
	gotOutbound, _ := store.Get(outbound.Key)
	gotInbound, _ := store.Get(inbound.Key)
	require.Equal(t, reliablemq.StatusPending, gotOutbound.Status)
	require.Equal(t, "socket closed", gotOutbound.ErrorMessage)
	require.Equal(t, reliablemq.StatusReceived, gotInbound.Status)
	require.Equal(t, "stdin closed", gotInbound.ErrorMessage)
	require.Equal(t, "true", gotInbound.Metadata["after"])
}

func TestStoreInvalidOperations(t *testing.T) {
	// Given
	store := New()
	ctx := context.Background()
	missing := reliablemq.FrameKey{QueueID: "missing", Stream: reliablemq.StreamACP, Seq: 1, Direction: reliablemq.DirectionOutbound}

	// When / Then
	_, err := store.AppendOutboundData(ctx, "", reliablemq.StreamACP, []byte(`{}`), nil)
	require.Error(t, err)
	_, err = store.AppendOutboundData(ctx, "conn_1", "", []byte(`{}`), nil)
	require.Error(t, err)
	_, err = store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{`), nil)
	require.Error(t, err)
	_, _, err = store.SaveInboundIfAbsent(ctx, reliablemq.Frame{Key: reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 1, Direction: reliablemq.DirectionOutbound}, Kind: reliablemq.FrameKindData, Payload: []byte(`{}`)})
	require.Error(t, err)
	require.Error(t, store.MarkSent(ctx, missing))
}
