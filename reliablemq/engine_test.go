package reliablemq

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEngineSendDataOrdering(t *testing.T) {
	// Given
	store := newSpyStore()
	sender := newSpySender(store)
	engine := NewEngine(Config{}, store, sender, nil, WithOutboundMiddleware(
		func(next OutboundHandler) OutboundHandler {
			return func(ctx context.Context, msg *OutboundMessage) (OutboundDecision, error) {
				store.record("BeforeSend")
				return next(ctx, msg)
			}
		},
	))

	// When
	frame, err := engine.Send(context.Background(), OutboundMessage{
		QueueID:  "conn_1",
		Stream:   StreamACP,
		Payload:  json.RawMessage(`{"ok":true}`),
		Metadata: Metadata{"agent_id": "agent_1"},
	})
	require.NoError(t, err)

	// Then
	wantOrder := []string{"BeforeSend", "AppendOutboundData", "Sender.Send:data:1", "MarkSent:1"}
	require.Equal(t, wantOrder, store.calls)
	require.Equal(t, FrameKindData, frame.Kind)
	require.Equal(t, StatusPending, frame.Status)
}

func TestEngineSendTombstone(t *testing.T) {
	// Given
	store := newSpyStore()
	sender := newSpySender(store)
	engine := NewEngine(Config{}, store, sender, nil, WithOutboundMiddleware(outboundDecisionMiddleware(OutboundDecision{
		Action:       OutboundTombstone,
		ErrorMessage: "blocked",
	})))

	// When
	frame, err := engine.Send(context.Background(), OutboundMessage{
		QueueID: "conn_1",
		Stream:  StreamACP,
		Payload: json.RawMessage(`{"ok":true}`),
	})
	require.NoError(t, err)

	// Then
	wantOrder := []string{"AppendOutboundTombstone", "Sender.Send:tombstone:1", "MarkSent:1"}
	require.Equal(t, wantOrder, store.calls)
	require.Equal(t, FrameKindTombstone, frame.Kind)
	require.Equal(t, "blocked", frame.ErrorMessage)
	require.Empty(t, sender.sent[0].Payload)
	require.Equal(t, "blocked", sender.sent[0].ErrorMessage)
}

func TestEngineSendHookErrorTombstone(t *testing.T) {
	// Given
	store := newSpyStore()
	sender := newSpySender(store)
	hookErr := errors.New("hook unavailable")
	engine := NewEngine(Config{OutboundHookErrorPolicy: OutboundHookErrorTombstone}, store, sender, nil, WithOutboundMiddleware(outboundErrorMiddleware(hookErr)))

	// When
	frame, err := engine.Send(context.Background(), OutboundMessage{
		QueueID: "conn_1",
		Stream:  StreamACP,
		Payload: json.RawMessage(`{"ok":true}`),
	})
	require.NoError(t, err)

	// Then
	require.Equal(t, FrameKindTombstone, frame.Kind)
	require.Equal(t, hookErr.Error(), frame.ErrorMessage)
}

func TestEngineSendNetworkFailure(t *testing.T) {
	// Given
	store := newSpyStore()
	sender := newSpySender(store)
	sender.err = errors.New("socket closed")
	engine := NewEngine(Config{}, store, sender, nil)

	// When
	_, err := engine.Send(context.Background(), OutboundMessage{QueueID: "conn_1", Stream: StreamACP, Payload: json.RawMessage(`{}`)})

	// Then
	require.ErrorIs(t, err, sender.err)
	wantOrder := []string{"AppendOutboundData", "Sender.Send:data:1", "RecordSendFailure:1:socket closed"}
	require.Equal(t, wantOrder, store.calls)
}

func TestEngineSendMarkSentFailure(t *testing.T) {
	// Given
	store := newSpyStore()
	store.markSentErr = errors.New("db unavailable")
	sender := newSpySender(store)
	engine := NewEngine(Config{}, store, sender, nil)

	// When
	_, err := engine.Send(context.Background(), OutboundMessage{QueueID: "conn_1", Stream: StreamACP, Payload: json.RawMessage(`{}`)})

	// Then
	require.ErrorIs(t, err, store.markSentErr)
}

func TestEngineReceiveDataOrdering(t *testing.T) {
	// Given
	store := newSpyStore()
	sender := newSpySender(store)
	dispatcher := newSpyDispatcher(store)
	engine := NewEngine(Config{}, store, sender, dispatcher, WithInboundMiddleware(
		func(next InboundHandler) InboundHandler {
			return func(ctx context.Context, frame *Frame) (InboundDecision, error) {
				store.record("BeforeDispatch")
				return next(ctx, frame)
			}
		},
	))

	// When
	err := engine.Receive(context.Background(), Envelope{Type: EnvelopeTypeData, QueueID: "conn_1", Stream: StreamACP, Seq: 1, Payload: json.RawMessage(`{"hello":true}`)})
	require.NoError(t, err)

	// Then
	wantOrder := []string{"SaveInboundIfAbsent:1", "Sender.Send:ack:1", "BeforeDispatch", "UpdateMetadata:1", "Dispatcher.Dispatch:1", "MarkApplied:1"}
	require.Equal(t, wantOrder, store.calls)
}

func TestEngineReceiveDuplicate(t *testing.T) {
	// Given
	store := newSpyStore()
	store.saveInboundInserted = false
	sender := newSpySender(store)
	dispatcher := newSpyDispatcher(store)
	engine := NewEngine(Config{}, store, sender, dispatcher, WithInboundMiddleware(
		func(next InboundHandler) InboundHandler {
			return func(ctx context.Context, frame *Frame) (InboundDecision, error) {
				require.FailNow(t, "BeforeDispatch should not run for duplicate inbound")
				return next(ctx, frame)
			}
		},
	))

	// When
	err := engine.Receive(context.Background(), Envelope{Type: EnvelopeTypeData, QueueID: "conn_1", Stream: StreamACP, Seq: 1, Payload: json.RawMessage(`{}`)})
	require.NoError(t, err)

	// Then
	wantOrder := []string{"SaveInboundIfAbsent:1", "Sender.Send:ack:1"}
	require.Equal(t, wantOrder, store.calls)
	require.False(t, dispatcher.called)
}

func TestEngineReceiveHookReject(t *testing.T) {
	// Given
	store := newSpyStore()
	sender := newSpySender(store)
	dispatcher := newSpyDispatcher(store)
	engine := NewEngine(Config{}, store, sender, dispatcher, WithInboundMiddleware(inboundDecisionMiddleware(InboundDecision{
		Action:       InboundReject,
		ErrorMessage: "denied",
	})))

	// When
	err := engine.Receive(context.Background(), Envelope{Type: EnvelopeTypeData, QueueID: "conn_1", Stream: StreamACP, Seq: 1, Payload: json.RawMessage(`{}`)})
	require.NoError(t, err)

	// Then
	wantOrder := []string{"SaveInboundIfAbsent:1", "Sender.Send:ack:1", "UpdateMetadata:1", "MarkRejected:1:denied"}
	require.Equal(t, wantOrder, store.calls)
	require.False(t, dispatcher.called)
}

func TestEngineReceiveHookError(t *testing.T) {
	// Given
	store := newSpyStore()
	sender := newSpySender(store)
	dispatcher := newSpyDispatcher(store)
	hookErr := errors.New("policy timeout")
	engine := NewEngine(Config{}, store, sender, dispatcher, WithInboundMiddleware(inboundErrorMiddleware(hookErr)))

	// When
	err := engine.Receive(context.Background(), Envelope{Type: EnvelopeTypeData, QueueID: "conn_1", Stream: StreamACP, Seq: 1, Payload: json.RawMessage(`{}`)})
	require.NoError(t, err)

	// Then
	wantOrder := []string{"SaveInboundIfAbsent:1", "Sender.Send:ack:1", "RecordDispatchFailure:1:policy timeout"}
	require.Equal(t, wantOrder, store.calls)
	require.False(t, dispatcher.called)
}

func TestEngineReceiveDispatchError(t *testing.T) {
	// Given
	store := newSpyStore()
	sender := newSpySender(store)
	dispatcher := newSpyDispatcher(store)
	dispatcher.err = errors.New("stdin closed")
	engine := NewEngine(Config{}, store, sender, dispatcher)

	// When
	err := engine.Receive(context.Background(), Envelope{Type: EnvelopeTypeData, QueueID: "conn_1", Stream: StreamACP, Seq: 1, Payload: json.RawMessage(`{}`)})
	require.NoError(t, err)

	// Then
	wantOrder := []string{"SaveInboundIfAbsent:1", "Sender.Send:ack:1", "UpdateMetadata:1", "Dispatcher.Dispatch:1", "RecordDispatchFailure:1:stdin closed"}
	require.Equal(t, wantOrder, store.calls)
}

func TestEngineReceiveTombstone(t *testing.T) {
	// Given
	store := newSpyStore()
	sender := newSpySender(store)
	dispatcher := newSpyDispatcher(store)
	engine := NewEngine(Config{}, store, sender, dispatcher)

	// When
	err := engine.Receive(context.Background(), Envelope{Type: EnvelopeTypeTombstone, QueueID: "conn_1", Stream: StreamACP, Seq: 2, ErrorMessage: "blocked"})
	require.NoError(t, err)

	// Then
	wantOrder := []string{"SaveInboundIfAbsent:2", "Sender.Send:ack:2", "MarkApplied:2"}
	require.Equal(t, wantOrder, store.calls)
	require.False(t, dispatcher.called)
}

func TestEngineReceiveACK(t *testing.T) {
	// Given
	store := newSpyStore()
	engine := NewEngine(Config{}, store, newSpySender(store), nil)

	// When
	err := engine.Receive(context.Background(), AckEnvelope("conn_1", StreamACP, 4))
	require.NoError(t, err)

	// Then
	wantOrder := []string{"AckOutboundThrough:4"}
	require.Equal(t, wantOrder, store.calls)
}

func TestEngineReceiveInvalidEnvelope(t *testing.T) {
	// Given
	store := newSpyStore()
	sender := newSpySender(store)
	engine := NewEngine(Config{}, store, sender, nil)

	// When
	err := engine.Receive(context.Background(), Envelope{Type: "unknown", QueueID: "conn_1", Stream: StreamACP, Seq: 1})

	// Then
	require.ErrorIs(t, err, ErrInvalidEnvelope)
	require.Empty(t, store.calls)
	require.Empty(t, sender.sent)
}

func TestEngineReplayOutboundOrdering(t *testing.T) {
	// Given
	store := newSpyStore()
	store.outboundReplay = []Frame{
		testFrame(1, DirectionOutbound, FrameKindData),
		testFrame(2, DirectionOutbound, FrameKindTombstone),
	}
	sender := newSpySender(store)
	engine := NewEngine(Config{}, store, sender, nil)

	// When
	err := engine.ReplayOutbound(context.Background(), "conn_1", StreamACP, 100)
	require.NoError(t, err)

	// Then
	wantOrder := []string{"ListOutboundReplay", "Sender.Send:data:1", "MarkSent:1", "Sender.Send:tombstone:2", "MarkSent:2"}
	require.Equal(t, wantOrder, store.calls)
}

func TestEngineReplayOutboundFailure(t *testing.T) {
	// Given
	store := newSpyStore()
	store.outboundReplay = []Frame{testFrame(1, DirectionOutbound, FrameKindData), testFrame(2, DirectionOutbound, FrameKindData)}
	sender := newSpySender(store)
	sender.err = errors.New("socket closed")
	engine := NewEngine(Config{}, store, sender, nil)

	// When
	err := engine.ReplayOutbound(context.Background(), "conn_1", StreamACP, 100)

	// Then
	require.ErrorIs(t, err, sender.err)
	wantOrder := []string{"ListOutboundReplay", "Sender.Send:data:1", "RecordSendFailure:1:socket closed"}
	require.Equal(t, wantOrder, store.calls)
}

func TestEngineReplayInboundOrdering(t *testing.T) {
	// Given
	store := newSpyStore()
	store.inboundReplay = []Frame{
		testFrame(1, DirectionInbound, FrameKindData),
		testFrame(2, DirectionInbound, FrameKindTombstone),
	}
	dispatcher := newSpyDispatcher(store)
	engine := NewEngine(Config{}, store, nil, dispatcher)

	// When
	err := engine.ReplayInbound(context.Background(), "conn_1", StreamACP, 100)
	require.NoError(t, err)

	// Then
	wantOrder := []string{"ListInboundReplay", "UpdateMetadata:1", "Dispatcher.Dispatch:1", "MarkApplied:1", "MarkApplied:2"}
	require.Equal(t, wantOrder, store.calls)
}

type spyStore struct {
	calls               []string
	nextSeq             int64
	saveInboundInserted bool
	outboundReplay      []Frame
	inboundReplay       []Frame
	markSentErr         error
}

func newSpyStore() *spyStore {
	return &spyStore{saveInboundInserted: true}
}

func (s *spyStore) record(call string) {
	s.calls = append(s.calls, call)
}

func (s *spyStore) AppendOutboundData(ctx context.Context, queueID string, stream Stream, payload json.RawMessage, metadata Metadata) (Frame, error) {
	s.record("AppendOutboundData")
	s.nextSeq++
	return Frame{
		Key:      FrameKey{QueueID: queueID, Stream: stream, Seq: s.nextSeq, Direction: DirectionOutbound},
		Kind:     FrameKindData,
		Payload:  append(json.RawMessage(nil), payload...),
		Metadata: metadata.Clone(),
		Status:   StatusPending,
	}, nil
}

func (s *spyStore) AppendOutboundTombstone(ctx context.Context, queueID string, stream Stream, errorMessage string, metadata Metadata) (Frame, error) {
	s.record("AppendOutboundTombstone")
	s.nextSeq++
	return Frame{
		Key:          FrameKey{QueueID: queueID, Stream: stream, Seq: s.nextSeq, Direction: DirectionOutbound},
		Kind:         FrameKindTombstone,
		Metadata:     metadata.Clone(),
		Status:       StatusPending,
		ErrorMessage: errorMessage,
	}, nil
}

func (s *spyStore) SaveInboundIfAbsent(ctx context.Context, frame Frame) (bool, Frame, error) {
	s.record("SaveInboundIfAbsent:" + itoa(frame.Key.Seq))
	return s.saveInboundInserted, frame, nil
}

func (s *spyStore) ListOutboundReplay(ctx context.Context, queueID string, stream Stream, limit int) ([]Frame, error) {
	s.record("ListOutboundReplay")
	return append([]Frame(nil), s.outboundReplay...), nil
}

func (s *spyStore) ListInboundReplay(ctx context.Context, queueID string, stream Stream, limit int) ([]Frame, error) {
	s.record("ListInboundReplay")
	return append([]Frame(nil), s.inboundReplay...), nil
}

func (s *spyStore) MarkSent(ctx context.Context, key FrameKey) error {
	s.record("MarkSent:" + itoa(key.Seq))
	return s.markSentErr
}

func (s *spyStore) AckOutboundThrough(ctx context.Context, queueID string, stream Stream, throughSeq int64) error {
	s.record("AckOutboundThrough:" + itoa(throughSeq))
	return nil
}

func (s *spyStore) MarkApplied(ctx context.Context, key FrameKey) error {
	s.record("MarkApplied:" + itoa(key.Seq))
	return nil
}

func (s *spyStore) MarkRejected(ctx context.Context, key FrameKey, errorMessage string) error {
	s.record("MarkRejected:" + itoa(key.Seq) + ":" + errorMessage)
	return nil
}

func (s *spyStore) RecordSendFailure(ctx context.Context, key FrameKey, errorMessage string) error {
	s.record("RecordSendFailure:" + itoa(key.Seq) + ":" + errorMessage)
	return nil
}

func (s *spyStore) RecordDispatchFailure(ctx context.Context, key FrameKey, errorMessage string) error {
	s.record("RecordDispatchFailure:" + itoa(key.Seq) + ":" + errorMessage)
	return nil
}

func (s *spyStore) UpdateMetadata(ctx context.Context, key FrameKey, metadata Metadata) error {
	s.record("UpdateMetadata:" + itoa(key.Seq))
	return nil
}

type spySender struct {
	recorder *spyStore
	sent     []Envelope
	err      error
}

func newSpySender(recorder *spyStore) *spySender {
	return &spySender{recorder: recorder}
}

func (s *spySender) Send(ctx context.Context, env Envelope) error {
	s.sent = append(s.sent, env)
	if s.recorder != nil {
		s.recorder.record("Sender.Send:" + string(env.Type) + ":" + itoa(env.Seq))
	}
	return s.err
}

type spyDispatcher struct {
	store  *spyStore
	called bool
	err    error
}

func newSpyDispatcher(store *spyStore) *spyDispatcher {
	return &spyDispatcher{store: store}
}

func (d *spyDispatcher) Dispatch(ctx context.Context, frame Frame) error {
	d.called = true
	d.store.record("Dispatcher.Dispatch:" + itoa(frame.Key.Seq))
	return d.err
}

func outboundDecisionMiddleware(decision OutboundDecision) OutboundMiddleware {
	return func(next OutboundHandler) OutboundHandler {
		return func(ctx context.Context, msg *OutboundMessage) (OutboundDecision, error) {
			if decision.Metadata == nil {
				decision.Metadata = msg.Metadata.Clone()
			}
			return decision, nil
		}
	}
}

func outboundErrorMiddleware(err error) OutboundMiddleware {
	return func(next OutboundHandler) OutboundHandler {
		return func(context.Context, *OutboundMessage) (OutboundDecision, error) {
			return OutboundDecision{}, err
		}
	}
}

func inboundDecisionMiddleware(decision InboundDecision) InboundMiddleware {
	return func(next InboundHandler) InboundHandler {
		return func(ctx context.Context, frame *Frame) (InboundDecision, error) {
			if decision.Metadata == nil {
				decision.Metadata = frame.Metadata.Clone()
			}
			return decision, nil
		}
	}
}

func inboundErrorMiddleware(err error) InboundMiddleware {
	return func(next InboundHandler) InboundHandler {
		return func(context.Context, *Frame) (InboundDecision, error) {
			return InboundDecision{}, err
		}
	}
}

func testFrame(seq int64, direction Direction, kind FrameKind) Frame {
	frame := Frame{
		Key:    FrameKey{QueueID: "conn_1", Stream: StreamACP, Seq: seq, Direction: direction},
		Kind:   kind,
		Status: StatusPending,
	}
	if direction == DirectionInbound {
		frame.Status = StatusReceived
	}
	if kind == FrameKindData {
		frame.Payload = json.RawMessage(`{}`)
	} else {
		frame.ErrorMessage = "blocked"
	}
	return frame
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
