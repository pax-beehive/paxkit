package reliablemq

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRuntimeFuncAdapters(t *testing.T) {
	// Given
	senderCalled := false
	dispatcherCalled := false
	sender := SenderFunc(func(ctx context.Context, env Envelope) error {
		senderCalled = env.Type == EnvelopeTypeAck
		return nil
	})
	dispatcher := DispatcherFunc(func(ctx context.Context, frame Frame) error {
		dispatcherCalled = frame.Kind == FrameKindData
		return nil
	})

	// When
	require.NoError(t, sender.Send(context.Background(), AckEnvelope("conn_1", StreamACP, 1)))
	require.NoError(t, dispatcher.Dispatch(context.Background(), Frame{Kind: FrameKindData}))

	// Then
	require.True(t, senderCalled)
	require.True(t, dispatcherCalled)
}

func TestFrameCloneIsolation(t *testing.T) {
	// Given
	frame := Frame{
		Key:      FrameKey{QueueID: "conn_1", Stream: StreamACP, Seq: 1, Direction: DirectionOutbound},
		Kind:     FrameKindData,
		Payload:  json.RawMessage(`{"before":true}`),
		Metadata: Metadata{"trace_id": "trace_1"},
	}

	// When
	cloned := frame.Clone()
	cloned.Payload[1] = 'X'
	cloned.Metadata["trace_id"] = "trace_2"

	// Then
	require.JSONEq(t, `{"before":true}`, string(frame.Payload))
	require.Equal(t, "trace_1", frame.Metadata["trace_id"])
}

func TestDefaultStatus(t *testing.T) {
	// When / Then
	require.Equal(t, StatusReceived, DefaultStatus(DirectionInbound))
	require.Equal(t, StatusPending, DefaultStatus(DirectionOutbound))
}

func TestValidateOutboundMessageErrors(t *testing.T) {
	tests := []struct {
		name string
		msg  OutboundMessage
	}{
		{name: "empty queue", msg: OutboundMessage{Stream: StreamACP, Payload: json.RawMessage(`{}`)}},
		{name: "empty stream", msg: OutboundMessage{QueueID: "conn_1", Payload: json.RawMessage(`{}`)}},
		{name: "missing payload", msg: OutboundMessage{QueueID: "conn_1", Stream: StreamACP}},
		{name: "invalid payload", msg: OutboundMessage{QueueID: "conn_1", Stream: StreamACP, Payload: json.RawMessage(`{`)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When
			err := ValidateOutboundMessage(tt.msg)

			// Then
			require.ErrorIs(t, err, ErrInvalidFrame)
		})
	}
}

func TestValidateFrameErrors(t *testing.T) {
	valid := Frame{
		Key:     FrameKey{QueueID: "conn_1", Stream: StreamACP, Seq: 1, Direction: DirectionOutbound},
		Kind:    FrameKindData,
		Payload: json.RawMessage(`{}`),
	}
	tests := []struct {
		name   string
		mutate func(*Frame)
	}{
		{name: "empty queue", mutate: func(f *Frame) { f.Key.QueueID = "" }},
		{name: "empty stream", mutate: func(f *Frame) { f.Key.Stream = "" }},
		{name: "invalid seq", mutate: func(f *Frame) { f.Key.Seq = 0 }},
		{name: "invalid direction", mutate: func(f *Frame) { f.Key.Direction = "sideways" }},
		{name: "invalid kind", mutate: func(f *Frame) { f.Kind = "mystery" }},
		{name: "data missing payload", mutate: func(f *Frame) { f.Payload = nil }},
		{name: "data invalid payload", mutate: func(f *Frame) { f.Payload = json.RawMessage(`{`) }},
		{name: "tombstone with payload", mutate: func(f *Frame) { f.Kind = FrameKindTombstone }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			frame := valid.Clone()
			tt.mutate(&frame)

			// When
			err := ValidateFrame(frame)

			// Then
			require.ErrorIs(t, err, ErrInvalidFrame)
		})
	}
}

func TestEngineConfigurationErrors(t *testing.T) {
	// Given
	store := newSpyStore()

	tests := []struct {
		name string
		run  func() error
	}{
		{name: "nil engine", run: func() error {
			var engine *Engine
			_, err := engine.Send(context.Background(), OutboundMessage{})
			return err
		}},
		{name: "missing store", run: func() error {
			_, err := NewEngine(Config{}, nil, newSpySender(nil), nil).Send(context.Background(), OutboundMessage{QueueID: "conn_1", Stream: StreamACP, Payload: json.RawMessage(`{}`)})
			return err
		}},
		{name: "missing sender", run: func() error {
			_, err := NewEngine(Config{}, store, nil, nil).Send(context.Background(), OutboundMessage{QueueID: "conn_1", Stream: StreamACP, Payload: json.RawMessage(`{}`)})
			return err
		}},
		{name: "missing dispatcher", run: func() error {
			return NewEngine(Config{}, store, nil, nil).ReplayInbound(context.Background(), "conn_1", StreamACP, 1)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When
			err := tt.run()

			// Then
			require.Error(t, err)
		})
	}
}
