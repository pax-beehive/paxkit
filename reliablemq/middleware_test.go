package reliablemq

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMiddlewareOutboundOnionOrder(t *testing.T) {
	// Given
	var order []string
	handler := buildOutboundHandler([]OutboundMiddleware{
		func(next OutboundHandler) OutboundHandler {
			return func(ctx context.Context, msg *OutboundMessage) (OutboundDecision, error) {
				order = append(order, "h1 before")
				decision, err := next(ctx, msg)
				order = append(order, "h1 after")
				return decision, err
			}
		},
		func(next OutboundHandler) OutboundHandler {
			return func(ctx context.Context, msg *OutboundMessage) (OutboundDecision, error) {
				order = append(order, "h2 before")
				decision, err := next(ctx, msg)
				order = append(order, "h2 after")
				return decision, err
			}
		},
	})
	msg := &OutboundMessage{QueueID: "conn_1", Stream: StreamACP, Payload: []byte(`{}`)}

	// When
	decision, err := handler(context.Background(), msg)
	require.NoError(t, err)

	// Then
	want := []string{"h1 before", "h2 before", "h2 after", "h1 after"}
	require.Equal(t, want, order)
	require.Equal(t, OutboundContinue, decision.Action)
}

func TestMiddlewareOutboundShortCircuit(t *testing.T) {
	// Given
	calledDownstream := false
	handler := buildOutboundHandler([]OutboundMiddleware{
		func(next OutboundHandler) OutboundHandler {
			return func(ctx context.Context, msg *OutboundMessage) (OutboundDecision, error) {
				return OutboundDecision{Action: OutboundTombstone, ErrorMessage: "blocked"}, nil
			}
		},
		func(next OutboundHandler) OutboundHandler {
			return func(ctx context.Context, msg *OutboundMessage) (OutboundDecision, error) {
				calledDownstream = true
				return next(ctx, msg)
			}
		},
	})

	// When
	decision, err := handler(context.Background(), &OutboundMessage{QueueID: "conn_1", Stream: StreamACP, Payload: []byte(`{}`)})
	require.NoError(t, err)

	// Then
	require.False(t, calledDownstream)
	require.Equal(t, OutboundTombstone, decision.Action)
	require.Equal(t, "blocked", decision.ErrorMessage)
}

func TestMiddlewareOutboundMetadataMutation(t *testing.T) {
	// Given
	handler := buildOutboundHandler([]OutboundMiddleware{
		func(next OutboundHandler) OutboundHandler {
			return func(ctx context.Context, msg *OutboundMessage) (OutboundDecision, error) {
				if msg.Metadata == nil {
					msg.Metadata = Metadata{}
				}
				msg.Metadata["trace_id"] = "trace_1"
				return next(ctx, msg)
			}
		},
	})

	// When
	decision, err := handler(context.Background(), &OutboundMessage{
		QueueID:  "conn_1",
		Stream:   StreamACP,
		Payload:  []byte(`{}`),
		Metadata: Metadata{"agent_id": "agent_1"},
	})
	require.NoError(t, err)

	// Then
	require.Equal(t, "trace_1", decision.Metadata["trace_id"])
	require.Equal(t, "agent_1", decision.Metadata["agent_id"])
}

func TestMiddlewareInboundOnionOrder(t *testing.T) {
	// Given
	var order []string
	handler := buildInboundHandler([]InboundMiddleware{
		func(next InboundHandler) InboundHandler {
			return func(ctx context.Context, frame *Frame) (InboundDecision, error) {
				order = append(order, "h1 before")
				decision, err := next(ctx, frame)
				order = append(order, "h1 after")
				return decision, err
			}
		},
		func(next InboundHandler) InboundHandler {
			return func(ctx context.Context, frame *Frame) (InboundDecision, error) {
				order = append(order, "h2 before")
				decision, err := next(ctx, frame)
				order = append(order, "h2 after")
				return decision, err
			}
		},
	})
	frame := &Frame{Key: FrameKey{QueueID: "conn_1", Stream: StreamACP, Seq: 1, Direction: DirectionInbound}, Kind: FrameKindData, Payload: []byte(`{}`)}

	// When
	decision, err := handler(context.Background(), frame)
	require.NoError(t, err)

	// Then
	want := []string{"h1 before", "h2 before", "h2 after", "h1 after"}
	require.Equal(t, want, order)
	require.Equal(t, InboundContinue, decision.Action)
}

func TestMiddlewareInboundRejectShortCircuit(t *testing.T) {
	// Given
	calledDownstream := false
	handler := buildInboundHandler([]InboundMiddleware{
		func(next InboundHandler) InboundHandler {
			return func(ctx context.Context, frame *Frame) (InboundDecision, error) {
				return InboundDecision{Action: InboundReject, ErrorMessage: "denied"}, nil
			}
		},
		func(next InboundHandler) InboundHandler {
			return func(ctx context.Context, frame *Frame) (InboundDecision, error) {
				calledDownstream = true
				return next(ctx, frame)
			}
		},
	})

	// When
	decision, err := handler(context.Background(), &Frame{
		Key:     FrameKey{QueueID: "conn_1", Stream: StreamACP, Seq: 1, Direction: DirectionInbound},
		Kind:    FrameKindData,
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)

	// Then
	require.False(t, calledDownstream)
	require.Equal(t, InboundReject, decision.Action)
	require.Equal(t, "denied", decision.ErrorMessage)
}

func TestMiddlewareErrorSeparateFromDecision(t *testing.T) {
	// Given
	wantErr := errors.New("policy service unavailable")
	handler := buildOutboundHandler([]OutboundMiddleware{
		func(next OutboundHandler) OutboundHandler {
			return func(ctx context.Context, msg *OutboundMessage) (OutboundDecision, error) {
				return OutboundDecision{}, wantErr
			}
		},
	})

	// When
	_, err := handler(context.Background(), &OutboundMessage{QueueID: "conn_1", Stream: StreamACP, Payload: []byte(`{}`)})

	// Then
	require.ErrorIs(t, err, wantErr)
}
