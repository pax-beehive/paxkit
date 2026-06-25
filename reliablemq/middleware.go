package reliablemq

import (
	"context"
	"encoding/json"
)

type OutboundAction string

const (
	OutboundContinue  OutboundAction = "continue"
	OutboundTombstone OutboundAction = "tombstone"
)

type OutboundMessage struct {
	QueueID  string
	Stream   Stream
	Payload  json.RawMessage
	Metadata Metadata
}

type OutboundDecision struct {
	Action       OutboundAction
	ErrorMessage string
	Metadata     Metadata
}

type InboundAction string

const (
	InboundContinue InboundAction = "continue"
	InboundReject   InboundAction = "reject"
)

type InboundDecision struct {
	Action       InboundAction
	ErrorMessage string
	Metadata     Metadata
}

type OutboundHandler func(context.Context, *OutboundMessage) (OutboundDecision, error)
type InboundHandler func(context.Context, *Frame) (InboundDecision, error)

type OutboundMiddleware func(OutboundHandler) OutboundHandler
type InboundMiddleware func(InboundHandler) InboundHandler

type Option interface {
	apply(*engineOptions)
}

type optionFunc func(*engineOptions)

func (fn optionFunc) apply(options *engineOptions) {
	fn(options)
}

func WithOutboundMiddleware(middlewares ...OutboundMiddleware) Option {
	return optionFunc(func(options *engineOptions) {
		options.outbound = append(options.outbound, middlewares...)
	})
}

func WithInboundMiddleware(middlewares ...InboundMiddleware) Option {
	return optionFunc(func(options *engineOptions) {
		options.inbound = append(options.inbound, middlewares...)
	})
}

type engineOptions struct {
	outbound []OutboundMiddleware
	inbound  []InboundMiddleware
}

func buildOutboundHandler(middlewares []OutboundMiddleware) OutboundHandler {
	handler := func(_ context.Context, msg *OutboundMessage) (OutboundDecision, error) {
		return OutboundDecision{Action: OutboundContinue, Metadata: msg.Metadata.Clone()}, nil
	}
	for i := len(middlewares) - 1; i >= 0; i-- {
		if middlewares[i] == nil {
			continue
		}
		handler = middlewares[i](handler)
	}
	return handler
}

func buildInboundHandler(middlewares []InboundMiddleware) InboundHandler {
	handler := func(_ context.Context, frame *Frame) (InboundDecision, error) {
		return InboundDecision{Action: InboundContinue, Metadata: frame.Metadata.Clone()}, nil
	}
	for i := len(middlewares) - 1; i >= 0; i-- {
		if middlewares[i] == nil {
			continue
		}
		handler = middlewares[i](handler)
	}
	return handler
}
