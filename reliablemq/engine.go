package reliablemq

import (
	"context"
	"encoding/json"
	"fmt"
)

type Sender interface {
	Send(ctx context.Context, env Envelope) error
}

type Dispatcher interface {
	Dispatch(ctx context.Context, frame Frame) error
}

type SenderFunc func(context.Context, Envelope) error

func (fn SenderFunc) Send(ctx context.Context, env Envelope) error {
	return fn(ctx, env)
}

type DispatcherFunc func(context.Context, Frame) error

func (fn DispatcherFunc) Dispatch(ctx context.Context, frame Frame) error {
	return fn(ctx, frame)
}

type OutboundHookErrorPolicy string

const (
	OutboundHookErrorTombstone OutboundHookErrorPolicy = "tombstone"
)

type Config struct {
	OutboundHookErrorPolicy OutboundHookErrorPolicy
}

type Engine struct {
	config          Config
	Store           DurableStore
	Sender          Sender
	Dispatcher      Dispatcher
	outboundHandler OutboundHandler
	inboundHandler  InboundHandler
}

func NewEngine(config Config, store DurableStore, sender Sender, dispatcher Dispatcher, opts ...Option) *Engine {
	if config.OutboundHookErrorPolicy == "" {
		config.OutboundHookErrorPolicy = OutboundHookErrorTombstone
	}
	var options engineOptions
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&options)
		}
	}
	return &Engine{
		config:          config,
		Store:           store,
		Sender:          sender,
		Dispatcher:      dispatcher,
		outboundHandler: buildOutboundHandler(options.outbound),
		inboundHandler:  buildInboundHandler(options.inbound),
	}
}

func (e *Engine) Send(ctx context.Context, msg OutboundMessage) (Frame, error) {
	if err := validateEngine(e, true, false); err != nil {
		return Frame{}, err
	}
	if err := ValidateOutboundMessage(msg); err != nil {
		return Frame{}, err
	}

	msg.Metadata = msg.Metadata.Clone()
	decision, hookErr := e.outboundHandler(ctx, &msg)
	if hookErr != nil {
		decision = OutboundDecision{
			Action:       OutboundTombstone,
			ErrorMessage: hookErr.Error(),
			Metadata:     msg.Metadata.Clone(),
		}
	}

	frame, err := e.appendOutboundFromDecision(ctx, msg, decision)
	if err != nil {
		return Frame{}, err
	}
	if err := e.sendFrame(ctx, frame); err != nil {
		return frame, err
	}
	return frame, nil
}

func (e *Engine) Receive(ctx context.Context, env Envelope) error {
	if err := validateEngine(e, false, false); err != nil {
		return err
	}
	if err := ValidateEnvelope(env); err != nil {
		return err
	}
	switch env.Type {
	case EnvelopeTypeAck:
		return e.Store.AckOutboundThrough(ctx, env.QueueID, env.Stream, env.Seq)
	case EnvelopeTypeData, EnvelopeTypeTombstone:
		return e.receiveFrame(ctx, env)
	default:
		return fmt.Errorf("%w: unknown type %q", ErrInvalidEnvelope, env.Type)
	}
}

func (e *Engine) ReplayOutbound(ctx context.Context, queueID string, stream Stream, limit int) error {
	if err := validateEngine(e, true, false); err != nil {
		return err
	}
	frames, err := e.Store.ListOutboundReplay(ctx, queueID, stream, limit)
	if err != nil {
		return err
	}
	for _, frame := range frames {
		if err := ValidateFrame(frame); err != nil {
			return err
		}
		if frame.Key.Direction != DirectionOutbound {
			return fmt.Errorf("%w: replay outbound frame direction is %q", ErrInvalidFrame, frame.Key.Direction)
		}
		if err := e.sendFrame(ctx, frame); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) ReplayInbound(ctx context.Context, queueID string, stream Stream, limit int) error {
	if err := validateEngine(e, false, true); err != nil {
		return err
	}
	frames, err := e.Store.ListInboundReplay(ctx, queueID, stream, limit)
	if err != nil {
		return err
	}
	for _, frame := range frames {
		if err := e.dispatchOrApplyInbound(ctx, frame); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) appendOutboundFromDecision(ctx context.Context, msg OutboundMessage, decision OutboundDecision) (Frame, error) {
	metadata := decision.Metadata.Clone()
	if metadata == nil {
		metadata = msg.Metadata.Clone()
	}
	switch decision.Action {
	case "", OutboundContinue:
		return e.Store.AppendOutboundData(ctx, msg.QueueID, msg.Stream, msg.Payload, metadata)
	case OutboundTombstone:
		return e.Store.AppendOutboundTombstone(ctx, msg.QueueID, msg.Stream, decision.ErrorMessage, metadata)
	default:
		return Frame{}, fmt.Errorf("%w: unknown outbound action %q", ErrInvalidDecision, decision.Action)
	}
}

func (e *Engine) receiveFrame(ctx context.Context, env Envelope) error {
	if e.Sender == nil {
		return fmt.Errorf("reliablemq: sender is required for inbound ACK")
	}
	frame := InboundFrameFromEnvelope(env)
	inserted, stored, err := e.Store.SaveInboundIfAbsent(ctx, frame)
	if err != nil {
		return err
	}
	if err := e.Sender.Send(ctx, AckEnvelope(env.QueueID, env.Stream, env.Seq)); err != nil {
		return err
	}
	if !inserted {
		return nil
	}
	return e.dispatchOrApplyInbound(ctx, stored)
}

func (e *Engine) dispatchOrApplyInbound(ctx context.Context, frame Frame) error {
	if err := ValidateFrame(frame); err != nil {
		return err
	}
	if frame.Key.Direction != DirectionInbound {
		return fmt.Errorf("%w: inbound frame direction is %q", ErrInvalidFrame, frame.Key.Direction)
	}
	if frame.Kind == FrameKindTombstone {
		return e.Store.MarkApplied(ctx, frame.Key)
	}
	if e.Dispatcher == nil {
		return fmt.Errorf("reliablemq: dispatcher is required for inbound data")
	}

	decision, hookErr := e.inboundHandler(ctx, &frame)
	if hookErr != nil {
		return e.Store.RecordDispatchFailure(ctx, frame.Key, hookErr.Error())
	}
	frame.Metadata = decision.Metadata.Clone()
	if err := e.Store.UpdateMetadata(ctx, frame.Key, frame.Metadata); err != nil {
		return err
	}
	switch decision.Action {
	case "", InboundContinue:
	case InboundReject:
		return e.Store.MarkRejected(ctx, frame.Key, decision.ErrorMessage)
	default:
		return fmt.Errorf("%w: unknown inbound action %q", ErrInvalidDecision, decision.Action)
	}

	if err := e.Dispatcher.Dispatch(ctx, frame); err != nil {
		return e.Store.RecordDispatchFailure(ctx, frame.Key, err.Error())
	}
	return e.Store.MarkApplied(ctx, frame.Key)
}

func (e *Engine) sendFrame(ctx context.Context, frame Frame) error {
	env := EnvelopeFromFrame(frame)
	if err := e.Sender.Send(ctx, env); err != nil {
		_ = e.Store.RecordSendFailure(ctx, frame.Key, err.Error())
		return err
	}
	if err := e.Store.MarkSent(ctx, frame.Key); err != nil {
		return err
	}
	return nil
}

func ValidateOutboundMessage(msg OutboundMessage) error {
	if msg.QueueID == "" {
		return fmt.Errorf("%w: queue_id is required", ErrInvalidFrame)
	}
	if msg.Stream == "" {
		return fmt.Errorf("%w: stream is required", ErrInvalidFrame)
	}
	if len(msg.Payload) == 0 {
		return fmt.Errorf("%w: payload is required", ErrInvalidFrame)
	}
	if !json.Valid(msg.Payload) {
		return fmt.Errorf("%w: payload must be valid JSON", ErrInvalidFrame)
	}
	return nil
}

func ValidateFrame(frame Frame) error {
	if frame.Key.QueueID == "" {
		return fmt.Errorf("%w: queue_id is required", ErrInvalidFrame)
	}
	if frame.Key.Stream == "" {
		return fmt.Errorf("%w: stream is required", ErrInvalidFrame)
	}
	if frame.Key.Seq <= 0 {
		return fmt.Errorf("%w: seq must be positive", ErrInvalidFrame)
	}
	if frame.Key.Direction != DirectionOutbound && frame.Key.Direction != DirectionInbound {
		return fmt.Errorf("%w: invalid direction %q", ErrInvalidFrame, frame.Key.Direction)
	}
	if frame.Kind != FrameKindData && frame.Kind != FrameKindTombstone {
		return fmt.Errorf("%w: invalid frame kind %q", ErrInvalidFrame, frame.Kind)
	}
	if frame.Kind == FrameKindData {
		if len(frame.Payload) == 0 {
			return fmt.Errorf("%w: payload is required", ErrInvalidFrame)
		}
		if !json.Valid(frame.Payload) {
			return fmt.Errorf("%w: payload must be valid JSON", ErrInvalidFrame)
		}
	}
	if frame.Kind == FrameKindTombstone && len(frame.Payload) > 0 {
		return fmt.Errorf("%w: tombstone must not carry payload", ErrInvalidFrame)
	}
	return nil
}

func validateEngine(e *Engine, needSender bool, needDispatcher bool) error {
	if e == nil {
		return fmt.Errorf("reliablemq: engine is nil")
	}
	if e.Store == nil {
		return fmt.Errorf("reliablemq: store is required")
	}
	if needSender && e.Sender == nil {
		return fmt.Errorf("reliablemq: sender is required")
	}
	if needDispatcher && e.Dispatcher == nil {
		return fmt.Errorf("reliablemq: dispatcher is required")
	}
	return nil
}
