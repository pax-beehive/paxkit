package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/pax-beehive/paxkit/reliablemq"
)

type Store struct {
	mu             sync.Mutex
	now            func() time.Time
	seqs           map[seqKey]int64
	inboundApplied map[queueKey]int64
	frames         map[reliablemq.FrameKey]reliablemq.Frame
}

type queueKey struct {
	queueID string
	stream  reliablemq.Stream
}

type seqKey struct {
	queueID   string
	stream    reliablemq.Stream
	direction reliablemq.Direction
}

func New() *Store {
	return &Store{
		now:            time.Now,
		seqs:           make(map[seqKey]int64),
		inboundApplied: make(map[queueKey]int64),
		frames:         make(map[reliablemq.FrameKey]reliablemq.Frame),
	}
}

func (s *Store) AppendOutboundData(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	payload json.RawMessage,
	metadata reliablemq.Metadata,
) (reliablemq.Frame, error) {
	_ = ctx
	if !json.Valid(payload) {
		return reliablemq.Frame{}, fmt.Errorf("%w: payload must be valid JSON", reliablemq.ErrInvalidFrame)
	}
	return s.appendOutbound(queueID, stream, reliablemq.FrameKindData, payload, "", metadata)
}

func (s *Store) AppendOutboundTombstone(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	errorMessage string,
	metadata reliablemq.Metadata,
) (reliablemq.Frame, error) {
	_ = ctx
	return s.appendOutbound(queueID, stream, reliablemq.FrameKindTombstone, nil, errorMessage, metadata)
}

func (s *Store) SaveInboundIfAbsent(
	ctx context.Context,
	frame reliablemq.Frame,
) (bool, reliablemq.Frame, error) {
	_ = ctx
	if err := reliablemq.ValidateFrame(frame); err != nil {
		return false, reliablemq.Frame{}, err
	}
	if frame.Key.Direction != reliablemq.DirectionInbound {
		return false, reliablemq.Frame{}, fmt.Errorf("%w: inbound frame direction is %q", reliablemq.ErrInvalidFrame, frame.Key.Direction)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if frame.Key.Seq <= s.inboundApplied[queueKey{queueID: frame.Key.QueueID, stream: frame.Key.Stream}] {
		frame.Status = reliablemq.StatusApplied
		return false, frame.Clone(), nil
	}
	if existing, ok := s.frames[frame.Key]; ok {
		return false, existing.Clone(), nil
	}
	now := s.now().UTC()
	frame = frame.Clone()
	frame.Status = reliablemq.StatusReceived
	frame.CreatedAt = now
	frame.UpdatedAt = now
	s.frames[frame.Key] = frame
	return true, frame.Clone(), nil
}

func (s *Store) ListOutboundReplay(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	limit int,
) ([]reliablemq.Frame, error) {
	_ = ctx
	return s.list(queueID, stream, reliablemq.DirectionOutbound, limit, map[reliablemq.Status]bool{
		reliablemq.StatusPending: true,
		reliablemq.StatusSent:    true,
	})
}

func (s *Store) ListOutboundReplayFrom(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	fromSeq int64,
	limit int,
) ([]reliablemq.Frame, error) {
	_ = ctx
	if fromSeq <= 0 {
		return nil, fmt.Errorf("%w: from seq must be positive", reliablemq.ErrInvalidFrame)
	}
	return s.listFrom(queueID, stream, reliablemq.DirectionOutbound, fromSeq, limit, map[reliablemq.Status]bool{
		reliablemq.StatusPending: true,
		reliablemq.StatusSent:    true,
	})
}

func (s *Store) LoadQueueState(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
) (reliablemq.QueueState, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	return reliablemq.QueueState{
		NextOutboundSeq:       s.seqs[seqKey{queueID: queueID, stream: stream, direction: reliablemq.DirectionOutbound}] + 1,
		InboundAppliedThrough: s.inboundApplied[queueKey{queueID: queueID, stream: stream}],
	}, nil
}

func (s *Store) LoadProducerReconcileCheckpoint(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
) (reliablemq.ProducerReconcileCheckpoint, error) {
	state, err := s.LoadQueueState(ctx, queueID, stream)
	if err != nil {
		return reliablemq.ProducerReconcileCheckpoint{}, err
	}
	frames, err := s.ListOutboundReplay(ctx, queueID, stream, 1_000_000)
	if err != nil {
		return reliablemq.ProducerReconcileCheckpoint{}, err
	}
	var from, through int64
	for _, frame := range frames {
		if from == 0 || frame.Key.Seq < from {
			from = frame.Key.Seq
		}
		if frame.Key.Seq > through {
			through = frame.Key.Seq
		}
	}
	return reliablemq.ProducerReconcileCheckpoint{
		QueueID:         queueID,
		Stream:          stream,
		ProducerNextSeq: state.NextOutboundSeq,
		ReplayFrom:      from,
		ReplayThrough:   through,
	}, nil
}

func (s *Store) AdvanceProducerNextSeq(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	nextSeq int64,
) error {
	_ = ctx
	if nextSeq <= 0 {
		return fmt.Errorf("%w: next seq must be positive", reliablemq.ErrInvalidFrame)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	seqKey := seqKey{queueID: queueID, stream: stream, direction: reliablemq.DirectionOutbound}
	if s.seqs[seqKey] < nextSeq-1 {
		s.seqs[seqKey] = nextSeq - 1
	}
	s.ackOutboundThroughLocked(queueID, stream, nextSeq-1)
	return nil
}

func (s *Store) ConsumerAckedThrough(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
) (int64, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	next := int64(1)
	for {
		key := reliablemq.FrameKey{QueueID: queueID, Stream: stream, Seq: next, Direction: reliablemq.DirectionInbound}
		frame, ok := s.frames[key]
		if !ok || (frame.Status != reliablemq.StatusReceived && frame.Status != reliablemq.StatusApplied && frame.Status != reliablemq.StatusRejected) {
			return next - 1, nil
		}
		next++
	}
}

func (s *Store) ListInboundReplay(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	limit int,
) ([]reliablemq.Frame, error) {
	_ = ctx
	return s.list(queueID, stream, reliablemq.DirectionInbound, limit, map[reliablemq.Status]bool{
		reliablemq.StatusReceived: true,
	})
}

func (s *Store) MarkSent(ctx context.Context, key reliablemq.FrameKey) error {
	_ = ctx
	return s.update(key, func(frame *reliablemq.Frame) {
		frame.Status = reliablemq.StatusSent
		frame.ErrorMessage = ""
	})
}

func (s *Store) AckOutboundThrough(ctx context.Context, queueID string, stream reliablemq.Stream, throughSeq int64) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ackOutboundThroughLocked(queueID, stream, throughSeq)
	return nil
}

func (s *Store) ackOutboundThroughLocked(queueID string, stream reliablemq.Stream, throughSeq int64) {
	now := s.now().UTC()
	for key, frame := range s.frames {
		if key.QueueID != queueID ||
			key.Stream != stream ||
			key.Direction != reliablemq.DirectionOutbound ||
			key.Seq > throughSeq {
			continue
		}
		frame.Status = reliablemq.StatusAcked
		frame.UpdatedAt = now
		frame.ErrorMessage = ""
		s.frames[key] = frame
	}
}

func (s *Store) MarkApplied(ctx context.Context, key reliablemq.FrameKey) error {
	_ = ctx
	return s.updateAndAdvanceInbound(key, func(frame *reliablemq.Frame) {
		frame.Status = reliablemq.StatusApplied
		frame.ErrorMessage = ""
	})
}

func (s *Store) MarkRejected(ctx context.Context, key reliablemq.FrameKey, errorMessage string) error {
	_ = ctx
	return s.updateAndAdvanceInbound(key, func(frame *reliablemq.Frame) {
		frame.Status = reliablemq.StatusRejected
		frame.ErrorMessage = errorMessage
	})
}

func (s *Store) RecordSendFailure(ctx context.Context, key reliablemq.FrameKey, errorMessage string) error {
	_ = ctx
	return s.update(key, func(frame *reliablemq.Frame) {
		frame.ErrorMessage = errorMessage
	})
}

func (s *Store) RecordDispatchFailure(ctx context.Context, key reliablemq.FrameKey, errorMessage string) error {
	_ = ctx
	return s.update(key, func(frame *reliablemq.Frame) {
		frame.ErrorMessage = errorMessage
	})
}

func (s *Store) UpdateMetadata(ctx context.Context, key reliablemq.FrameKey, metadata reliablemq.Metadata) error {
	_ = ctx
	return s.update(key, func(frame *reliablemq.Frame) {
		frame.Metadata = metadata.Clone()
	})
}

func (s *Store) Get(key reliablemq.FrameKey) (reliablemq.Frame, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	frame, ok := s.frames[key]
	return frame.Clone(), ok
}

func (s *Store) appendOutbound(
	queueID string,
	stream reliablemq.Stream,
	kind reliablemq.FrameKind,
	payload json.RawMessage,
	errorMessage string,
	metadata reliablemq.Metadata,
) (reliablemq.Frame, error) {
	if queueID == "" {
		return reliablemq.Frame{}, fmt.Errorf("%w: queue_id is required", reliablemq.ErrInvalidFrame)
	}
	if stream == "" {
		return reliablemq.Frame{}, fmt.Errorf("%w: stream is required", reliablemq.ErrInvalidFrame)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	seqKey := seqKey{queueID: queueID, stream: stream, direction: reliablemq.DirectionOutbound}
	s.seqs[seqKey]++
	now := s.now().UTC()
	frame := reliablemq.Frame{
		Key: reliablemq.FrameKey{
			QueueID:   queueID,
			Stream:    stream,
			Seq:       s.seqs[seqKey],
			Direction: reliablemq.DirectionOutbound,
		},
		Kind:         kind,
		Payload:      append(json.RawMessage(nil), payload...),
		Metadata:     metadata.Clone(),
		Status:       reliablemq.StatusPending,
		ErrorMessage: errorMessage,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := reliablemq.ValidateFrame(frame); err != nil {
		return reliablemq.Frame{}, err
	}
	s.frames[frame.Key] = frame
	return frame.Clone(), nil
}

func (s *Store) list(
	queueID string,
	stream reliablemq.Stream,
	direction reliablemq.Direction,
	limit int,
	statuses map[reliablemq.Status]bool,
) ([]reliablemq.Frame, error) {
	return s.listFrom(queueID, stream, direction, 1, limit, statuses)
}

func (s *Store) listFrom(
	queueID string,
	stream reliablemq.Stream,
	direction reliablemq.Direction,
	fromSeq int64,
	limit int,
	statuses map[reliablemq.Status]bool,
) ([]reliablemq.Frame, error) {
	if limit <= 0 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	frames := make([]reliablemq.Frame, 0)
	for key, frame := range s.frames {
		if key.QueueID != queueID ||
			key.Stream != stream ||
			key.Direction != direction ||
			key.Seq < fromSeq ||
			!statuses[frame.Status] {
			continue
		}
		frames = append(frames, frame.Clone())
	}
	sort.Slice(frames, func(i, j int) bool {
		return frames[i].Key.Seq < frames[j].Key.Seq
	})
	if len(frames) > limit {
		frames = frames[:limit]
	}
	return frames, nil
}

func (s *Store) updateAndAdvanceInbound(key reliablemq.FrameKey, apply func(*reliablemq.Frame)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	frame, ok := s.frames[key]
	if !ok {
		return fmt.Errorf("%w: frame not found", reliablemq.ErrInvalidFrame)
	}
	apply(&frame)
	frame.UpdatedAt = s.now().UTC()
	s.frames[key] = frame
	if key.Direction == reliablemq.DirectionInbound {
		queueKey := queueKey{queueID: key.QueueID, stream: key.Stream}
		if key.Seq > s.inboundApplied[queueKey] {
			s.inboundApplied[queueKey] = key.Seq
		}
	}
	return nil
}

func (s *Store) update(key reliablemq.FrameKey, apply func(*reliablemq.Frame)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	frame, ok := s.frames[key]
	if !ok {
		return fmt.Errorf("%w: frame not found", reliablemq.ErrInvalidFrame)
	}
	apply(&frame)
	frame.UpdatedAt = s.now().UTC()
	s.frames[key] = frame
	return nil
}
