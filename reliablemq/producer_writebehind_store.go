package reliablemq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	ErrProducerStoreClosed        = errors.New("reliablemq: producer write-behind store is closed")
	ErrProducerBatchStoreRequired = errors.New("reliablemq: producer write-behind sink must implement batch store")
)

type ProducerWriteBehindStoreConfig struct {
	FlushInterval  time.Duration
	ManualFlush    bool
	RequireBatch   bool
	Now            func() time.Time
	OnFlushFailure func(error, ProducerWriteBehindStats)
}

type ProducerWriteBehindStoreOption interface {
	applyProducerWriteBehind(*ProducerWriteBehindStoreConfig)
}

type producerWriteBehindOptionFunc func(*ProducerWriteBehindStoreConfig)

func (fn producerWriteBehindOptionFunc) applyProducerWriteBehind(config *ProducerWriteBehindStoreConfig) {
	fn(config)
}

func WithProducerWriteBehindFlushInterval(interval time.Duration) ProducerWriteBehindStoreOption {
	return producerWriteBehindOptionFunc(func(config *ProducerWriteBehindStoreConfig) {
		if interval > 0 {
			config.FlushInterval = interval
		}
	})
}

func WithProducerWriteBehindManualFlush() ProducerWriteBehindStoreOption {
	return producerWriteBehindOptionFunc(func(config *ProducerWriteBehindStoreConfig) {
		config.ManualFlush = true
	})
}

func WithProducerWriteBehindRequireBatchStore() ProducerWriteBehindStoreOption {
	return producerWriteBehindOptionFunc(func(config *ProducerWriteBehindStoreConfig) {
		config.RequireBatch = true
	})
}

func WithProducerWriteBehindClock(now func() time.Time) ProducerWriteBehindStoreOption {
	return producerWriteBehindOptionFunc(func(config *ProducerWriteBehindStoreConfig) {
		if now != nil {
			config.Now = now
		}
	})
}

func WithProducerWriteBehindFlushFailureHandler(
	handler func(error, ProducerWriteBehindStats),
) ProducerWriteBehindStoreOption {
	return producerWriteBehindOptionFunc(func(config *ProducerWriteBehindStoreConfig) {
		config.OnFlushFailure = handler
	})
}

type ProducerWriteBehindStats struct {
	DirtyFrames              int
	DirtyPatches             int
	DirtyBytes               int64
	Degraded                 bool
	ConsecutiveFlushFailures int
	LastFlushError           string
	LastFlushAt              time.Time
	LastFlushFailureAt       time.Time
}

type ProducerWriteBehindStore struct {
	sink   DurableStore
	config ProducerWriteBehindStoreConfig

	mu           sync.Mutex
	queues       map[producerQueueKey]producerQueueState
	frames       map[FrameKey]Frame
	dirtyFrames  map[FrameKey]Frame
	dirtyPatches map[FrameKey]StorePatch
	closed       bool
	stats        ProducerWriteBehindStats
	retryAt      time.Time

	notify chan struct{}
	done   chan struct{}
	cancel context.CancelFunc
}

type producerQueueKey struct {
	queueID string
	stream  Stream
}

type producerQueueState struct {
	nextOutboundSeq int64
	loaded          bool
}

func NewProducerWriteBehindStore(
	sink DurableStore,
	opts ...ProducerWriteBehindStoreOption,
) *ProducerWriteBehindStore {
	config := ProducerWriteBehindStoreConfig{
		FlushInterval: time.Second,
		Now:           time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt.applyProducerWriteBehind(&config)
		}
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	store := &ProducerWriteBehindStore{
		sink:         sink,
		config:       config,
		queues:       make(map[producerQueueKey]producerQueueState),
		frames:       make(map[FrameKey]Frame),
		dirtyFrames:  make(map[FrameKey]Frame),
		dirtyPatches: make(map[FrameKey]StorePatch),
		notify:       make(chan struct{}, 1),
		done:         make(chan struct{}),
	}
	if !config.ManualFlush {
		var ctx context.Context
		ctx, store.cancel = context.WithCancel(context.Background())
		go store.run(ctx)
	}
	return store
}

func (s *ProducerWriteBehindStore) AppendOutboundData(
	ctx context.Context,
	queueID string,
	stream Stream,
	payload json.RawMessage,
	metadata Metadata,
) (Frame, error) {
	_ = ctx
	if len(payload) == 0 {
		return Frame{}, fmt.Errorf("%w: payload is required", ErrInvalidFrame)
	}
	if !json.Valid(payload) {
		return Frame{}, fmt.Errorf("%w: payload must be valid JSON", ErrInvalidFrame)
	}
	return s.appendOutbound(ctx, queueID, stream, FrameKindData, payload, "", metadata)
}

func (s *ProducerWriteBehindStore) AppendOutboundTombstone(
	ctx context.Context,
	queueID string,
	stream Stream,
	errorMessage string,
	metadata Metadata,
) (Frame, error) {
	return s.appendOutbound(ctx, queueID, stream, FrameKindTombstone, nil, errorMessage, metadata)
}

func (s *ProducerWriteBehindStore) SaveInboundIfAbsent(ctx context.Context, frame Frame) (bool, Frame, error) {
	return s.sink.SaveInboundIfAbsent(ctx, frame)
}

func (s *ProducerWriteBehindStore) ListOutboundReplay(
	ctx context.Context,
	queueID string,
	stream Stream,
	limit int,
) ([]Frame, error) {
	frames, err := s.sink.ListOutboundReplay(ctx, queueID, stream, limit)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	byKey := make(map[FrameKey]Frame, len(frames))
	for _, frame := range frames {
		byKey[frame.Key] = frame.Clone()
	}

	s.mu.Lock()
	for key, frame := range s.frames {
		if key.QueueID != queueID ||
			key.Stream != stream ||
			key.Direction != DirectionOutbound ||
			(frame.Status != StatusPending && frame.Status != StatusSent) {
			continue
		}
		byKey[key] = frame.Clone()
	}
	s.mu.Unlock()

	merged := make([]Frame, 0, len(byKey))
	for _, frame := range byKey {
		merged = append(merged, frame)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Key.Seq < merged[j].Key.Seq
	})
	if len(merged) > limit {
		merged = merged[:limit]
	}
	return merged, nil
}

func (s *ProducerWriteBehindStore) ListInboundReplay(
	ctx context.Context,
	queueID string,
	stream Stream,
	limit int,
) ([]Frame, error) {
	return s.sink.ListInboundReplay(ctx, queueID, stream, limit)
}

func (s *ProducerWriteBehindStore) LoadProducerReconcileCheckpoint(
	ctx context.Context,
	queueID string,
	stream Stream,
) (ProducerReconcileCheckpoint, error) {
	s.mu.Lock()
	state, err := s.queueStateLocked(ctx, queueID, stream)
	if err != nil {
		s.mu.Unlock()
		return ProducerReconcileCheckpoint{}, err
	}
	memoryFrames := make([]Frame, 0)
	for key, frame := range s.frames {
		if key.QueueID != queueID ||
			key.Stream != stream ||
			key.Direction != DirectionOutbound ||
			(frame.Status != StatusPending && frame.Status != StatusSent) {
			continue
		}
		memoryFrames = append(memoryFrames, frame.Clone())
	}
	s.mu.Unlock()

	sinkFrames, err := s.sink.ListOutboundReplay(ctx, queueID, stream, 1_000_000)
	if err != nil {
		return ProducerReconcileCheckpoint{}, err
	}
	replayFrom, replayThrough := replayBounds(append(sinkFrames, memoryFrames...))
	return ProducerReconcileCheckpoint{
		QueueID:         queueID,
		Stream:          stream,
		ProducerNextSeq: state.nextOutboundSeq,
		ReplayFrom:      replayFrom,
		ReplayThrough:   replayThrough,
	}, nil
}

func (s *ProducerWriteBehindStore) AdvanceProducerNextSeq(
	ctx context.Context,
	queueID string,
	stream Stream,
	nextSeq int64,
) error {
	if nextSeq <= 0 {
		return fmt.Errorf("%w: next seq must be positive", ErrInvalidFrame)
	}
	if advancer, ok := s.sink.(ReconcileProducerStore); ok {
		if err := advancer.AdvanceProducerNextSeq(ctx, queueID, stream, nextSeq); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrProducerStoreClosed
	}
	key := producerQueueKey{queueID: queueID, stream: stream}
	state := s.queues[key]
	if !state.loaded || state.nextOutboundSeq < nextSeq {
		state.nextOutboundSeq = nextSeq
		state.loaded = true
		s.queues[key] = state
	}
	throughSeq := nextSeq - 1
	now := s.config.Now().UTC()
	for frameKey, frame := range s.frames {
		if frameKey.QueueID != queueID ||
			frameKey.Stream != stream ||
			frameKey.Direction != DirectionOutbound ||
			frameKey.Seq > throughSeq {
			continue
		}
		frame.Status = StatusAcked
		frame.ErrorMessage = ""
		frame.UpdatedAt = now
		s.frames[frameKey] = frame.Clone()
		if _, dirty := s.dirtyFrames[frameKey]; dirty {
			s.dirtyFrames[frameKey] = frame.Clone()
		}
	}
	patchKey := FrameKey{
		QueueID:   queueID,
		Stream:    stream,
		Seq:       throughSeq,
		Direction: DirectionOutbound,
	}
	if throughSeq > 0 {
		s.dirtyPatches[patchKey] = StorePatch{Key: patchKey, Status: StatusAcked}
	}
	s.updateDirtyStatsLocked()
	s.notifyFlushLocked()
	return nil
}

func (s *ProducerWriteBehindStore) MarkSent(ctx context.Context, key FrameKey) error {
	_ = ctx
	if key.Direction != DirectionOutbound {
		return s.sink.MarkSent(ctx, key)
	}
	return s.markOutbound(key, StatusSent, "", nil, false)
}

func (s *ProducerWriteBehindStore) AckOutboundThrough(
	ctx context.Context,
	queueID string,
	stream Stream,
	throughSeq int64,
) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrProducerStoreClosed
	}
	now := s.config.Now().UTC()
	for key, frame := range s.frames {
		if key.QueueID != queueID ||
			key.Stream != stream ||
			key.Direction != DirectionOutbound ||
			key.Seq > throughSeq {
			continue
		}
		frame.Status = StatusAcked
		frame.ErrorMessage = ""
		frame.UpdatedAt = now
		s.frames[key] = frame.Clone()
		if _, dirty := s.dirtyFrames[key]; dirty {
			s.dirtyFrames[key] = frame.Clone()
		}
	}
	patchKey := FrameKey{
		QueueID:   queueID,
		Stream:    stream,
		Seq:       throughSeq,
		Direction: DirectionOutbound,
	}
	s.dirtyPatches[patchKey] = StorePatch{Key: patchKey, Status: StatusAcked}
	s.updateDirtyStatsLocked()
	s.notifyFlushLocked()
	return nil
}

func (s *ProducerWriteBehindStore) MarkApplied(ctx context.Context, key FrameKey) error {
	return s.sink.MarkApplied(ctx, key)
}

func (s *ProducerWriteBehindStore) MarkRejected(
	ctx context.Context,
	key FrameKey,
	errorMessage string,
) error {
	return s.sink.MarkRejected(ctx, key, errorMessage)
}

func (s *ProducerWriteBehindStore) RecordSendFailure(
	ctx context.Context,
	key FrameKey,
	errorMessage string,
) error {
	_ = ctx
	if key.Direction != DirectionOutbound {
		return s.sink.RecordSendFailure(ctx, key, errorMessage)
	}
	return s.markOutbound(key, "", errorMessage, nil, false)
}

func (s *ProducerWriteBehindStore) RecordDispatchFailure(
	ctx context.Context,
	key FrameKey,
	errorMessage string,
) error {
	return s.sink.RecordDispatchFailure(ctx, key, errorMessage)
}

func (s *ProducerWriteBehindStore) UpdateMetadata(
	ctx context.Context,
	key FrameKey,
	metadata Metadata,
) error {
	_ = ctx
	if key.Direction != DirectionOutbound {
		return s.sink.UpdateMetadata(ctx, key, metadata)
	}
	return s.markOutbound(key, "", "", metadata, true)
}

func (s *ProducerWriteBehindStore) Flush(ctx context.Context) error {
	snapshot := s.snapshot()
	if snapshot.empty() {
		return nil
	}
	if err := s.flushSnapshot(ctx, snapshot); err != nil {
		s.recordFlushFailure(snapshot, err)
		return err
	}
	s.recordFlushSuccess()
	return nil
}

func (s *ProducerWriteBehindStore) Close(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
		<-s.done
	}
	err := s.Flush(ctx)
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return err
}

func (s *ProducerWriteBehindStore) Stats() ProducerWriteBehindStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *ProducerWriteBehindStore) appendOutbound(
	ctx context.Context,
	queueID string,
	stream Stream,
	kind FrameKind,
	payload json.RawMessage,
	errorMessage string,
	metadata Metadata,
) (Frame, error) {
	if queueID == "" {
		return Frame{}, fmt.Errorf("%w: queue_id is required", ErrInvalidFrame)
	}
	if stream == "" {
		return Frame{}, fmt.Errorf("%w: stream is required", ErrInvalidFrame)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Frame{}, ErrProducerStoreClosed
	}
	state, err := s.queueStateLocked(ctx, queueID, stream)
	if err != nil {
		return Frame{}, err
	}
	seq := state.nextOutboundSeq
	state.nextOutboundSeq++
	s.queues[producerQueueKey{queueID: queueID, stream: stream}] = state

	now := s.config.Now().UTC()
	frame := Frame{
		Key: FrameKey{
			QueueID:   queueID,
			Stream:    stream,
			Seq:       seq,
			Direction: DirectionOutbound,
		},
		Kind:         kind,
		Payload:      append(json.RawMessage(nil), payload...),
		Metadata:     metadata.Clone(),
		Status:       StatusPending,
		ErrorMessage: errorMessage,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := ValidateFrame(frame); err != nil {
		return Frame{}, err
	}
	s.frames[frame.Key] = frame.Clone()
	s.dirtyFrames[frame.Key] = frame.Clone()
	s.updateDirtyStatsLocked()
	s.notifyFlushLocked()
	return frame.Clone(), nil
}

func (s *ProducerWriteBehindStore) queueStateLocked(
	ctx context.Context,
	queueID string,
	stream Stream,
) (producerQueueState, error) {
	key := producerQueueKey{queueID: queueID, stream: stream}
	state := s.queues[key]
	if state.loaded {
		return state, nil
	}
	state.nextOutboundSeq = 1
	if loader, ok := s.sink.(QueueStateStore); ok {
		loaded, err := loader.LoadQueueState(ctx, queueID, stream)
		if err != nil {
			return producerQueueState{}, err
		}
		if loaded.NextOutboundSeq > 0 {
			state.nextOutboundSeq = loaded.NextOutboundSeq
		}
	}
	state.loaded = true
	s.queues[key] = state
	return state, nil
}

func (s *ProducerWriteBehindStore) markOutbound(
	key FrameKey,
	status Status,
	errorMessage string,
	metadata Metadata,
	hasMetadata bool,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrProducerStoreClosed
	}
	frame, ok := s.frames[key]
	if ok {
		if status != "" {
			frame.Status = status
		}
		if errorMessage != "" {
			frame.ErrorMessage = errorMessage
		} else if status == StatusSent || status == StatusAcked {
			frame.ErrorMessage = ""
		}
		if hasMetadata {
			frame.Metadata = metadata.Clone()
		}
		frame.UpdatedAt = s.config.Now().UTC()
		s.frames[key] = frame.Clone()
		if _, dirty := s.dirtyFrames[key]; dirty {
			s.dirtyFrames[key] = frame.Clone()
			delete(s.dirtyPatches, key)
			s.updateDirtyStatsLocked()
			s.notifyFlushLocked()
			return nil
		}
	}

	patch := s.dirtyPatches[key]
	patch.Key = key
	if status != "" {
		patch.Status = status
	}
	if errorMessage != "" {
		patch.ErrorMessage = errorMessage
	}
	if hasMetadata {
		patch.Metadata = metadata.Clone()
		patch.HasMetadata = true
	}
	s.dirtyPatches[key] = patch
	s.updateDirtyStatsLocked()
	s.notifyFlushLocked()
	return nil
}

func (s *ProducerWriteBehindStore) run(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(s.config.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.notify:
			s.flushWhenReady(ctx)
		case <-ticker.C:
			s.flushWhenReady(ctx)
		}
	}
}

func (s *ProducerWriteBehindStore) flushWhenReady(ctx context.Context) {
	s.mu.Lock()
	retryAt := s.retryAt
	now := s.config.Now()
	s.mu.Unlock()
	if !retryAt.IsZero() && now.Before(retryAt) {
		return
	}
	_ = s.Flush(ctx)
}

type producerWriteBehindSnapshot struct {
	frames  []Frame
	patches []StorePatch
}

func (s producerWriteBehindSnapshot) empty() bool {
	return len(s.frames) == 0 && len(s.patches) == 0
}

func (s producerWriteBehindSnapshot) batch() StoreBatch {
	return StoreBatch{Frames: s.frames, Patches: s.patches}
}

func (s *ProducerWriteBehindStore) snapshot() producerWriteBehindSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	frames := make([]Frame, 0, len(s.dirtyFrames))
	for _, frame := range s.dirtyFrames {
		frames = append(frames, frame.Clone())
	}
	sort.Slice(frames, func(i, j int) bool {
		return frames[i].Key.Seq < frames[j].Key.Seq
	})
	patches := make([]StorePatch, 0, len(s.dirtyPatches))
	for _, patch := range s.dirtyPatches {
		patch.Metadata = patch.Metadata.Clone()
		patches = append(patches, patch)
	}
	sort.Slice(patches, func(i, j int) bool {
		return patches[i].Key.Seq < patches[j].Key.Seq
	})
	s.dirtyFrames = make(map[FrameKey]Frame)
	s.dirtyPatches = make(map[FrameKey]StorePatch)
	s.updateDirtyStatsLocked()
	return producerWriteBehindSnapshot{frames: frames, patches: patches}
}

func (s *ProducerWriteBehindStore) flushSnapshot(
	ctx context.Context,
	snapshot producerWriteBehindSnapshot,
) error {
	if batcher, ok := s.sink.(BatchStore); ok {
		return batcher.ApplyBatch(ctx, snapshot.batch())
	}
	if s.config.RequireBatch {
		return ErrProducerBatchStoreRequired
	}
	for _, frame := range snapshot.frames {
		if err := s.flushFrame(ctx, frame); err != nil {
			return err
		}
	}
	for _, patch := range snapshot.patches {
		if err := s.flushPatch(ctx, patch); err != nil {
			return err
		}
	}
	return nil
}

func (s *ProducerWriteBehindStore) flushFrame(ctx context.Context, frame Frame) error {
	var stored Frame
	var err error
	switch frame.Kind {
	case FrameKindData:
		stored, err = s.sink.AppendOutboundData(ctx, frame.Key.QueueID, frame.Key.Stream, frame.Payload, frame.Metadata)
	case FrameKindTombstone:
		stored, err = s.sink.AppendOutboundTombstone(ctx, frame.Key.QueueID, frame.Key.Stream, frame.ErrorMessage, frame.Metadata)
	default:
		return fmt.Errorf("%w: invalid frame kind %q", ErrInvalidFrame, frame.Kind)
	}
	if err != nil {
		return err
	}
	if stored.Key != frame.Key {
		return fmt.Errorf("reliablemq: producer write-behind sink allocated frame %+v, want %+v", stored.Key, frame.Key)
	}
	return s.applyFrameState(ctx, frame)
}

func (s *ProducerWriteBehindStore) applyFrameState(ctx context.Context, frame Frame) error {
	if len(frame.Metadata) > 0 {
		if err := s.sink.UpdateMetadata(ctx, frame.Key, frame.Metadata); err != nil {
			return err
		}
	}
	switch frame.Status {
	case StatusPending:
	case StatusSent:
		if err := s.sink.MarkSent(ctx, frame.Key); err != nil {
			return err
		}
	case StatusAcked:
		if err := s.sink.AckOutboundThrough(ctx, frame.Key.QueueID, frame.Key.Stream, frame.Key.Seq); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: invalid outbound status %q", ErrInvalidFrame, frame.Status)
	}
	if frame.ErrorMessage != "" {
		return s.sink.RecordSendFailure(ctx, frame.Key, frame.ErrorMessage)
	}
	return nil
}

func (s *ProducerWriteBehindStore) flushPatch(ctx context.Context, patch StorePatch) error {
	if patch.HasMetadata {
		if err := s.sink.UpdateMetadata(ctx, patch.Key, patch.Metadata); err != nil {
			return err
		}
	}
	switch patch.Status {
	case "":
	case StatusSent:
		if err := s.sink.MarkSent(ctx, patch.Key); err != nil {
			return err
		}
	case StatusAcked:
		if err := s.sink.AckOutboundThrough(ctx, patch.Key.QueueID, patch.Key.Stream, patch.Key.Seq); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: invalid outbound patch status %q", ErrInvalidFrame, patch.Status)
	}
	if patch.ErrorMessage != "" {
		return s.sink.RecordSendFailure(ctx, patch.Key, patch.ErrorMessage)
	}
	return nil
}

func (s *ProducerWriteBehindStore) recordFlushFailure(
	snapshot producerWriteBehindSnapshot,
	err error,
) {
	s.mu.Lock()
	for _, frame := range snapshot.frames {
		if _, ok := s.dirtyFrames[frame.Key]; !ok {
			s.dirtyFrames[frame.Key] = frame.Clone()
		}
	}
	for _, patch := range snapshot.patches {
		if _, ok := s.dirtyPatches[patch.Key]; !ok {
			patch.Metadata = patch.Metadata.Clone()
			s.dirtyPatches[patch.Key] = patch
		}
	}
	s.stats.Degraded = true
	s.stats.ConsecutiveFlushFailures++
	s.stats.LastFlushError = err.Error()
	s.stats.LastFlushFailureAt = s.config.Now().UTC()
	s.retryAt = s.config.Now().Add(s.nextRetryDelayLocked())
	s.updateDirtyStatsLocked()
	stats := s.stats
	s.mu.Unlock()

	if s.config.OnFlushFailure != nil {
		s.config.OnFlushFailure(err, stats)
	}
}

func (s *ProducerWriteBehindStore) recordFlushSuccess() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.Degraded = false
	s.stats.ConsecutiveFlushFailures = 0
	s.stats.LastFlushError = ""
	s.stats.LastFlushAt = s.config.Now().UTC()
	s.retryAt = time.Time{}
	s.updateDirtyStatsLocked()
}

func (s *ProducerWriteBehindStore) nextRetryDelayLocked() time.Duration {
	failures := s.stats.ConsecutiveFlushFailures
	switch {
	case failures <= 1:
		return 100 * time.Millisecond
	case failures == 2:
		return 250 * time.Millisecond
	case failures == 3:
		return 500 * time.Millisecond
	case failures == 4:
		return time.Second
	default:
		return 2 * time.Second
	}
}

func (s *ProducerWriteBehindStore) updateDirtyStatsLocked() {
	s.stats.DirtyFrames = len(s.dirtyFrames)
	s.stats.DirtyPatches = len(s.dirtyPatches)
	var bytes int64
	for _, frame := range s.dirtyFrames {
		bytes += int64(len(frame.Payload) + len(frame.ErrorMessage))
		for key, value := range frame.Metadata {
			bytes += int64(len(key) + len(value))
		}
	}
	for _, patch := range s.dirtyPatches {
		bytes += int64(len(patch.ErrorMessage))
		for key, value := range patch.Metadata {
			bytes += int64(len(key) + len(value))
		}
	}
	s.stats.DirtyBytes = bytes
}

func (s *ProducerWriteBehindStore) notifyFlushLocked() {
	if s.config.ManualFlush {
		return
	}
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func replayBounds(frames []Frame) (int64, int64) {
	var from int64
	var through int64
	for _, frame := range frames {
		if frame.Key.Direction != DirectionOutbound {
			continue
		}
		if from == 0 || frame.Key.Seq < from {
			from = frame.Key.Seq
		}
		if frame.Key.Seq > through {
			through = frame.Key.Seq
		}
	}
	return from, through
}

var _ DurableStore = (*ProducerWriteBehindStore)(nil)
var _ ReconcileProducerStore = (*ProducerWriteBehindStore)(nil)
