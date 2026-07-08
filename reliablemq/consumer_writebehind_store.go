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
	ErrConsumerStoreClosed        = errors.New("reliablemq: consumer write-behind store is closed")
	ErrConsumerBatchStoreRequired = errors.New("reliablemq: consumer write-behind sink must implement batch store")
)

type ConsumerWriteBehindStoreConfig struct {
	FlushInterval  time.Duration
	ManualFlush    bool
	RequireBatch   bool
	Now            func() time.Time
	OnFlushFailure func(error, ConsumerWriteBehindStats)
}

type ConsumerWriteBehindStoreOption interface {
	applyConsumerWriteBehind(*ConsumerWriteBehindStoreConfig)
}

type consumerWriteBehindOptionFunc func(*ConsumerWriteBehindStoreConfig)

func (fn consumerWriteBehindOptionFunc) applyConsumerWriteBehind(config *ConsumerWriteBehindStoreConfig) {
	fn(config)
}

func WithConsumerWriteBehindFlushInterval(interval time.Duration) ConsumerWriteBehindStoreOption {
	return consumerWriteBehindOptionFunc(func(config *ConsumerWriteBehindStoreConfig) {
		if interval > 0 {
			config.FlushInterval = interval
		}
	})
}

func WithConsumerWriteBehindManualFlush() ConsumerWriteBehindStoreOption {
	return consumerWriteBehindOptionFunc(func(config *ConsumerWriteBehindStoreConfig) {
		config.ManualFlush = true
	})
}

func WithConsumerWriteBehindRequireBatchStore() ConsumerWriteBehindStoreOption {
	return consumerWriteBehindOptionFunc(func(config *ConsumerWriteBehindStoreConfig) {
		config.RequireBatch = true
	})
}

func WithConsumerWriteBehindClock(now func() time.Time) ConsumerWriteBehindStoreOption {
	return consumerWriteBehindOptionFunc(func(config *ConsumerWriteBehindStoreConfig) {
		if now != nil {
			config.Now = now
		}
	})
}

func WithConsumerWriteBehindFlushFailureHandler(
	handler func(error, ConsumerWriteBehindStats),
) ConsumerWriteBehindStoreOption {
	return consumerWriteBehindOptionFunc(func(config *ConsumerWriteBehindStoreConfig) {
		config.OnFlushFailure = handler
	})
}

type ConsumerWriteBehindStats struct {
	DirtyFrames              int
	DirtyPatches             int
	DirtyBytes               int64
	Degraded                 bool
	ConsecutiveFlushFailures int
	LastFlushError           string
	LastFlushAt              time.Time
	LastFlushFailureAt       time.Time
}

type ConsumerWriteBehindStore struct {
	sink   DurableStore
	config ConsumerWriteBehindStoreConfig

	mu           sync.Mutex
	frames       map[FrameKey]Frame
	dirtyFrames  map[FrameKey]Frame
	dirtyPatches map[FrameKey]StorePatch
	closed       bool
	stats        ConsumerWriteBehindStats
	retryAt      time.Time

	notify chan struct{}
	done   chan struct{}
	cancel context.CancelFunc
}

func NewConsumerWriteBehindStore(
	sink DurableStore,
	opts ...ConsumerWriteBehindStoreOption,
) *ConsumerWriteBehindStore {
	config := ConsumerWriteBehindStoreConfig{
		FlushInterval: time.Second,
		Now:           time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt.applyConsumerWriteBehind(&config)
		}
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	store := &ConsumerWriteBehindStore{
		sink:         sink,
		config:       config,
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

func (s *ConsumerWriteBehindStore) AppendOutboundData(
	ctx context.Context,
	queueID string,
	stream Stream,
	payload json.RawMessage,
	metadata Metadata,
) (Frame, error) {
	return s.sink.AppendOutboundData(ctx, queueID, stream, payload, metadata)
}

func (s *ConsumerWriteBehindStore) AppendOutboundTombstone(
	ctx context.Context,
	queueID string,
	stream Stream,
	errorMessage string,
	metadata Metadata,
) (Frame, error) {
	return s.sink.AppendOutboundTombstone(ctx, queueID, stream, errorMessage, metadata)
}

func (s *ConsumerWriteBehindStore) SaveInboundIfAbsent(ctx context.Context, frame Frame) (bool, Frame, error) {
	_ = ctx
	if err := ValidateFrame(frame); err != nil {
		return false, Frame{}, err
	}
	if frame.Key.Direction != DirectionInbound {
		return false, Frame{}, fmt.Errorf("%w: inbound frame direction is %q", ErrInvalidFrame, frame.Key.Direction)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, Frame{}, ErrConsumerStoreClosed
	}
	if existing, ok := s.frames[frame.Key]; ok {
		return false, existing.Clone(), nil
	}

	now := s.config.Now().UTC()
	frame = frame.Clone()
	frame.Status = StatusReceived
	frame.CreatedAt = now
	frame.UpdatedAt = now
	s.frames[frame.Key] = frame
	s.dirtyFrames[frame.Key] = frame.Clone()
	s.updateDirtyStatsLocked()
	s.notifyFlushLocked()
	return true, frame.Clone(), nil
}

func (s *ConsumerWriteBehindStore) ListOutboundReplay(
	ctx context.Context,
	queueID string,
	stream Stream,
	limit int,
) ([]Frame, error) {
	return s.sink.ListOutboundReplay(ctx, queueID, stream, limit)
}

func (s *ConsumerWriteBehindStore) ListInboundReplay(
	ctx context.Context,
	queueID string,
	stream Stream,
	limit int,
) ([]Frame, error) {
	frames, err := s.sink.ListInboundReplay(ctx, queueID, stream, limit)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	seen := make(map[FrameKey]bool, len(frames))
	for _, frame := range frames {
		seen[frame.Key] = true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, frame := range s.frames {
		if len(frames) >= limit {
			break
		}
		if frame.Key.QueueID != queueID ||
			frame.Key.Stream != stream ||
			frame.Key.Direction != DirectionInbound ||
			frame.Status != StatusReceived ||
			seen[frame.Key] {
			continue
		}
		frames = append(frames, frame.Clone())
	}
	return frames, nil
}

func (s *ConsumerWriteBehindStore) MarkSent(ctx context.Context, key FrameKey) error {
	return s.sink.MarkSent(ctx, key)
}

func (s *ConsumerWriteBehindStore) AckOutboundThrough(
	ctx context.Context,
	queueID string,
	stream Stream,
	throughSeq int64,
) error {
	return s.sink.AckOutboundThrough(ctx, queueID, stream, throughSeq)
}

func (s *ConsumerWriteBehindStore) ConsumerAckedThrough(
	ctx context.Context,
	queueID string,
	stream Stream,
) (int64, error) {
	var through int64
	if checker, ok := s.sink.(ReconcileConsumerStore); ok {
		var err error
		through, err = checker.ConsumerAckedThrough(ctx, queueID, stream)
		if err != nil {
			return 0, err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	next := through + 1
	for {
		frame, ok := s.frames[FrameKey{
			QueueID:   queueID,
			Stream:    stream,
			Seq:       next,
			Direction: DirectionInbound,
		}]
		if !ok || !isConsumerAckedStatus(frame.Status) {
			break
		}
		through = next
		next++
	}
	return through, nil
}

func (s *ConsumerWriteBehindStore) MarkApplied(ctx context.Context, key FrameKey) error {
	_ = ctx
	return s.markInbound(key, StatusApplied, "", nil, false)
}

func (s *ConsumerWriteBehindStore) MarkRejected(
	ctx context.Context,
	key FrameKey,
	errorMessage string,
) error {
	_ = ctx
	return s.markInbound(key, StatusRejected, errorMessage, nil, false)
}

func (s *ConsumerWriteBehindStore) RecordSendFailure(
	ctx context.Context,
	key FrameKey,
	errorMessage string,
) error {
	return s.sink.RecordSendFailure(ctx, key, errorMessage)
}

func (s *ConsumerWriteBehindStore) RecordDispatchFailure(
	ctx context.Context,
	key FrameKey,
	errorMessage string,
) error {
	_ = ctx
	return s.markInbound(key, "", errorMessage, nil, false)
}

func (s *ConsumerWriteBehindStore) UpdateMetadata(
	ctx context.Context,
	key FrameKey,
	metadata Metadata,
) error {
	_ = ctx
	return s.markInbound(key, "", "", metadata, true)
}

func (s *ConsumerWriteBehindStore) Flush(ctx context.Context) error {
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

func (s *ConsumerWriteBehindStore) Close(ctx context.Context) error {
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

func (s *ConsumerWriteBehindStore) Stats() ConsumerWriteBehindStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *ConsumerWriteBehindStore) markInbound(
	key FrameKey,
	status Status,
	errorMessage string,
	metadata Metadata,
	hasMetadata bool,
) error {
	if key.Direction != DirectionInbound {
		return s.applyOutboundPatch(context.Background(), key, status, errorMessage, metadata, hasMetadata)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrConsumerStoreClosed
	}
	frame, ok := s.frames[key]
	if !ok {
		frame = Frame{
			Key:    key,
			Kind:   FrameKindData,
			Status: StatusReceived,
		}
	}
	if status != "" {
		frame.Status = status
	}
	if errorMessage != "" {
		frame.ErrorMessage = errorMessage
	} else if status == StatusApplied || status == StatusRejected {
		frame.ErrorMessage = errorMessage
	}
	if hasMetadata {
		frame.Metadata = metadata.Clone()
	}
	frame.UpdatedAt = s.config.Now().UTC()
	s.frames[key] = frame.Clone()
	if _, dirty := s.dirtyFrames[key]; dirty {
		s.dirtyFrames[key] = frame.Clone()
		delete(s.dirtyPatches, key)
	} else {
		patch := s.dirtyPatches[key]
		patch.Key = key
		if status != "" {
			patch.Status = status
		}
		if errorMessage != "" || status == StatusRejected {
			patch.ErrorMessage = errorMessage
		}
		if hasMetadata {
			patch.Metadata = metadata.Clone()
			patch.HasMetadata = true
		}
		s.dirtyPatches[key] = patch
	}
	s.updateDirtyStatsLocked()
	s.notifyFlushLocked()
	return nil
}

func (s *ConsumerWriteBehindStore) applyOutboundPatch(
	ctx context.Context,
	key FrameKey,
	status Status,
	errorMessage string,
	metadata Metadata,
	hasMetadata bool,
) error {
	if hasMetadata {
		return s.sink.UpdateMetadata(ctx, key, metadata)
	}
	switch status {
	case "":
		if errorMessage != "" {
			return s.sink.RecordSendFailure(ctx, key, errorMessage)
		}
		return nil
	case StatusSent:
		return s.sink.MarkSent(ctx, key)
	case StatusApplied:
		return s.sink.MarkApplied(ctx, key)
	case StatusRejected:
		return s.sink.MarkRejected(ctx, key, errorMessage)
	default:
		return fmt.Errorf("%w: invalid outbound status %q", ErrInvalidFrame, status)
	}
}

func (s *ConsumerWriteBehindStore) run(ctx context.Context) {
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

func (s *ConsumerWriteBehindStore) flushWhenReady(ctx context.Context) {
	s.mu.Lock()
	retryAt := s.retryAt
	now := s.config.Now()
	s.mu.Unlock()
	if !retryAt.IsZero() && now.Before(retryAt) {
		return
	}
	_ = s.Flush(ctx)
}

type consumerWriteBehindSnapshot struct {
	frames  []Frame
	patches []StorePatch
}

func (s consumerWriteBehindSnapshot) empty() bool {
	return len(s.frames) == 0 && len(s.patches) == 0
}

func (s consumerWriteBehindSnapshot) batch() StoreBatch {
	return StoreBatch{Frames: s.frames, Patches: s.patches}
}

func (s *ConsumerWriteBehindStore) snapshot() consumerWriteBehindSnapshot {
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
	return consumerWriteBehindSnapshot{frames: frames, patches: patches}
}

func (s *ConsumerWriteBehindStore) flushSnapshot(
	ctx context.Context,
	snapshot consumerWriteBehindSnapshot,
) error {
	batcher, ok := s.sink.(BatchStore)
	if ok {
		return batcher.ApplyBatch(ctx, snapshot.batch())
	}
	if s.config.RequireBatch {
		return ErrConsumerBatchStoreRequired
	}
	for _, frame := range snapshot.frames {
		if _, _, err := s.sink.SaveInboundIfAbsent(ctx, frame); err != nil {
			return err
		}
		if err := s.applyFrameStatus(ctx, frame); err != nil {
			return err
		}
	}
	for _, patch := range snapshot.patches {
		if err := s.applyPatch(ctx, patch); err != nil {
			return err
		}
	}
	return nil
}

func (s *ConsumerWriteBehindStore) applyFrameStatus(ctx context.Context, frame Frame) error {
	if frame.Metadata != nil {
		if err := s.sink.UpdateMetadata(ctx, frame.Key, frame.Metadata); err != nil {
			return err
		}
	}
	switch frame.Status {
	case StatusReceived:
	case StatusApplied:
		if err := s.sink.MarkApplied(ctx, frame.Key); err != nil {
			return err
		}
	case StatusRejected:
		if err := s.sink.MarkRejected(ctx, frame.Key, frame.ErrorMessage); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: invalid inbound status %q", ErrInvalidFrame, frame.Status)
	}
	if frame.ErrorMessage != "" && frame.Status != StatusRejected {
		return s.sink.RecordDispatchFailure(ctx, frame.Key, frame.ErrorMessage)
	}
	return nil
}

func (s *ConsumerWriteBehindStore) applyPatch(ctx context.Context, patch StorePatch) error {
	if patch.HasMetadata {
		if err := s.sink.UpdateMetadata(ctx, patch.Key, patch.Metadata); err != nil {
			return err
		}
	}
	switch patch.Status {
	case "":
	case StatusApplied:
		if err := s.sink.MarkApplied(ctx, patch.Key); err != nil {
			return err
		}
	case StatusRejected:
		if err := s.sink.MarkRejected(ctx, patch.Key, patch.ErrorMessage); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: invalid inbound patch status %q", ErrInvalidFrame, patch.Status)
	}
	if patch.ErrorMessage != "" {
		return s.sink.RecordDispatchFailure(ctx, patch.Key, patch.ErrorMessage)
	}
	return nil
}

func (s *ConsumerWriteBehindStore) recordFlushFailure(
	snapshot consumerWriteBehindSnapshot,
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

func (s *ConsumerWriteBehindStore) recordFlushSuccess() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.Degraded = false
	s.stats.ConsecutiveFlushFailures = 0
	s.stats.LastFlushError = ""
	s.stats.LastFlushAt = s.config.Now().UTC()
	s.retryAt = time.Time{}
	s.updateDirtyStatsLocked()
}

func (s *ConsumerWriteBehindStore) nextRetryDelayLocked() time.Duration {
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

func (s *ConsumerWriteBehindStore) updateDirtyStatsLocked() {
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

func (s *ConsumerWriteBehindStore) notifyFlushLocked() {
	if s.config.ManualFlush {
		return
	}
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func isConsumerAckedStatus(status Status) bool {
	return status == StatusReceived || status == StatusApplied || status == StatusRejected
}

var _ DurableStore = (*ConsumerWriteBehindStore)(nil)
var _ ReconcileConsumerStore = (*ConsumerWriteBehindStore)(nil)
