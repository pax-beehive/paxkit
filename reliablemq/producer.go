package reliablemq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	producerStateInitializing int32 = iota
	producerStateReady
	producerStateClosing
	producerStateClosed
	producerStateFailed
)

type ProducerConfig struct {
	QueueID             string
	Stream              Stream
	CursorBatchSize     int
	MaintenanceEvery    time.Duration
	MaxUnpersistedBytes int64
	MaxUnpersistedAge   time.Duration
	Now                 func() time.Time
	OnError             func(error)
}

type ProducerStats struct {
	Ready                bool
	Bound                bool
	BindingGeneration    uint64
	Tail                 int64
	PersistedThrough     int64
	NextToSend           int64
	AckedThrough         int64
	PendingACKThrough    int64
	HotFrames            int64
	UnpersistedBytes     int64
	OldestUnpersistedAge time.Duration
	AcceptedPending      int64
	LastError            string
}

// Producer owns one queue_id + stream outbound sequence. Send acceptance uses
// only atomics and an MPSC append; one owner goroutine sequences entries while
// journal flushing and socket writing advance independently.
type Producer struct {
	config ProducerConfig
	store  DurableStore

	ingress   *producerIngressQueue
	wake      chan struct{}
	commands  chan any
	writeDone chan producerWriteResult
	loadDone  chan producerLoadResult
	progress  chan struct{}
	done      chan struct{}
	cancel    context.CancelFunc

	state      atomic.Int32
	accepting  atomic.Int64
	peerACK    atomic.Int64
	inboundACK atomic.Int64
	ackSignal  atomic.Uint64

	tailStat                 atomic.Int64
	persistedStat            atomic.Int64
	nextToSendStat           atomic.Int64
	ackedStat                atomic.Int64
	pendingACKStat           atomic.Int64
	hotStat                  atomic.Int64
	unpersistedBytesStat     atomic.Int64
	oldestUnpersistedAgeStat atomic.Int64
	generationStat           atomic.Uint64
	boundStat                atomic.Bool
	lastError                atomic.Value
}

type ProducerBinding struct {
	producer   *Producer
	generation uint64
	targetTail int64
	lifecycle  *producerBindingLifecycle
	once       sync.Once
}

type producerBindingLifecycle struct {
	done chan struct{}
	once sync.Once
	mu   sync.RWMutex
	err  error
}

type acceptedOutbound struct {
	kind         FrameKind
	payload      []byte
	metadata     Metadata
	errorMessage string
}

type producerOwnerState struct {
	nextSeq          int64
	tail             int64
	persistedThrough int64
	nextToSend       int64
	ackedThrough     int64
	pendingACK       int64
	lastACKSent      int64
	pendingACKSignal uint64
	lastACKSignal    uint64
	hot              map[int64]Frame
	cursorLoaded     map[int64]struct{}
	unpersisted      map[int64]producerUnpersistedEntry
	unpersistedBytes int64
	binding          *producerWriterBinding
	networkInFlight  bool
	loadInFlight     bool
	loadFrom         int64
	nextGeneration   uint64
}

type producerUnpersistedEntry struct {
	bytes      int64
	acceptedAt time.Time
}

type producerWriterBinding struct {
	generation uint64
	requests   chan producerWriteRequest
	cancel     context.CancelFunc
	lifecycle  *producerBindingLifecycle
}

type producerWriteKind uint8

const (
	producerWriteData producerWriteKind = iota + 1
	producerWriteACK
)

type producerWriteRequest struct {
	generation uint64
	kind       producerWriteKind
	seq        int64
	ackSignal  uint64
	envelope   Envelope
}

type producerWriteResult struct {
	request producerWriteRequest
	err     error
}

type producerLoadResult struct {
	from   int64
	frames []Frame
	err    error
}

type producerBindCommand struct {
	ctx              context.Context
	sender           Sender
	peerAckedThrough int64
	response         chan producerBindResult
}

type producerBindResult struct {
	binding *ProducerBinding
	err     error
}

type producerCheckpointCommand struct {
	response chan producerCheckpointResult
}

type producerCheckpointResult struct {
	checkpoint ProducerReconcileCheckpoint
	err        error
}

type producerAdvanceCommand struct {
	ctx      context.Context
	nextSeq  int64
	response chan error
}

type producerUnbindCommand struct {
	generation uint64
}

type producerErrorValue struct {
	err error
}

func NewProducer(ctx context.Context, config ProducerConfig, store DurableStore) (*Producer, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config.QueueID == "" {
		return nil, fmt.Errorf("%w: queue_id is required", ErrInvalidFrame)
	}
	if config.Stream == "" {
		return nil, fmt.Errorf("%w: stream is required", ErrInvalidFrame)
	}
	if store == nil {
		return nil, fmt.Errorf("reliablemq: producer store is required")
	}
	if config.CursorBatchSize <= 0 {
		config.CursorBatchSize = 256
	}
	if config.MaintenanceEvery <= 0 {
		config.MaintenanceEvery = 10 * time.Millisecond
	}
	if config.MaxUnpersistedBytes <= 0 {
		config.MaxUnpersistedBytes = 64 << 20
	}
	if config.MaxUnpersistedAge <= 0 {
		config.MaxUnpersistedAge = 30 * time.Second
	}
	if config.Now == nil {
		config.Now = time.Now
	}

	checkpoint, err := bootstrapProducer(ctx, store, config.QueueID, config.Stream)
	if err != nil {
		return nil, err
	}
	nextSeq := checkpoint.ProducerNextSeq
	if nextSeq <= 0 {
		nextSeq = 1
	}
	tail := nextSeq - 1
	if checkpoint.ReplayThrough > tail {
		tail = checkpoint.ReplayThrough
		nextSeq = tail + 1
	}
	ackedThrough := tail
	if checkpoint.ReplayFrom > 0 {
		ackedThrough = checkpoint.ReplayFrom - 1
	}
	if ackedThrough < 0 {
		ackedThrough = 0
	}
	persistedThrough := tail
	if persistence, ok := store.(ProducerPersistence); ok {
		persistedThrough = persistence.PersistedThrough(config.QueueID, config.Stream)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	p := &Producer{
		config:    config,
		store:     store,
		ingress:   newProducerIngressQueue(),
		wake:      make(chan struct{}, 1),
		commands:  make(chan any, 16),
		writeDone: make(chan producerWriteResult, 8),
		loadDone:  make(chan producerLoadResult, 2),
		progress:  make(chan struct{}, 1),
		done:      make(chan struct{}),
		cancel:    cancel,
	}
	p.tailStat.Store(tail)
	p.persistedStat.Store(persistedThrough)
	p.nextToSendStat.Store(ackedThrough + 1)
	p.ackedStat.Store(ackedThrough)
	p.state.Store(producerStateReady)
	go p.run(runCtx, producerOwnerState{
		nextSeq:          nextSeq,
		tail:             tail,
		persistedThrough: persistedThrough,
		nextToSend:       ackedThrough + 1,
		ackedThrough:     ackedThrough,
		hot:              make(map[int64]Frame),
		cursorLoaded:     make(map[int64]struct{}),
		unpersisted:      make(map[int64]producerUnpersistedEntry),
	})
	return p, nil
}

func bootstrapProducer(
	ctx context.Context,
	store DurableStore,
	queueID string,
	stream Stream,
) (ProducerReconcileCheckpoint, error) {
	if reconciler, ok := store.(ReconcileProducerStore); ok {
		checkpoint, err := reconciler.LoadProducerReconcileCheckpoint(ctx, queueID, stream)
		if err != nil {
			return ProducerReconcileCheckpoint{}, fmt.Errorf("bootstrap producer checkpoint: %w", err)
		}
		return checkpoint, nil
	}

	nextSeq := int64(1)
	if loader, ok := store.(QueueStateStore); ok {
		state, err := loader.LoadQueueState(ctx, queueID, stream)
		if err != nil {
			return ProducerReconcileCheckpoint{}, fmt.Errorf("bootstrap producer queue state: %w", err)
		}
		if state.NextOutboundSeq > 0 {
			nextSeq = state.NextOutboundSeq
		}
	}
	frames, err := store.ListOutboundReplay(ctx, queueID, stream, 1_000_000)
	if err != nil {
		return ProducerReconcileCheckpoint{}, fmt.Errorf("bootstrap producer replay: %w", err)
	}
	from, through := replayBounds(frames)
	return ProducerReconcileCheckpoint{
		QueueID:         queueID,
		Stream:          stream,
		ProducerNextSeq: nextSeq,
		ReplayFrom:      from,
		ReplayThrough:   through,
	}, nil
}

func (p *Producer) accept(ctx context.Context, entry acceptedOutbound) error {
	if p == nil {
		return ErrProducerNotReady
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.state.Load() != producerStateReady {
		return p.stateError()
	}

	p.accepting.Add(1)
	if p.state.Load() != producerStateReady {
		p.accepting.Add(-1)
		return p.stateError()
	}
	entry.payload = append([]byte(nil), entry.payload...)
	entry.metadata = entry.metadata.Clone()
	p.ingress.Push(entry)
	p.accepting.Add(-1)
	p.signalWake()
	return nil
}

func (p *Producer) Bind(
	ctx context.Context,
	sender Sender,
	peerAckedThrough int64,
) (*ProducerBinding, error) {
	if p == nil || p.state.Load() != producerStateReady {
		return nil, p.stateError()
	}
	if sender == nil {
		return nil, fmt.Errorf("reliablemq: sender is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	response := make(chan producerBindResult, 1)
	command := producerBindCommand{
		ctx:              ctx,
		sender:           sender,
		peerAckedThrough: peerAckedThrough,
		response:         response,
	}
	select {
	case p.commands <- command:
		p.signalWake()
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.done:
		return nil, p.stateError()
	}
	select {
	case result := <-response:
		return result.binding, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.done:
		return nil, p.stateError()
	}
}

func (p *Producer) Checkpoint(ctx context.Context) (ProducerReconcileCheckpoint, error) {
	if p == nil || p.state.Load() != producerStateReady {
		return ProducerReconcileCheckpoint{}, p.stateError()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	response := make(chan producerCheckpointResult, 1)
	select {
	case p.commands <- producerCheckpointCommand{response: response}:
		p.signalWake()
	case <-ctx.Done():
		return ProducerReconcileCheckpoint{}, ctx.Err()
	case <-p.done:
		return ProducerReconcileCheckpoint{}, p.stateError()
	}
	select {
	case result := <-response:
		return result.checkpoint, result.err
	case <-ctx.Done():
		return ProducerReconcileCheckpoint{}, ctx.Err()
	case <-p.done:
		return ProducerReconcileCheckpoint{}, p.stateError()
	}
}

func (p *Producer) AdvanceProducerNextSeq(ctx context.Context, nextSeq int64) error {
	if p == nil || p.state.Load() != producerStateReady {
		return p.stateError()
	}
	if nextSeq <= 0 {
		return fmt.Errorf("%w: next seq must be positive", ErrInvalidFrame)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	response := make(chan error, 1)
	select {
	case p.commands <- producerAdvanceCommand{ctx: ctx, nextSeq: nextSeq, response: response}:
		p.signalWake()
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return p.stateError()
	}
	select {
	case err := <-response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return p.stateError()
	}
}

func (p *Producer) acknowledge(through int64) error {
	if p == nil || p.state.Load() != producerStateReady {
		return p.stateError()
	}
	if through <= 0 {
		return fmt.Errorf("%w: ACK seq must be positive", ErrInvalidEnvelope)
	}
	atomicMaxInt64(&p.peerACK, through)
	p.signalWake()
	return nil
}

func (p *Producer) submitInboundACK(through int64) error {
	if through <= 0 {
		return nil
	}
	if p == nil || p.state.Load() != producerStateReady {
		return p.stateError()
	}
	atomicMaxInt64(&p.inboundACK, through)
	p.ackSignal.Add(1)
	p.signalWake()
	return nil
}

func (p *Producer) Stats() ProducerStats {
	if p == nil {
		return ProducerStats{}
	}
	stats := ProducerStats{
		Ready:                p.state.Load() == producerStateReady,
		Bound:                p.boundStat.Load(),
		BindingGeneration:    p.generationStat.Load(),
		Tail:                 p.tailStat.Load(),
		PersistedThrough:     p.persistedStat.Load(),
		NextToSend:           p.nextToSendStat.Load(),
		AckedThrough:         p.ackedStat.Load(),
		PendingACKThrough:    p.pendingACKStat.Load(),
		HotFrames:            p.hotStat.Load(),
		UnpersistedBytes:     p.unpersistedBytesStat.Load(),
		OldestUnpersistedAge: time.Duration(p.oldestUnpersistedAgeStat.Load()),
		AcceptedPending:      p.accepting.Load(),
	}
	if value := p.lastError.Load(); value != nil {
		stats.LastError = value.(producerErrorValue).err.Error()
	}
	return stats
}

func (p *Producer) Close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	for {
		state := p.state.Load()
		switch state {
		case producerStateClosed:
			return nil
		case producerStateClosing:
		case producerStateReady, producerStateFailed:
			if !p.state.CompareAndSwap(state, producerStateClosing) {
				continue
			}
			p.signalWake()
		default:
			return ErrProducerNotReady
		}
		break
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *ProducerBinding) Close() {
	if b == nil || b.producer == nil {
		return
	}
	b.once.Do(func() {
		select {
		case b.producer.commands <- producerUnbindCommand{generation: b.generation}:
			b.producer.signalWake()
		case <-b.producer.done:
		}
	})
}

// Done is closed when this generation is no longer bound to its sender.
func (b *ProducerBinding) Done() <-chan struct{} {
	if b == nil || b.lifecycle == nil {
		return nil
	}
	return b.lifecycle.done
}

// Err reports why this binding generation ended. It returns nil while the
// binding is active or when it was closed without a transport failure.
func (b *ProducerBinding) Err() error {
	if b == nil || b.lifecycle == nil {
		return ErrProducerDisconnected
	}
	return b.lifecycle.loadError()
}

func (b *ProducerBinding) WaitCaughtUp(ctx context.Context) error {
	if b == nil || b.producer == nil {
		return ErrProducerDisconnected
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		stats := b.producer.Stats()
		if stats.BindingGeneration != b.generation || !stats.Bound {
			return b.disconnectedError()
		}
		if stats.NextToSend > b.targetTail {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.producer.done:
			return ErrProducerClosed
		case <-b.Done():
			return b.disconnectedError()
		case <-b.producer.progress:
		case <-ticker.C:
		}
	}
}

func (b *ProducerBinding) disconnectedError() error {
	if err := b.Err(); err != nil {
		return err
	}
	return ErrProducerDisconnected
}

func newProducerBindingLifecycle() *producerBindingLifecycle {
	return &producerBindingLifecycle{done: make(chan struct{})}
}

func (l *producerBindingLifecycle) finish(err error) {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.mu.Lock()
		l.err = err
		l.mu.Unlock()
		close(l.done)
	})
}

func (l *producerBindingLifecycle) loadError() error {
	if l == nil {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.err
}

func (p *Producer) run(ctx context.Context, state producerOwnerState) {
	defer close(p.done)
	ticker := time.NewTicker(p.config.MaintenanceEvery)
	defer ticker.Stop()
	for {
		p.sequenceIngress(&state)
		p.applyACKSignals(&state)
		p.refreshPersistence(&state)
		p.enforceJournalLimits(&state)
		p.scheduleNetwork(ctx, &state)
		p.publishStats(&state)

		if p.state.Load() == producerStateClosing &&
			p.accepting.Load() == 0 && p.ingress.Empty() {
			p.disableBinding(&state, ErrProducerClosed)
			p.state.Store(producerStateClosed)
			p.cancel()
			p.signalProgress()
			return
		}

		select {
		case <-ctx.Done():
			p.disableBinding(&state, errors.Join(ErrProducerClosed, ctx.Err()))
			p.state.Store(producerStateClosed)
			return
		case <-p.wake:
		case command := <-p.commands:
			p.handleCommand(ctx, &state, command)
		case result := <-p.writeDone:
			p.handleWriteResult(&state, result)
		case result := <-p.loadDone:
			p.handleLoadResult(&state, result)
		case <-ticker.C:
		}
	}
}

func (p *Producer) sequenceIngress(state *producerOwnerState) {
	for {
		entry, ok := p.ingress.Pop()
		if !ok {
			return
		}
		var (
			frame Frame
			err   error
		)
		switch entry.kind {
		case FrameKindData:
			frame, err = p.store.AppendOutboundData(
				context.Background(),
				p.config.QueueID,
				p.config.Stream,
				entry.payload,
				entry.metadata,
			)
		case FrameKindTombstone:
			frame, err = p.store.AppendOutboundTombstone(
				context.Background(),
				p.config.QueueID,
				p.config.Stream,
				entry.errorMessage,
				entry.metadata,
			)
		default:
			err = fmt.Errorf("%w: unknown outbound frame kind %q", ErrInvalidFrame, entry.kind)
		}
		if err != nil {
			p.fail(state, fmt.Errorf("sequence accepted outbound message: %w", err))
			return
		}
		if frame.Key.Seq != state.nextSeq {
			p.fail(state, fmt.Errorf(
				"%w: sequencer allocated %d, want %d",
				ErrProducerJournalGap,
				frame.Key.Seq,
				state.nextSeq,
			))
			return
		}
		state.hot[frame.Key.Seq] = frame.Clone()
		if _, asynchronous := p.store.(ProducerPersistence); asynchronous {
			bytes := outboundFrameBytes(frame)
			state.unpersisted[frame.Key.Seq] = producerUnpersistedEntry{
				bytes:      bytes,
				acceptedAt: p.config.Now(),
			}
			state.unpersistedBytes += bytes
		} else {
			state.persistedThrough = frame.Key.Seq
		}
		state.tail = frame.Key.Seq
		state.nextSeq = frame.Key.Seq + 1
	}
}

func (p *Producer) applyACKSignals(state *producerOwnerState) {
	peerThrough := p.peerACK.Load()
	if peerThrough > state.ackedThrough {
		if err := p.store.AckOutboundThrough(
			context.Background(),
			p.config.QueueID,
			p.config.Stream,
			peerThrough,
		); err != nil {
			p.fail(state, fmt.Errorf("persist cumulative ACK: %w", err))
			return
		}
		state.ackedThrough = peerThrough
		if state.nextToSend <= peerThrough {
			state.nextToSend = peerThrough + 1
		}
		for seq := range state.hot {
			if seq <= peerThrough {
				delete(state.hot, seq)
				delete(state.cursorLoaded, seq)
			}
		}
	}
	ackSignal := p.ackSignal.Load()
	inboundThrough := p.inboundACK.Load()
	if inboundThrough > state.pendingACK {
		state.pendingACK = inboundThrough
	}
	if ackSignal > state.pendingACKSignal {
		state.pendingACKSignal = ackSignal
	}
}

func (p *Producer) refreshPersistence(state *producerOwnerState) {
	if persistence, ok := p.store.(ProducerPersistence); ok {
		through := persistence.PersistedThrough(p.config.QueueID, p.config.Stream)
		if through > state.persistedThrough {
			state.persistedThrough = through
		}
	}
	for seq, entry := range state.unpersisted {
		if seq <= state.persistedThrough {
			state.unpersistedBytes -= entry.bytes
			delete(state.unpersisted, seq)
		}
	}
	for seq := range state.hot {
		_, loadedFromCursor := state.cursorLoaded[seq]
		if seq <= state.persistedThrough && !loadedFromCursor {
			delete(state.hot, seq)
		}
	}
}

func (p *Producer) enforceJournalLimits(state *producerOwnerState) {
	if p.state.Load() != producerStateReady || len(state.unpersisted) == 0 {
		return
	}
	if state.unpersistedBytes > p.config.MaxUnpersistedBytes {
		p.fail(state, fmt.Errorf(
			"%w: %d unpersisted bytes exceeds %d",
			ErrProducerJournalLimit,
			state.unpersistedBytes,
			p.config.MaxUnpersistedBytes,
		))
		return
	}
	oldest := p.config.Now()
	for _, entry := range state.unpersisted {
		if entry.acceptedAt.Before(oldest) {
			oldest = entry.acceptedAt
		}
	}
	age := p.config.Now().Sub(oldest)
	if age > p.config.MaxUnpersistedAge {
		p.fail(state, fmt.Errorf(
			"%w: oldest unpersisted frame age %s exceeds %s",
			ErrProducerJournalLimit,
			age,
			p.config.MaxUnpersistedAge,
		))
	}
}

func (p *Producer) scheduleNetwork(ctx context.Context, state *producerOwnerState) {
	if p.state.Load() != producerStateReady || state.binding == nil || state.networkInFlight {
		return
	}
	if state.pendingACK > 0 && state.pendingACKSignal > state.lastACKSignal {
		request := producerWriteRequest{
			generation: state.binding.generation,
			kind:       producerWriteACK,
			seq:        state.pendingACK,
			ackSignal:  state.pendingACKSignal,
			envelope:   AckEnvelope(p.config.QueueID, p.config.Stream, state.pendingACK),
		}
		if p.submitWrite(state, request) {
			return
		}
	}
	if state.nextToSend > state.tail {
		return
	}
	frame, ok := state.hot[state.nextToSend]
	if !ok {
		if !state.loadInFlight {
			state.loadInFlight = true
			state.loadFrom = state.nextToSend
			go p.loadCursor(ctx, state.nextToSend)
		}
		return
	}
	request := producerWriteRequest{
		generation: state.binding.generation,
		kind:       producerWriteData,
		seq:        frame.Key.Seq,
		envelope:   EnvelopeFromFrame(frame),
	}
	p.submitWrite(state, request)
}

func (p *Producer) submitWrite(state *producerOwnerState, request producerWriteRequest) bool {
	select {
	case state.binding.requests <- request:
		state.networkInFlight = true
		return true
	default:
		return false
	}
}

func (p *Producer) loadCursor(ctx context.Context, from int64) {
	var (
		frames []Frame
		err    error
	)
	if cursor, ok := p.store.(OutboundCursorStore); ok {
		frames, err = cursor.ListOutboundReplayFrom(
			ctx,
			p.config.QueueID,
			p.config.Stream,
			from,
			p.config.CursorBatchSize,
		)
	} else {
		var all []Frame
		all, err = p.store.ListOutboundReplay(ctx, p.config.QueueID, p.config.Stream, 1_000_000)
		if err == nil {
			for _, frame := range all {
				if frame.Key.Seq < from {
					continue
				}
				frames = append(frames, frame)
				if len(frames) >= p.config.CursorBatchSize {
					break
				}
			}
		}
	}
	result := producerLoadResult{from: from, frames: frames, err: err}
	select {
	case p.loadDone <- result:
	case <-ctx.Done():
	case <-p.done:
	}
}

func (p *Producer) handleLoadResult(state *producerOwnerState, result producerLoadResult) {
	if !state.loadInFlight || result.from != state.loadFrom {
		return
	}
	state.loadInFlight = false
	if result.err != nil {
		p.disconnect(state, fmt.Errorf("load outbound cursor: %w", result.err))
		return
	}
	expectedSeq := result.from
	for _, frame := range result.frames {
		if err := ValidateFrame(frame); err != nil {
			p.disconnect(state, err)
			return
		}
		if frame.Key.Direction != DirectionOutbound || frame.Key.Seq != expectedSeq {
			p.disconnect(state, fmt.Errorf(
				"%w: cursor returned frame %+v, want outbound seq %d",
				ErrProducerJournalGap,
				frame.Key,
				expectedSeq,
			))
			return
		}
		state.hot[frame.Key.Seq] = frame.Clone()
		state.cursorLoaded[frame.Key.Seq] = struct{}{}
		expectedSeq++
	}
	if len(result.frames) == 0 && state.nextToSend <= state.tail {
		p.disconnect(state, fmt.Errorf("%w at seq %d", ErrProducerJournalGap, state.nextToSend))
	}
}

func (p *Producer) handleWriteResult(state *producerOwnerState, result producerWriteResult) {
	if state.binding == nil || result.request.generation != state.binding.generation {
		return
	}
	state.networkInFlight = false
	if result.err != nil {
		p.disconnect(state, result.err)
		return
	}
	switch result.request.kind {
	case producerWriteACK:
		if result.request.seq > state.lastACKSent {
			state.lastACKSent = result.request.seq
		}
		if result.request.ackSignal > state.lastACKSignal {
			state.lastACKSignal = result.request.ackSignal
		}
	case producerWriteData:
		if result.request.seq != state.nextToSend {
			p.disconnect(state, fmt.Errorf(
				"%w: wrote seq %d while cursor was %d",
				ErrProducerJournalGap,
				result.request.seq,
				state.nextToSend,
			))
			return
		}
		state.nextToSend++
		if result.request.seq <= state.persistedThrough {
			delete(state.hot, result.request.seq)
			delete(state.cursorLoaded, result.request.seq)
		}
	}
	p.signalProgress()
}

func (p *Producer) handleCommand(ctx context.Context, state *producerOwnerState, command any) {
	p.sequenceIngress(state)
	switch command := command.(type) {
	case producerBindCommand:
		if p.state.Load() != producerStateReady {
			command.response <- producerBindResult{err: p.stateError()}
			return
		}
		if err := command.ctx.Err(); err != nil {
			command.response <- producerBindResult{err: err}
			return
		}
		if command.peerAckedThrough > state.ackedThrough {
			atomicMaxInt64(&p.peerACK, command.peerAckedThrough)
			p.applyACKSignals(state)
			if p.state.Load() == producerStateFailed {
				command.response <- producerBindResult{err: p.stateError()}
				return
			}
		}
		p.disableBinding(state, nil)
		state.nextGeneration++
		writerCtx, cancel := context.WithCancel(ctx)
		lifecycle := newProducerBindingLifecycle()
		bindingState := &producerWriterBinding{
			generation: state.nextGeneration,
			requests:   make(chan producerWriteRequest, 1),
			cancel:     cancel,
			lifecycle:  lifecycle,
		}
		state.binding = bindingState
		state.networkInFlight = false
		state.nextToSend = state.ackedThrough + 1
		p.boundStat.Store(true)
		p.generationStat.Store(bindingState.generation)
		go p.runWriter(writerCtx, command.sender, bindingState.requests)
		binding := &ProducerBinding{
			producer:   p,
			generation: bindingState.generation,
			targetTail: state.tail,
			lifecycle:  lifecycle,
		}
		command.response <- producerBindResult{binding: binding}
		p.signalProgress()
	case producerCheckpointCommand:
		from := int64(0)
		through := int64(0)
		if state.ackedThrough < state.tail {
			from = state.ackedThrough + 1
			through = state.tail
		}
		command.response <- producerCheckpointResult{checkpoint: ProducerReconcileCheckpoint{
			QueueID:         p.config.QueueID,
			Stream:          p.config.Stream,
			ProducerNextSeq: state.nextSeq,
			ReplayFrom:      from,
			ReplayThrough:   through,
		}}
	case producerAdvanceCommand:
		if err := command.ctx.Err(); err != nil {
			command.response <- err
			return
		}
		reconciler, ok := p.store.(ReconcileProducerStore)
		if !ok {
			command.response <- fmt.Errorf("reliablemq: producer store cannot advance sequence")
			return
		}
		if err := reconciler.AdvanceProducerNextSeq(
			command.ctx,
			p.config.QueueID,
			p.config.Stream,
			command.nextSeq,
		); err != nil {
			command.response <- err
			return
		}
		through := command.nextSeq - 1
		if command.nextSeq > state.nextSeq {
			state.nextSeq = command.nextSeq
		}
		if through > state.tail {
			state.tail = through
		}
		if through > state.ackedThrough {
			state.ackedThrough = through
		}
		if state.nextToSend <= through {
			state.nextToSend = through + 1
		}
		for seq := range state.hot {
			if seq <= through {
				delete(state.hot, seq)
				delete(state.cursorLoaded, seq)
			}
		}
		command.response <- nil
	case producerUnbindCommand:
		if state.binding != nil && state.binding.generation == command.generation {
			p.disableBinding(state, nil)
			p.signalProgress()
		}
	}
}

func (p *Producer) runWriter(
	ctx context.Context,
	sender Sender,
	requests <-chan producerWriteRequest,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case request := <-requests:
			err := sender.Send(ctx, request.envelope)
			result := producerWriteResult{request: request, err: err}
			select {
			case p.writeDone <- result:
			case <-ctx.Done():
			case <-p.done:
			}
			if err != nil {
				return
			}
		}
	}
}

func (p *Producer) disconnect(state *producerOwnerState, err error) {
	p.lastError.Store(producerErrorValue{err: err})
	p.disableBinding(state, errors.Join(ErrProducerDisconnected, err))
	p.signalProgress()
}

func (p *Producer) disableBinding(state *producerOwnerState, err error) {
	if state.binding != nil {
		state.binding.cancel()
		state.binding.lifecycle.finish(err)
		state.binding = nil
	}
	state.networkInFlight = false
	for seq := range state.cursorLoaded {
		delete(state.hot, seq)
		delete(state.cursorLoaded, seq)
	}
	p.boundStat.Store(false)
}

func (p *Producer) fail(state *producerOwnerState, err error) {
	if err == nil {
		return
	}
	if !p.state.CompareAndSwap(producerStateReady, producerStateFailed) {
		return
	}
	p.lastError.Store(producerErrorValue{err: err})
	p.disableBinding(state, errors.Join(ErrProducerDisconnected, err))
	p.signalProgress()
	if p.config.OnError != nil {
		p.config.OnError(err)
	}
}

func (p *Producer) publishStats(state *producerOwnerState) {
	p.tailStat.Store(state.tail)
	p.persistedStat.Store(state.persistedThrough)
	p.nextToSendStat.Store(state.nextToSend)
	p.ackedStat.Store(state.ackedThrough)
	p.pendingACKStat.Store(state.pendingACK)
	p.hotStat.Store(int64(len(state.hot)))
	p.unpersistedBytesStat.Store(state.unpersistedBytes)
	var oldest time.Time
	for _, entry := range state.unpersisted {
		if oldest.IsZero() || entry.acceptedAt.Before(oldest) {
			oldest = entry.acceptedAt
		}
	}
	var oldestAge time.Duration
	if !oldest.IsZero() {
		oldestAge = p.config.Now().Sub(oldest)
	}
	p.oldestUnpersistedAgeStat.Store(int64(oldestAge))
}

func outboundFrameBytes(frame Frame) int64 {
	bytes := int64(len(frame.Payload) + len(frame.ErrorMessage))
	for key, value := range frame.Metadata {
		bytes += int64(len(key) + len(value))
	}
	return bytes
}

func (p *Producer) stateError() error {
	if p == nil {
		return ErrProducerNotReady
	}
	if value := p.lastError.Load(); value != nil && p.state.Load() == producerStateFailed {
		return errors.Join(ErrProducerNotReady, value.(producerErrorValue).err)
	}
	switch p.state.Load() {
	case producerStateReady:
		return nil
	case producerStateClosing, producerStateClosed:
		return ErrProducerClosed
	default:
		return ErrProducerNotReady
	}
}

func (p *Producer) signalWake() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *Producer) signalProgress() {
	select {
	case p.progress <- struct{}{}:
	default:
	}
}

func atomicMaxInt64(value *atomic.Int64, candidate int64) {
	for {
		current := value.Load()
		if candidate <= current || value.CompareAndSwap(current, candidate) {
			return
		}
	}
}

type producerIngressNode struct {
	next  atomic.Pointer[producerIngressNode]
	entry acceptedOutbound
}

type producerIngressQueue struct {
	head *producerIngressNode
	tail atomic.Pointer[producerIngressNode]
}

func newProducerIngressQueue() *producerIngressQueue {
	stub := &producerIngressNode{}
	queue := &producerIngressQueue{head: stub}
	queue.tail.Store(stub)
	return queue
}

func (q *producerIngressQueue) Push(entry acceptedOutbound) {
	node := &producerIngressNode{entry: entry}
	previous := q.tail.Swap(node)
	previous.next.Store(node)
}

func (q *producerIngressQueue) Pop() (acceptedOutbound, bool) {
	next := q.head.next.Load()
	if next == nil {
		return acceptedOutbound{}, false
	}
	q.head = next
	entry := next.entry
	next.entry = acceptedOutbound{}
	return entry, true
}

func (q *producerIngressQueue) Empty() bool {
	return q.tail.Load() == q.head
}
