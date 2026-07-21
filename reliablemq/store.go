package reliablemq

import (
	"context"
	"encoding/json"
)

// DurableStore is implemented by paxd, pax-manager, or another host process
// using its own database and transaction machinery.
type DurableStore interface {
	// AppendOutboundData atomically allocates the next outbound seq and records a
	// data frame in this store layer. ProducerWriteBehindStore may stage the frame
	// in memory before asynchronously flushing it to its durable sink.
	AppendOutboundData(
		ctx context.Context,
		queueID string,
		stream Stream,
		payload json.RawMessage,
		metadata Metadata,
	) (Frame, error)

	// AppendOutboundTombstone atomically allocates the next outbound seq and
	// records a tombstone in this store layer. ProducerWriteBehindStore may stage
	// the frame in memory before asynchronously flushing it to its durable sink.
	AppendOutboundTombstone(
		ctx context.Context,
		queueID string,
		stream Stream,
		errorMessage string,
		metadata Metadata,
	) (Frame, error)

	// SaveInboundIfAbsent durably records an inbound frame unless the same
	// queueID+stream+seq+inbound direction already exists.
	SaveInboundIfAbsent(ctx context.Context, frame Frame) (inserted bool, stored Frame, err error)

	ListOutboundReplay(ctx context.Context, queueID string, stream Stream, limit int) ([]Frame, error)
	ListInboundReplay(ctx context.Context, queueID string, stream Stream, limit int) ([]Frame, error)

	MarkSent(ctx context.Context, key FrameKey) error
	AckOutboundThrough(ctx context.Context, queueID string, stream Stream, throughSeq int64) error
	MarkApplied(ctx context.Context, key FrameKey) error
	MarkRejected(ctx context.Context, key FrameKey, errorMessage string) error
	RecordSendFailure(ctx context.Context, key FrameKey, errorMessage string) error
	RecordDispatchFailure(ctx context.Context, key FrameKey, errorMessage string) error
	UpdateMetadata(ctx context.Context, key FrameKey, metadata Metadata) error
}

type QueueState struct {
	NextOutboundSeq       int64
	OutboundAckedThrough  int64
	InboundAppliedThrough int64
}

// QueueStateStore is an optional companion interface for stores that expose
// transport queue cursors without mutating journal rows.
type QueueStateStore interface {
	LoadQueueState(ctx context.Context, queueID string, stream Stream) (QueueState, error)
}

type StoreBatch struct {
	Frames  []Frame
	Patches []StorePatch
}

type StorePatch struct {
	Key          FrameKey
	Status       Status
	ErrorMessage string
	Metadata     Metadata
	HasMetadata  bool
}

// BatchStore is an optional fast path for DurableStore implementations that can
// apply one write-behind flush in a single transaction.
type BatchStore interface {
	ApplyBatch(ctx context.Context, batch StoreBatch) error
}

type ProducerReconcileCheckpoint struct {
	QueueID         string
	Stream          Stream
	ProducerNextSeq int64
	ReplayFrom      int64
	ReplayThrough   int64
}

type ReconcileProducerStore interface {
	LoadProducerReconcileCheckpoint(ctx context.Context, queueID string, stream Stream) (ProducerReconcileCheckpoint, error)
	AdvanceProducerNextSeq(ctx context.Context, queueID string, stream Stream, nextSeq int64) error
}

type ReconcileConsumerStore interface {
	ConsumerAckedThrough(ctx context.Context, queueID string, stream Stream) (int64, error)
}

// OutboundCursorStore reads replayable outbound frames starting at an exact
// sequence. The producer uses it to page through a journal without marking each
// successful socket write as sent.
type OutboundCursorStore interface {
	ListOutboundReplayFrom(
		ctx context.Context,
		queueID string,
		stream Stream,
		fromSeq int64,
		limit int,
	) ([]Frame, error)
}

// ProducerPersistence reports the highest outbound sequence known to have
// reached the journal. It lets the producer evict persisted hot entries while a
// network connection is unavailable.
type ProducerPersistence interface {
	PersistedThrough(queueID string, stream Stream) int64
}
