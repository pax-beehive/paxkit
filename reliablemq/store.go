package reliablemq

import (
	"context"
	"encoding/json"
)

// DurableStore is implemented by paxd, pax-manager, or another host process
// using its own database and transaction machinery.
type DurableStore interface {
	// AppendOutboundData atomically allocates the next outbound seq and durably
	// records a data frame before any network send.
	AppendOutboundData(
		ctx context.Context,
		queueID string,
		stream Stream,
		payload json.RawMessage,
		metadata Metadata,
	) (Frame, error)

	// AppendOutboundTombstone atomically allocates the next outbound seq and
	// durably records a tombstone frame before any network send.
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
