package reliablemq

import (
	"encoding/json"
	"time"
)

type Stream string

const (
	StreamACP     Stream = "acp"
	StreamControl Stream = "control"
)

type Direction string

const (
	DirectionOutbound Direction = "outbound"
	DirectionInbound  Direction = "inbound"
)

type Status string

const (
	StatusPending  Status = "pending"
	StatusSent     Status = "sent"
	StatusAcked    Status = "acked"
	StatusReceived Status = "received"
	StatusApplied  Status = "applied"
	StatusRejected Status = "rejected"
)

type FrameKind string

const (
	FrameKindData      FrameKind = "data"
	FrameKindTombstone FrameKind = "tombstone"
)

type Metadata map[string]string

func (m Metadata) Clone() Metadata {
	if len(m) == 0 {
		return nil
	}
	cloned := make(Metadata, len(m))
	for key, value := range m {
		cloned[key] = value
	}
	return cloned
}

type FrameKey struct {
	QueueID   string    `json:"queue_id"`
	Stream    Stream    `json:"stream"`
	Seq       int64     `json:"seq"`
	Direction Direction `json:"direction"`
}

type Frame struct {
	Key          FrameKey        `json:"key"`
	Kind         FrameKind       `json:"kind"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	Metadata     Metadata        `json:"metadata,omitempty"`
	Status       Status          `json:"status"`
	ErrorMessage string          `json:"error_message,omitempty"`
	CreatedAt    time.Time       `json:"created_at,omitempty"`
	UpdatedAt    time.Time       `json:"updated_at,omitempty"`
}

func (f Frame) Clone() Frame {
	f.Payload = append(json.RawMessage(nil), f.Payload...)
	f.Metadata = f.Metadata.Clone()
	return f
}

func DefaultStatus(direction Direction) Status {
	if direction == DirectionInbound {
		return StatusReceived
	}
	return StatusPending
}
