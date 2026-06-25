package reliablemq

import (
	"encoding/json"
	"fmt"
)

type EnvelopeType string

const (
	EnvelopeTypeData      EnvelopeType = "data"
	EnvelopeTypeAck       EnvelopeType = "ack"
	EnvelopeTypeTombstone EnvelopeType = "tombstone"
)

type Envelope struct {
	Type         EnvelopeType    `json:"type"`
	QueueID      string          `json:"queue_id"`
	Stream       Stream          `json:"stream"`
	Seq          int64           `json:"seq"`
	Metadata     Metadata        `json:"metadata,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	ErrorMessage string          `json:"error_message,omitempty"`
}

func DataEnvelope(frame Frame) Envelope {
	return Envelope{
		Type:     EnvelopeTypeData,
		QueueID:  frame.Key.QueueID,
		Stream:   frame.Key.Stream,
		Seq:      frame.Key.Seq,
		Metadata: frame.Metadata.Clone(),
		Payload:  append(json.RawMessage(nil), frame.Payload...),
	}
}

func TombstoneEnvelope(frame Frame) Envelope {
	return Envelope{
		Type:         EnvelopeTypeTombstone,
		QueueID:      frame.Key.QueueID,
		Stream:       frame.Key.Stream,
		Seq:          frame.Key.Seq,
		Metadata:     frame.Metadata.Clone(),
		ErrorMessage: frame.ErrorMessage,
	}
}

func EnvelopeFromFrame(frame Frame) Envelope {
	if frame.Kind == FrameKindTombstone {
		return TombstoneEnvelope(frame)
	}
	return DataEnvelope(frame)
}

func AckEnvelope(queueID string, stream Stream, throughSeq int64) Envelope {
	return Envelope{
		Type:    EnvelopeTypeAck,
		QueueID: queueID,
		Stream:  stream,
		Seq:     throughSeq,
	}
}

func MarshalEnvelope(env Envelope) ([]byte, error) {
	if err := ValidateEnvelope(env); err != nil {
		return nil, err
	}
	return json.Marshal(env)
}

func UnmarshalEnvelope(data []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, fmt.Errorf("%w: %v", ErrInvalidEnvelope, err)
	}
	if err := ValidateEnvelope(env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

func ValidateEnvelope(env Envelope) error {
	if env.Type != EnvelopeTypeData && env.Type != EnvelopeTypeAck && env.Type != EnvelopeTypeTombstone {
		return fmt.Errorf("%w: unknown type %q", ErrInvalidEnvelope, env.Type)
	}
	if env.QueueID == "" {
		return fmt.Errorf("%w: queue_id is required", ErrInvalidEnvelope)
	}
	if env.Stream == "" {
		return fmt.Errorf("%w: stream is required", ErrInvalidEnvelope)
	}
	if env.Seq <= 0 {
		return fmt.Errorf("%w: seq must be positive", ErrInvalidEnvelope)
	}
	if env.Type == EnvelopeTypeData {
		if len(env.Payload) == 0 {
			return fmt.Errorf("%w: payload is required", ErrInvalidEnvelope)
		}
		if !json.Valid(env.Payload) {
			return fmt.Errorf("%w: payload must be valid JSON", ErrInvalidEnvelope)
		}
	}
	if env.Type == EnvelopeTypeTombstone && len(env.Payload) > 0 {
		return fmt.Errorf("%w: tombstone must not carry payload", ErrInvalidEnvelope)
	}
	return nil
}

func InboundFrameFromEnvelope(env Envelope) Frame {
	kind := FrameKindData
	if env.Type == EnvelopeTypeTombstone {
		kind = FrameKindTombstone
	}
	return Frame{
		Key: FrameKey{
			QueueID:   env.QueueID,
			Stream:    env.Stream,
			Seq:       env.Seq,
			Direction: DirectionInbound,
		},
		Kind:         kind,
		Payload:      append(json.RawMessage(nil), env.Payload...),
		Metadata:     env.Metadata.Clone(),
		Status:       StatusReceived,
		ErrorMessage: env.ErrorMessage,
	}
}
