package reliablemq

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnvelopeDataRoundTrip(t *testing.T) {
	// Given
	env := Envelope{
		Type:     EnvelopeTypeData,
		QueueID:  "conn_1",
		Stream:   StreamACP,
		Seq:      7,
		Metadata: Metadata{"agent_id": "agent_1"},
		Payload:  json.RawMessage(`{"jsonrpc":"2.0","method":"ping"}`),
	}

	// When
	data, err := MarshalEnvelope(env)
	require.NoError(t, err)
	got, err := UnmarshalEnvelope(data)
	require.NoError(t, err)

	// Then
	require.Equal(t, env.Type, got.Type)
	require.Equal(t, env.QueueID, got.QueueID)
	require.Equal(t, env.Stream, got.Stream)
	require.Equal(t, env.Seq, got.Seq)
	require.Equal(t, "agent_1", got.Metadata["agent_id"])
	require.JSONEq(t, string(env.Payload), string(got.Payload))
}

func TestEnvelopeACKValidation(t *testing.T) {
	// Given
	env := AckEnvelope("conn_1", StreamACP, 12)

	// When
	err := ValidateEnvelope(env)

	// Then
	require.NoError(t, err)
}

func TestEnvelopeTombstoneRoundTrip(t *testing.T) {
	// Given
	env := Envelope{
		Type:         EnvelopeTypeTombstone,
		QueueID:      "conn_1",
		Stream:       StreamACP,
		Seq:          13,
		Metadata:     Metadata{"agent_id": "agent_1"},
		ErrorMessage: "outbound payload rejected by policy",
	}

	// When
	data, err := MarshalEnvelope(env)
	require.NoError(t, err)
	got, err := UnmarshalEnvelope(data)
	require.NoError(t, err)

	// Then
	require.Equal(t, EnvelopeTypeTombstone, got.Type)
	require.Nil(t, got.Payload)
	require.Equal(t, env.ErrorMessage, got.ErrorMessage)
}

func TestEnvelopeReconcileRequestRoundTrip(t *testing.T) {
	// Given
	env := ReconcileRequestEnvelope(ProducerReconcileCheckpoint{
		QueueID:         "conn_1",
		Stream:          StreamACP,
		ProducerNextSeq: 4,
		ReplayFrom:      2,
		ReplayThrough:   3,
	})

	// When
	data, err := MarshalEnvelope(env)
	require.NoError(t, err)
	got, err := UnmarshalEnvelope(data)
	require.NoError(t, err)

	// Then
	require.Equal(t, EnvelopeTypeReconcileRequest, got.Type)
	require.Equal(t, int64(4), got.ProducerNextSeq)
	require.Equal(t, int64(2), got.ReplayFrom)
	require.Equal(t, int64(3), got.ReplayThrough)
}

func TestEnvelopeReconcileResponseValidation(t *testing.T) {
	tests := []struct {
		name string
		env  Envelope
	}{
		{
			name: "aligned",
			env: Envelope{
				Type:                 EnvelopeTypeReconcileResponse,
				QueueID:              "conn_1",
				Stream:               StreamACP,
				Action:               ReconcileActionAligned,
				ConsumerAckedThrough: 3,
			},
		},
		{
			name: "replay",
			env: Envelope{
				Type:                 EnvelopeTypeReconcileResponse,
				QueueID:              "conn_1",
				Stream:               StreamACP,
				Action:               ReconcileActionReplay,
				ConsumerAckedThrough: 1,
				From:                 2,
				Through:              3,
			},
		},
		{
			name: "advance producer",
			env: Envelope{
				Type:                   EnvelopeTypeReconcileResponse,
				QueueID:                "conn_1",
				Stream:                 StreamACP,
				Action:                 ReconcileActionAdvanceProducer,
				ConsumerAckedThrough:   8,
				AdvanceProducerNextSeq: 9,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When
			err := ValidateEnvelope(tt.env)

			// Then
			require.NoError(t, err)
		})
	}
}

func TestEnvelopeInvalidFields(t *testing.T) {
	tests := []struct {
		name string
		env  Envelope
	}{
		{
			name: "unknown type",
			env:  Envelope{Type: "wat", QueueID: "conn_1", Stream: StreamACP, Seq: 1},
		},
		{
			name: "empty queue_id",
			env:  Envelope{Type: EnvelopeTypeAck, Stream: StreamACP, Seq: 1},
		},
		{
			name: "empty stream",
			env:  Envelope{Type: EnvelopeTypeAck, QueueID: "conn_1", Seq: 1},
		},
		{
			name: "invalid seq",
			env:  Envelope{Type: EnvelopeTypeAck, QueueID: "conn_1", Stream: StreamACP},
		},
		{
			name: "data missing payload",
			env:  Envelope{Type: EnvelopeTypeData, QueueID: "conn_1", Stream: StreamACP, Seq: 1},
		},
		{
			name: "data invalid payload",
			env:  Envelope{Type: EnvelopeTypeData, QueueID: "conn_1", Stream: StreamACP, Seq: 1, Payload: json.RawMessage(`{`)},
		},
		{
			name: "tombstone with payload",
			env:  Envelope{Type: EnvelopeTypeTombstone, QueueID: "conn_1", Stream: StreamACP, Seq: 1, Payload: json.RawMessage(`{}`)},
		},
		{
			name: "reconcile request missing producer next seq",
			env:  Envelope{Type: EnvelopeTypeReconcileRequest, QueueID: "conn_1", Stream: StreamACP},
		},
		{
			name: "reconcile response invalid action",
			env:  Envelope{Type: EnvelopeTypeReconcileResponse, QueueID: "conn_1", Stream: StreamACP, Action: "wat"},
		},
		{
			name: "reconcile replay invalid range",
			env: Envelope{
				Type:    EnvelopeTypeReconcileResponse,
				QueueID: "conn_1",
				Stream:  StreamACP,
				Action:  ReconcileActionReplay,
				From:    4,
				Through: 3,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When
			err := ValidateEnvelope(tt.env)

			// Then
			require.ErrorIs(t, err, ErrInvalidEnvelope)
		})
	}
}
