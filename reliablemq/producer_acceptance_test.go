package reliablemq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducerCloseWaitsForReservedACKPublication(t *testing.T) {
	for _, inbound := range []bool{false, true} {
		t.Run(map[bool]string{false: "peer_ack", true: "inbound_ack"}[inbound], func(t *testing.T) {
			// Given acceptance has reserved its ready-state slot but has not published its signal.
			store := newSpyStore()
			producer := newEngineTestProducer(t, store)
			require.NoError(t, producer.beginAcceptance())
			reserved := true
			t.Cleanup(func() {
				if reserved {
					producer.finishAcceptance()
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			closed := make(chan error, 1)
			go func() { closed <- producer.Close(ctx) }()
			require.Eventually(t, func() bool { return !producer.Stats().Ready }, time.Second, time.Millisecond)

			// When Close races with publication, it must wait for that reservation.
			select {
			case err := <-closed:
				t.Fatalf("Close returned before reserved ACK publication: %v", err)
			default:
			}
			if inbound {
				atomicMaxInt64(&producer.inboundACK, 1)
				producer.ackSignal.Add(1)
			} else {
				atomicMaxInt64(&producer.peerACK, 1)
			}
			producer.finishAcceptance()
			reserved = false

			// Then the final owner drain includes the signal and all reservations are released.
			require.NoError(t, <-closed)
			assert.Zero(t, producer.Stats().AcceptedPending)
			if inbound {
				assert.Equal(t, int64(1), producer.Stats().PendingACKThrough)
			} else {
				assert.Equal(t, int64(1), producer.Stats().AckedThrough)
				assert.Contains(t, store.callSnapshot(), "AckOutboundThrough:1")
			}
		})
	}
}

func TestProducerACKAcceptanceRejectsInvalidAndClosedSignals(t *testing.T) {
	// Given an active producer accepts only positive peer ACKs.
	producer := newEngineTestProducer(t, newSpyStore())
	assert.ErrorIs(t, producer.acknowledge(0), ErrInvalidEnvelope)
	assert.ErrorIs(t, producer.acknowledge(-1), ErrInvalidEnvelope)
	assert.NoError(t, producer.submitInboundACK(0))
	assert.Zero(t, producer.Stats().AcceptedPending)

	// When it closes, no new peer or inbound ACK signal is accepted.
	require.NoError(t, producer.Close(context.Background()))
	assert.ErrorIs(t, producer.acknowledge(1), ErrProducerClosed)
	assert.ErrorIs(t, producer.submitInboundACK(1), ErrProducerClosed)
	assert.NoError(t, producer.submitInboundACK(0))
	assert.Zero(t, producer.Stats().AcceptedPending)
	var absent *Producer
	assert.ErrorIs(t, absent.acknowledge(1), ErrProducerNotReady)
	assert.ErrorIs(t, absent.submitInboundACK(1), ErrProducerNotReady)
}
