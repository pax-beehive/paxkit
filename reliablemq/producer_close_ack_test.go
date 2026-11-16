package reliablemq_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pax-beehive/paxkit/reliablemq"
	"github.com/pax-beehive/paxkit/reliablemq/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducerCloseDrainsACKAcceptedAfterOwnerSignalRead(t *testing.T) {
	for _, backend := range []string{"memory", "write_behind"} {
		for _, signal := range []string{"peer_ack", "inbound_ack", "peer_ack_store_error"} {
			t.Run(backend+"/"+signal, func(t *testing.T) {
				// Given the owner is paused after reading ACK signals but before checking Close.
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				sink := memory.New()
				frame, err := sink.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
				require.NoError(t, err)
				require.NoError(t, sink.MarkSent(ctx, frame.Key))
				var durable reliablemq.DurableStore = sink
				var closeStore func(context.Context) error
				if backend == "write_behind" {
					buffer := reliablemq.NewProducerWriteBehindStore(sink, reliablemq.WithProducerWriteBehindManualFlush())
					durable, closeStore = buffer, buffer.Close
				}
				store := &closeACKGateStore{DurableStore: durable, entered: make(chan struct{}), release: make(chan struct{})}
				var wantErr error
				if signal == "peer_ack_store_error" {
					wantErr = errors.New("journal ACK write failed")
					store.ackErr = wantErr
				}
				producer, err := reliablemq.NewProducer(ctx, reliablemq.ProducerConfig{QueueID: "conn_1", Stream: reliablemq.StreamACP}, store)
				require.NoError(t, err)
				var release sync.Once
				t.Cleanup(func() {
					release.Do(func() { close(store.release) })
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
					defer cleanupCancel()
					_ = producer.Close(cleanupCtx)
					if closeStore != nil {
						_ = closeStore(context.Background())
					}
				})
				select {
				case <-store.entered:
				case <-ctx.Done():
					t.Fatal("owner did not reach the post-ACK-read gate")
				}
				engine := reliablemq.NewEngine(reliablemq.Config{}, store, producer,
					reliablemq.DispatcherFunc(func(context.Context, reliablemq.Frame) error { return nil }))

				// When a public Receive succeeds, then Close begins before the owner resumes.
				if signal == "inbound_ack" {
					require.NoError(t, engine.Receive(ctx, reliablemq.DataEnvelope(frame)))
				} else {
					require.NoError(t, engine.Receive(ctx, reliablemq.AckEnvelope("conn_1", reliablemq.StreamACP, 1)))
				}
				closed := make(chan error, 1)
				go func() { closed <- producer.Close(ctx) }()
				require.Eventually(t, func() bool { return !producer.Stats().Ready }, time.Second, time.Millisecond)
				release.Do(func() { close(store.release) })
				closeErr := <-closed

				// Then accepted signals are drained, and failed persistence is returned without retrying forever.
				if wantErr != nil {
					assert.ErrorIs(t, closeErr, wantErr)
					assert.Contains(t, producer.Stats().LastError, wantErr.Error())
				} else {
					require.NoError(t, closeErr)
				}
				if closeStore != nil {
					require.NoError(t, closeStore(ctx))
				}
				stored, found := sink.Get(frame.Key)
				require.True(t, found)
				if signal == "peer_ack" {
					assert.Equal(t, reliablemq.StatusAcked, stored.Status)
					assert.Equal(t, int64(1), producer.Stats().AckedThrough)
				} else {
					assert.Equal(t, reliablemq.StatusSent, stored.Status)
				}
				if signal == "inbound_ack" {
					assert.Equal(t, int64(1), producer.Stats().PendingACKThrough)
				} else {
					assert.Equal(t, int64(1), store.ackCalls.Load())
				}
				if wantErr != nil {
					assert.ErrorIs(t, producer.Close(ctx), wantErr, "repeated Close retains its persistence error")
				}
			})
		}
	}
}

type closeACKGateStore struct {
	reliablemq.DurableStore
	calls    atomic.Int64
	ackCalls atomic.Int64
	entered  chan struct{}
	release  chan struct{}
	ackErr   error
}

func (s *closeACKGateStore) PersistedThrough(string, reliablemq.Stream) int64 {
	if s.calls.Add(1) == 2 {
		close(s.entered)
		<-s.release
	}
	return 1
}

func (s *closeACKGateStore) AckOutboundThrough(ctx context.Context, queueID string, stream reliablemq.Stream, through int64) error {
	s.ackCalls.Add(1)
	if s.ackErr != nil {
		return s.ackErr
	}
	return s.DurableStore.AckOutboundThrough(ctx, queueID, stream, through)
}
