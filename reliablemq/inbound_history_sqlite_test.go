package reliablemq_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pax-beehive/paxkit/reliablemq"
	"github.com/pax-beehive/paxkit/reliablemq/memory"
	"github.com/pax-beehive/paxkit/reliablemq/sqlstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInboundReplayGivenAcknowledgedSQLiteHistoryGapThenFailsClosed(t *testing.T) {
	for _, legacyTerminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "acknowledged_missing_row", true: "legacy_terminal_prefix"}[legacyTerminal], func(t *testing.T) {
			// Given a durable receipt cursor that cannot be reconciled with replay rows.
			db, store := historySQLiteStore(t)
			if legacyTerminal {
				insertHistoryFrame(t, db, 1, "applied")
			}
			insertHistoryFrame(t, db, 2, "received")
			if !legacyTerminal {
				_, err := db.Exec(`INSERT INTO reliablemq_frames_queue_state
				 (queue_id,stream,next_outbound_seq,outbound_acked_through,inbound_acked_through,inbound_applied_through,created_at,updated_at)
				 VALUES('conn_1','acp',1,0,2,0,?,?)`, historyTimestamp, historyTimestamp)
				require.NoError(t, err)
			}
			dispatched := false
			engine := reliablemq.NewEngine(reliablemq.Config{}, store, nil, reliablemq.DispatcherFunc(func(context.Context, reliablemq.Frame) error {
				dispatched = true
				return nil
			}))

			// When the replay barrier is checked before opening the tunnel.
			err := engine.ReplayInbound(context.Background(), "conn_1", reliablemq.StreamACP, 1000)

			// Then it does not claim recovery or execute an uncertain historical request.
			require.ErrorIs(t, err, reliablemq.ErrInboundHistoryInconsistent)
			var detail *reliablemq.InboundHistoryError
			require.ErrorAs(t, err, &detail)
			assert.Equal(t, int64(1), detail.ExpectedSeq)
			assert.Equal(t, int64(2), detail.Seq)
			assert.Equal(t, int64(2), detail.AckedThrough)
			assert.Equal(t, int64(0), detail.AppliedThrough)
			assert.Contains(t, err.Error(), "needs seq 1")
			assert.False(t, dispatched)
			frame, found, err := store.Get(context.Background(), historyKey(2))
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, reliablemq.StatusReceived, frame.Status)
			state, err := store.LoadQueueState(context.Background(), "conn_1", reliablemq.StreamACP)
			require.NoError(t, err)
			assert.Equal(t, int64(0), state.InboundAppliedThrough)
			assert.Equal(t, int64(2), state.InboundAckedThrough)
			if legacyTerminal {
				frame, found, err = store.Get(context.Background(), historyKey(1))
				require.NoError(t, err)
				require.True(t, found)
				assert.Equal(t, reliablemq.StatusApplied, frame.Status)
			}
		})
	}
}

func TestInboundReplayGivenFillableSQLiteGapThenWaitsForPeerAndPreservesOrder(t *testing.T) {
	// Given sequence two was received before sequence one and was never ACKed.
	db, store := historySQLiteStore(t)
	insertHistoryFrame(t, db, 2, "received")
	var dispatched []int64
	engine := reliablemq.NewEngine(reliablemq.Config{}, store, nil, reliablemq.DispatcherFunc(func(_ context.Context, frame reliablemq.Frame) error {
		dispatched = append(dispatched, frame.Key.Seq)
		return nil
	}))
	require.NoError(t, engine.ReplayInbound(context.Background(), "conn_1", reliablemq.StreamACP, 1000))
	assert.Empty(t, dispatched)

	// When the peer supplies the missing frame after reconciliation.
	_, _, err := store.SaveInboundIfAbsent(context.Background(), reliablemq.Frame{
		Key: historyKey(1), Kind: reliablemq.FrameKindData, Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	require.NoError(t, engine.ReplayInbound(context.Background(), "conn_1", reliablemq.StreamACP, 1000))

	// Then ordinary out-of-order network arrival remains recoverable.
	assert.Equal(t, []int64{1, 2}, dispatched)
	state, err := store.LoadQueueState(context.Background(), "conn_1", reliablemq.StreamACP)
	require.NoError(t, err)
	assert.Equal(t, int64(2), state.InboundAppliedThrough)
}

func TestInboundReplayGivenDeletedSQLiteTailThenFailsClosed(t *testing.T) {
	for _, prefixApplied := range []bool{true, false} {
		t.Run(map[bool]string{true: "empty_replay", false: "short_prefix_page"}[prefixApplied], func(t *testing.T) {
			// Given the only unprocessed tail was lost after durable receipt.
			db, store := historySQLiteStore(t)
			ctx := context.Background()
			for _, seq := range []int64{1, 2} {
				_, _, err := store.SaveInboundIfAbsent(ctx, reliablemq.Frame{
					Key: historyKey(seq), Kind: reliablemq.FrameKindData, Payload: []byte(`{}`),
				})
				require.NoError(t, err)
			}
			if prefixApplied {
				require.NoError(t, store.MarkApplied(ctx, historyKey(1)))
			}
			_, err := db.Exec(`DELETE FROM reliablemq_frames WHERE seq = 2`)
			require.NoError(t, err)
			before, err := store.LoadQueueState(ctx, "conn_1", reliablemq.StreamACP)
			require.NoError(t, err)
			var dispatched []int64
			engine := reliablemq.NewEngine(reliablemq.Config{}, store, nil, reliablemq.DispatcherFunc(func(_ context.Context, frame reliablemq.Frame) error {
				dispatched = append(dispatched, frame.Key.Seq)
				return nil
			}))

			// When there is no higher row to reveal the missing tail as an ordinary gap.
			err = engine.ReplayInbound(ctx, "conn_1", reliablemq.StreamACP, 1000)

			// Then recovery reports the missing acknowledged sequence and retains history.
			require.ErrorIs(t, err, reliablemq.ErrInboundHistoryInconsistent)
			var detail *reliablemq.InboundHistoryError
			require.ErrorAs(t, err, &detail)
			assert.Equal(t, int64(2), detail.ExpectedSeq)
			assert.Zero(t, detail.Seq)
			assert.Contains(t, err.Error(), "no replay row")
			after, err := store.LoadQueueState(ctx, "conn_1", reliablemq.StreamACP)
			require.NoError(t, err)
			assert.Equal(t, int64(2), after.InboundAckedThrough)
			assert.Equal(t, int64(1), after.InboundAppliedThrough)
			if prefixApplied {
				assert.Empty(t, dispatched)
				assert.Equal(t, before, after)
			} else {
				assert.Equal(t, []int64{1}, dispatched)
			}
			frame, found, err := store.Get(ctx, historyKey(1))
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, reliablemq.StatusApplied, frame.Status)
			_, found, err = store.Get(ctx, historyKey(2))
			require.NoError(t, err)
			assert.False(t, found)
		})
	}
}

func TestInboundCheckpointAheadOfReceiptPreservesMemoryAndSQLiteHistory(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, seq := range []int64{1, 3} {
			t.Run(backend+map[int64]string{1: "/below_applied", 3: "/new_frame"}[seq], func(t *testing.T) {
				// Given an older runtime applied sequence two without durably receiving one.
				var store historyCheckpointStore
				var lookup func(reliablemq.FrameKey) (reliablemq.Frame, bool, error)
				if backend == "memory" {
					mem := memory.New()
					store = mem
					lookup = func(key reliablemq.FrameKey) (reliablemq.Frame, bool, error) {
						frame, ok := mem.Get(key)
						return frame, ok, nil
					}
				} else {
					_, sqlite := historySQLiteStore(t)
					store = sqlite
					lookup = func(key reliablemq.FrameKey) (reliablemq.Frame, bool, error) {
						return sqlite.Get(context.Background(), key)
					}
				}
				_, _, err := store.SaveInboundIfAbsent(context.Background(), reliablemq.Frame{
					Key: historyKey(2), Kind: reliablemq.FrameKindData, Payload: []byte(`{}`),
				})
				require.NoError(t, err)
				require.NoError(t, store.MarkApplied(context.Background(), historyKey(2)))
				before, err := store.LoadQueueState(context.Background(), "conn_1", reliablemq.StreamACP)
				require.NoError(t, err)
				require.Equal(t, int64(2), before.InboundAppliedThrough)
				require.Zero(t, before.InboundAckedThrough)
				original, found, err := lookup(historyKey(2))
				require.NoError(t, err)
				require.True(t, found)
				producer, err := reliablemq.NewProducer(context.Background(), reliablemq.ProducerConfig{
					QueueID: "conn_1", Stream: reliablemq.StreamACP,
				}, store)
				require.NoError(t, err)
				t.Cleanup(func() { _ = producer.Close(context.Background()) })
				dispatched := false
				engine := reliablemq.NewEngine(reliablemq.Config{}, store, producer, reliablemq.DispatcherFunc(func(context.Context, reliablemq.Frame) error {
					dispatched = true
					return nil
				}))

				// When replay has no received rows, the invalid checkpoint is still rejected.
				err = engine.ReplayInbound(context.Background(), "conn_1", reliablemq.StreamACP, 1000)
				require.ErrorIs(t, err, reliablemq.ErrInboundHistoryInconsistent)
				var detail *reliablemq.InboundHistoryError
				require.ErrorAs(t, err, &detail)
				assert.Zero(t, detail.Seq)
				assert.Contains(t, err.Error(), "applied cursor 2 exceeds durable receipt cursor 0")

				// Receiving a missing retry must not silently ACK it or insert new history.
				err = engine.Receive(context.Background(), reliablemq.EnvelopeFromFrame(reliablemq.Frame{
					Key: historyKey(seq), Kind: reliablemq.FrameKindData, Payload: []byte(`{}`),
				}))
				require.ErrorIs(t, err, reliablemq.ErrInboundHistoryInconsistent)
				assert.False(t, dispatched)
				after, err := store.LoadQueueState(context.Background(), "conn_1", reliablemq.StreamACP)
				require.NoError(t, err)
				assert.Equal(t, before, after)
				_, found, err = lookup(historyKey(seq))
				require.NoError(t, err)
				assert.False(t, found)
				retained, found, err := lookup(historyKey(2))
				require.NoError(t, err)
				require.True(t, found)
				assert.Equal(t, original, retained)
			})
		}
	}
}

type historyCheckpointStore interface {
	reliablemq.DurableStore
	reliablemq.QueueStateStore
}

const historyTimestamp = "2026-01-01T00:00:00Z"

func historySQLiteStore(t *testing.T) (*sql.DB, *sqlstore.Store) {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "history.db"))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	store, err := sqlstore.NewSQLite(db)
	require.NoError(t, err)
	return db, store
}

func insertHistoryFrame(t *testing.T, db *sql.DB, seq int64, status string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO reliablemq_frames
	 (queue_id,stream,seq,direction,kind,payload_json,metadata_json,status,error_message,created_at,updated_at)
	 VALUES('conn_1','acp',?,'inbound','data','{}','{}',?,'',?,?)`, seq, status,
		time.Now().UTC().Format(time.RFC3339Nano), historyTimestamp)
	require.NoError(t, err)
}

func historyKey(seq int64) reliablemq.FrameKey {
	return reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: seq, Direction: reliablemq.DirectionInbound}
}
