package sqlstore

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pax-beehive/paxkit/reliablemq"
	"github.com/stretchr/testify/require"
)

func TestNewDefaultTable(t *testing.T) {
	// Given
	db := openTestDB(t)

	// When
	store, err := NewSQLite(db)

	// Then
	require.NoError(t, err)
	require.NotNil(t, store)
	require.True(t, sqliteTableExists(t, db, DefaultTableName))
	require.True(t, sqliteTableExists(t, db, DefaultQueueStateTableName))
	require.True(t, sqliteIndexExists(t, db, DefaultTableName+"_replay_idx"))
}

func TestNewCustomTable(t *testing.T) {
	// Given
	db := openTestDB(t)

	// When
	store, err := NewSQLite(db, WithTableName("custom_reliablemq_frames"))

	// Then
	require.NoError(t, err)
	require.NotNil(t, store)
	require.True(t, sqliteTableExists(t, db, "custom_reliablemq_frames"))
	require.True(t, sqliteTableExists(t, db, "custom_reliablemq_frames_queue_state"))
	require.False(t, sqliteTableExists(t, db, DefaultTableName))
}

func TestNewInvalidConfig(t *testing.T) {
	tests := []string{"frames;drop", "frames-name", "frames.name", "123frames"}
	for _, tableName := range tests {
		t.Run(tableName, func(t *testing.T) {
			// Given
			db := openTestDB(t)

			// When
			store, err := NewSQLite(db, WithTableName(tableName))

			// Then
			require.Error(t, err)
			require.Nil(t, store)
		})
	}
}

func TestNewNilDB(t *testing.T) {
	// When
	store, err := NewSQLite(nil)

	// Then
	require.Error(t, err)
	require.Nil(t, store)
}

func TestPostgresDialect(t *testing.T) {
	// When
	d := postgresDialect{}

	// Then
	require.Equal(t, "$3", d.bind(3))
	require.Equal(t, "INSERT", d.insertIgnorePrefix())
	require.Equal(t, "ON CONFLICT (queue_id, stream, seq, direction) DO NOTHING", d.insertIgnoreSuffix())
	require.Contains(t, d.createTableSQL("reliablemq_frames"), "id BIGSERIAL PRIMARY KEY")
	require.Contains(t, d.createReplayIndexSQL("reliablemq_frames"), "CREATE INDEX IF NOT EXISTS reliablemq_frames_replay_idx")
	require.Equal(t, "$4, $5, $6", placeholders(d, 4, 3))
}

func TestNewExistingTableSchemaValidation(t *testing.T) {
	tests := []struct {
		name      string
		schemaSQL string
		wantErr   bool
	}{
		{
			name: "compatible table with extra column",
			schemaSQL: `
				CREATE TABLE reliablemq_frames (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					queue_id TEXT NOT NULL,
					stream TEXT NOT NULL,
					seq INTEGER NOT NULL,
					direction TEXT NOT NULL,
					kind TEXT NOT NULL,
					payload_json TEXT,
					metadata_json TEXT NOT NULL DEFAULT '{}',
					status TEXT NOT NULL,
					error_message TEXT NOT NULL DEFAULT '',
					created_at TEXT NOT NULL,
					updated_at TEXT NOT NULL,
					extra_col TEXT,
					UNIQUE(queue_id, stream, seq, direction)
				);
				CREATE INDEX old_replay_idx
				ON reliablemq_frames (queue_id, stream, direction, status, seq)
			`,
		},
		{
			name: "missing required column",
			schemaSQL: `
				CREATE TABLE reliablemq_frames (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					queue_id TEXT NOT NULL,
					stream TEXT NOT NULL,
					seq INTEGER NOT NULL,
					direction TEXT NOT NULL,
					kind TEXT NOT NULL,
					payload_json TEXT,
					metadata_json TEXT NOT NULL DEFAULT '{}',
					status TEXT NOT NULL,
					error_message TEXT NOT NULL DEFAULT '',
					created_at TEXT NOT NULL,
					UNIQUE(queue_id, stream, seq, direction)
				)
			`,
			wantErr: true,
		},
		{
			name: "missing unique key",
			schemaSQL: `
				CREATE TABLE reliablemq_frames (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					queue_id TEXT NOT NULL,
					stream TEXT NOT NULL,
					seq INTEGER NOT NULL,
					direction TEXT NOT NULL,
					kind TEXT NOT NULL,
					payload_json TEXT,
					metadata_json TEXT NOT NULL DEFAULT '{}',
					status TEXT NOT NULL,
					error_message TEXT NOT NULL DEFAULT '',
					created_at TEXT NOT NULL,
					updated_at TEXT NOT NULL
				)
			`,
			wantErr: true,
		},
		{
			name: "nullable required column",
			schemaSQL: `
				CREATE TABLE reliablemq_frames (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					queue_id TEXT,
					stream TEXT NOT NULL,
					seq INTEGER NOT NULL,
					direction TEXT NOT NULL,
					kind TEXT NOT NULL,
					payload_json TEXT,
					metadata_json TEXT NOT NULL DEFAULT '{}',
					status TEXT NOT NULL,
					error_message TEXT NOT NULL DEFAULT '',
					created_at TEXT NOT NULL,
					updated_at TEXT NOT NULL,
					UNIQUE(queue_id, stream, seq, direction)
				)
			`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			db := openTestDB(t)
			_, err := db.Exec(tt.schemaSQL)
			require.NoError(t, err)

			// When
			store, err := NewSQLite(db)

			// Then
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, store)
			} else {
				require.NoError(t, err)
				require.NotNil(t, store)
			}
		})
	}
}

func TestAppendOutboundData(t *testing.T) {
	// Given
	store := newTestStore(t)
	ctx := context.Background()

	// When
	first, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{"n":1}`), reliablemq.Metadata{"agent_id": "agent_1"})
	require.NoError(t, err)
	second, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{"n":2}`), nil)
	require.NoError(t, err)
	third, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{"n":3}`), nil)
	require.NoError(t, err)

	// Then
	require.Equal(t, []int64{1, 2, 3}, []int64{first.Key.Seq, second.Key.Seq, third.Key.Seq})
	require.Equal(t, reliablemq.DirectionOutbound, first.Key.Direction)
	require.Equal(t, reliablemq.FrameKindData, first.Kind)
	require.Equal(t, reliablemq.StatusPending, first.Status)
	require.JSONEq(t, `{"n":1}`, string(first.Payload))
	require.Equal(t, "agent_1", first.Metadata["agent_id"])
}

func TestAppendOutboundTombstone(t *testing.T) {
	// Given
	store := newTestStore(t)

	// When
	frame, err := store.AppendOutboundTombstone(context.Background(), "conn_1", reliablemq.StreamACP, "blocked", reliablemq.Metadata{"agent_id": "agent_1"})

	// Then
	require.NoError(t, err)
	require.Equal(t, reliablemq.FrameKindTombstone, frame.Kind)
	require.Empty(t, frame.Payload)
	require.Equal(t, "blocked", frame.ErrorMessage)
	require.Equal(t, reliablemq.StatusPending, frame.Status)
	require.Equal(t, "agent_1", frame.Metadata["agent_id"])
}

func TestAppendOutboundSeqScopes(t *testing.T) {
	// Given
	store := newTestStore(t)
	ctx := context.Background()

	// When
	queueOne, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	require.NoError(t, err)
	queueTwo, err := store.AppendOutboundData(ctx, "conn_2", reliablemq.StreamACP, []byte(`{}`), nil)
	require.NoError(t, err)
	control, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamControl, []byte(`{}`), nil)
	require.NoError(t, err)

	// Then
	require.Equal(t, int64(1), queueOne.Key.Seq)
	require.Equal(t, int64(1), queueTwo.Key.Seq)
	require.Equal(t, int64(1), control.Key.Seq)
}

func TestAppendOutboundSeqDoesNotReuseSweptJournalRows(t *testing.T) {
	// Given
	db := openTestDB(t)
	store, err := NewSQLite(db)
	require.NoError(t, err)
	ctx := context.Background()
	first, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{"n":1}`), nil)
	require.NoError(t, err)
	second, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{"n":2}`), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, []int64{first.Key.Seq, second.Key.Seq})
	_, err = db.Exec(`DELETE FROM reliablemq_frames WHERE queue_id = ? AND stream = ? AND direction = ?`,
		"conn_1", string(reliablemq.StreamACP), string(reliablemq.DirectionOutbound))
	require.NoError(t, err)

	// When
	third, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{"n":3}`), nil)

	// Then
	require.NoError(t, err)
	require.Equal(t, int64(3), third.Key.Seq)
}

func TestSaveInboundIfAbsent(t *testing.T) {
	// Given
	store := newTestStore(t)
	ctx := context.Background()
	original := reliablemq.Frame{
		Key:      reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 1, Direction: reliablemq.DirectionInbound},
		Kind:     reliablemq.FrameKindData,
		Payload:  []byte(`{"first":true}`),
		Metadata: reliablemq.Metadata{"source": "first"},
	}
	duplicate := original.Clone()
	duplicate.Payload = []byte(`{"second":true}`)
	duplicate.Metadata = reliablemq.Metadata{"source": "second"}

	// When
	inserted, stored, err := store.SaveInboundIfAbsent(ctx, original)
	require.NoError(t, err)
	insertedAgain, duplicateStored, err := store.SaveInboundIfAbsent(ctx, duplicate)
	require.NoError(t, err)

	// Then
	require.True(t, inserted)
	require.Equal(t, reliablemq.StatusReceived, stored.Status)
	require.False(t, insertedAgain)
	require.JSONEq(t, string(original.Payload), string(duplicateStored.Payload))
	require.Equal(t, "first", duplicateStored.Metadata["source"])
}

func TestSaveInboundSkipsAlreadyAppliedSweptFrame(t *testing.T) {
	// Given
	db := openTestDB(t)
	store, err := NewSQLite(db)
	require.NoError(t, err)
	ctx := context.Background()
	inbound := reliablemq.Frame{
		Key:     reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 1, Direction: reliablemq.DirectionInbound},
		Kind:    reliablemq.FrameKindData,
		Payload: []byte(`{"first":true}`),
	}
	inserted, stored, err := store.SaveInboundIfAbsent(ctx, inbound)
	require.NoError(t, err)
	require.True(t, inserted)
	require.NoError(t, store.MarkApplied(ctx, stored.Key))
	_, err = db.Exec(`DELETE FROM reliablemq_frames WHERE queue_id = ? AND stream = ? AND seq = ? AND direction = ?`,
		"conn_1", string(reliablemq.StreamACP), int64(1), string(reliablemq.DirectionInbound))
	require.NoError(t, err)

	// When
	insertedAgain, duplicateStored, err := store.SaveInboundIfAbsent(ctx, inbound)

	// Then
	require.NoError(t, err)
	require.False(t, insertedAgain)
	require.Equal(t, inbound.Key, duplicateStored.Key)
	require.Equal(t, reliablemq.StatusApplied, duplicateStored.Status)
}

func TestAckOutboundThrough(t *testing.T) {
	// Given
	store := newTestStore(t)
	ctx := context.Background()
	first, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	second, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	third, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	otherQueue, _ := store.AppendOutboundData(ctx, "conn_2", reliablemq.StreamACP, []byte(`{}`), nil)

	// When
	require.NoError(t, store.AckOutboundThrough(ctx, "conn_1", reliablemq.StreamACP, 2))

	// Then
	require.Equal(t, reliablemq.StatusAcked, mustGet(t, store, first.Key).Status)
	require.Equal(t, reliablemq.StatusAcked, mustGet(t, store, second.Key).Status)
	require.Equal(t, reliablemq.StatusPending, mustGet(t, store, third.Key).Status)
	require.Equal(t, reliablemq.StatusPending, mustGet(t, store, otherQueue.Key).Status)
}

func TestStatusTransitions(t *testing.T) {
	// Given
	store := newTestStore(t)
	ctx := context.Background()
	outbound, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	inbound := saveInbound(t, store, 1)

	// When / Then
	require.NoError(t, store.RecordSendFailure(ctx, outbound.Key, "socket closed"))
	require.Equal(t, "socket closed", mustGet(t, store, outbound.Key).ErrorMessage)
	require.NoError(t, store.MarkSent(ctx, outbound.Key))
	require.Equal(t, reliablemq.StatusSent, mustGet(t, store, outbound.Key).Status)
	require.Empty(t, mustGet(t, store, outbound.Key).ErrorMessage)

	require.NoError(t, store.RecordDispatchFailure(ctx, inbound.Key, "stdin closed"))
	require.Equal(t, reliablemq.StatusReceived, mustGet(t, store, inbound.Key).Status)
	require.Equal(t, "stdin closed", mustGet(t, store, inbound.Key).ErrorMessage)
	require.NoError(t, store.MarkApplied(ctx, inbound.Key))
	require.Equal(t, reliablemq.StatusApplied, mustGet(t, store, inbound.Key).Status)
	require.Empty(t, mustGet(t, store, inbound.Key).ErrorMessage)

	inboundRejected := saveInbound(t, store, 2)
	require.NoError(t, store.MarkRejected(ctx, inboundRejected.Key, "denied"))
	require.Equal(t, reliablemq.StatusRejected, mustGet(t, store, inboundRejected.Key).Status)
	require.Equal(t, "denied", mustGet(t, store, inboundRejected.Key).ErrorMessage)
}

func TestApplyBatchUpdatesExistingFrameFinalStatus(t *testing.T) {
	// Given
	store := newTestStore(t)
	ctx := context.Background()
	frame, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{"n":1}`), reliablemq.Metadata{"agent_id": "agent_1"})
	require.NoError(t, err)
	sent := frame.Clone()
	sent.Status = reliablemq.StatusSent
	sent.Metadata = reliablemq.Metadata{"agent_id": "agent_1", "phase": "sent"}

	// When
	err = store.ApplyBatch(ctx, reliablemq.StoreBatch{Frames: []reliablemq.Frame{sent}})

	// Then
	require.NoError(t, err)
	got := mustGet(t, store, frame.Key)
	require.Equal(t, reliablemq.StatusSent, got.Status)
	require.Equal(t, "sent", got.Metadata["phase"])
}

func TestApplyBatchAdvancesOutboundSeqCursor(t *testing.T) {
	// Given
	store := newTestStore(t)
	ctx := context.Background()
	frame := reliablemq.Frame{
		Key:     reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 7, Direction: reliablemq.DirectionOutbound},
		Kind:    reliablemq.FrameKindData,
		Payload: []byte(`{"n":7}`),
		Status:  reliablemq.StatusSent,
	}

	// When
	require.NoError(t, store.ApplyBatch(ctx, reliablemq.StoreBatch{Frames: []reliablemq.Frame{frame}}))
	next, err := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{"n":8}`), nil)

	// Then
	require.NoError(t, err)
	require.Equal(t, int64(8), next.Key.Seq)
}

func TestApplyBatchRollsBackWhenPatchFails(t *testing.T) {
	// Given
	store := newTestStore(t)
	ctx := context.Background()
	inserted := reliablemq.Frame{
		Key:     reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 1, Direction: reliablemq.DirectionOutbound},
		Kind:    reliablemq.FrameKindData,
		Payload: []byte(`{"n":1}`),
		Status:  reliablemq.StatusPending,
	}
	missingPatch := reliablemq.StorePatch{
		Key:    reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 99, Direction: reliablemq.DirectionOutbound},
		Status: reliablemq.StatusSent,
	}

	// When
	err := store.ApplyBatch(ctx, reliablemq.StoreBatch{
		Frames:  []reliablemq.Frame{inserted},
		Patches: []reliablemq.StorePatch{missingPatch},
	})

	// Then
	require.Error(t, err)
	_, ok, err := store.Get(ctx, inserted.Key)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestUpdateMetadata(t *testing.T) {
	// Given
	store := newTestStore(t)
	frame := saveInbound(t, store, 1)

	// When
	require.NoError(t, store.UpdateMetadata(context.Background(), frame.Key, reliablemq.Metadata{"after": "true"}))

	// Then
	got := mustGet(t, store, frame.Key)
	require.Equal(t, reliablemq.Metadata{"after": "true"}, got.Metadata)
}

func TestReplayLists(t *testing.T) {
	// Given
	store := newTestStore(t)
	ctx := context.Background()
	acked, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	require.NoError(t, store.AckOutboundThrough(ctx, "conn_1", reliablemq.StreamACP, acked.Key.Seq))
	pending, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	sent, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	rejectedOutbound, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	require.NoError(t, store.MarkSent(ctx, sent.Key))
	require.NoError(t, store.MarkRejected(ctx, rejectedOutbound.Key, "denied"))

	received := saveInbound(t, store, 1)
	applied := saveInbound(t, store, 2)
	rejected := saveInbound(t, store, 3)
	require.NoError(t, store.MarkApplied(ctx, applied.Key))
	require.NoError(t, store.MarkRejected(ctx, rejected.Key, "denied"))

	// When
	outbound, err := store.ListOutboundReplay(ctx, "conn_1", reliablemq.StreamACP, 1)
	require.NoError(t, err)
	inbound, err := store.ListInboundReplay(ctx, "conn_1", reliablemq.StreamACP, 10)
	require.NoError(t, err)

	// Then
	require.Len(t, outbound, 1)
	require.Equal(t, pending.Key, outbound[0].Key)
	require.Len(t, inbound, 1)
	require.Equal(t, received.Key, inbound[0].Key)
}

func TestInvalidOperations(t *testing.T) {
	// Given
	store := newTestStore(t)
	ctx := context.Background()
	missing := reliablemq.FrameKey{QueueID: "missing", Stream: reliablemq.StreamACP, Seq: 1, Direction: reliablemq.DirectionOutbound}

	// When / Then
	_, err := store.AppendOutboundData(ctx, "", reliablemq.StreamACP, []byte(`{}`), nil)
	require.Error(t, err)
	_, err = store.AppendOutboundData(ctx, "conn_1", "", []byte(`{}`), nil)
	require.Error(t, err)
	_, err = store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{`), nil)
	require.Error(t, err)
	_, _, err = store.SaveInboundIfAbsent(ctx, reliablemq.Frame{Key: reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 1, Direction: reliablemq.DirectionOutbound}, Kind: reliablemq.FrameKindData, Payload: []byte(`{}`)})
	require.Error(t, err)
	require.Error(t, store.MarkSent(ctx, missing))
	require.Error(t, store.MarkApplied(ctx, missing))
	require.Error(t, store.RecordSendFailure(ctx, missing, "missing"))
	require.Error(t, store.RecordDispatchFailure(ctx, missing, "missing"))
	require.Error(t, store.UpdateMetadata(ctx, missing, reliablemq.Metadata{"x": "y"}))
	_, ok, err := store.Get(ctx, missing)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestReplayDefaultLimit(t *testing.T) {
	// Given
	store := newTestStore(t)
	ctx := context.Background()
	first, _ := store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)

	// When
	frames, err := store.ListOutboundReplay(ctx, "conn_1", reliablemq.StreamACP, 0)

	// Then
	require.NoError(t, err)
	require.Len(t, frames, 1)
	require.Equal(t, first.Key, frames[0].Key)
}

func TestCorruptMetadataReturnsError(t *testing.T) {
	// Given
	db := openTestDB(t)
	store, err := NewSQLite(db)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO reliablemq_frames (
			queue_id, stream, seq, direction, kind, payload_json, metadata_json,
			status, error_message, created_at, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "conn_1", string(reliablemq.StreamACP), 1, string(reliablemq.DirectionInbound),
		string(reliablemq.FrameKindData), `{}`, `{`, string(reliablemq.StatusReceived), "",
		time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)

	// When
	_, _, err = store.Get(context.Background(), reliablemq.FrameKey{
		QueueID:   "conn_1",
		Stream:    reliablemq.StreamACP,
		Seq:       1,
		Direction: reliablemq.DirectionInbound,
	})

	// Then
	require.Error(t, err)
}

func TestTimeFormatting(t *testing.T) {
	// When
	formatted := formatTime(time.Time{})
	parsed := parseTime(formatted)

	// Then
	require.False(t, parsed.IsZero())
}

func TestSameColumns(t *testing.T) {
	want := []string{"queue_id", "stream", "seq", "direction"}

	require.True(t, sameColumns([]string{"stream", "queue_id", "direction", "seq"}, want))
	require.False(t, sameColumns([]string{"queue_id", "stream", "seq"}, want))
	require.False(t, sameColumns([]string{"queue_id", "stream", "seq", "seq"}, want))
}

func TestClosedDBErrors(t *testing.T) {
	// Given
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	store, err := NewSQLite(db)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	ctx := context.Background()
	key := reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 1, Direction: reliablemq.DirectionOutbound}

	// When / Then
	_, err = NewSQLite(db, WithTableName("another_table"))
	require.Error(t, err)
	_, err = store.AppendOutboundData(ctx, "conn_1", reliablemq.StreamACP, []byte(`{}`), nil)
	require.Error(t, err)
	_, _, err = store.SaveInboundIfAbsent(ctx, reliablemq.Frame{
		Key:     reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: 1, Direction: reliablemq.DirectionInbound},
		Kind:    reliablemq.FrameKindData,
		Payload: []byte(`{}`),
	})
	require.Error(t, err)
	_, err = store.ListOutboundReplay(ctx, "conn_1", reliablemq.StreamACP, 1)
	require.Error(t, err)
	require.Error(t, store.MarkSent(ctx, key))
	require.Error(t, store.RecordSendFailure(ctx, key, "closed"))
	require.Error(t, store.UpdateMetadata(ctx, key, reliablemq.Metadata{"x": "y"}))
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := NewSQLite(openTestDB(t))
	require.NoError(t, err)
	return store
}

func sqliteTableExists(t *testing.T, db *sql.DB, tableName string) bool {
	t.Helper()
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, tableName).Scan(&count)
	require.NoError(t, err)
	return count == 1
}

func sqliteIndexExists(t *testing.T, db *sql.DB, indexName string) bool {
	t.Helper()
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?`, indexName).Scan(&count)
	require.NoError(t, err)
	return count == 1
}

func saveInbound(t *testing.T, store *Store, seq int64) reliablemq.Frame {
	t.Helper()
	inserted, frame, err := store.SaveInboundIfAbsent(context.Background(), reliablemq.Frame{
		Key:     reliablemq.FrameKey{QueueID: "conn_1", Stream: reliablemq.StreamACP, Seq: seq, Direction: reliablemq.DirectionInbound},
		Kind:    reliablemq.FrameKindData,
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	require.True(t, inserted)
	return frame
}

func mustGet(t *testing.T, store *Store, key reliablemq.FrameKey) reliablemq.Frame {
	t.Helper()
	frame, ok, err := store.Get(context.Background(), key)
	require.NoError(t, err)
	require.True(t, ok)
	return frame
}
