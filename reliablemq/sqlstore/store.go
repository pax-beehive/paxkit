package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/pax-beehive/paxkit/reliablemq"
)

const (
	DefaultTableName           = "reliablemq_frames"
	DefaultQueueStateTableName = "reliablemq_frames_queue_state"
)

type Option func(*config)

type config struct {
	TableName string
}

func WithTableName(tableName string) Option {
	return func(cfg *config) {
		cfg.TableName = tableName
	}
}

type Store struct {
	db                  *sql.DB
	tableName           string
	queueStateTableName string
	dialect             dialect
}

type dialectName string

const (
	dialectSQLite   dialectName = "sqlite"
	dialectPostgres dialectName = "postgres"
)

type dialect interface {
	name() dialectName
	insertIgnorePrefix() string
	insertIgnoreSuffix() string
	bind(int) string
	createTableSQL(string) string
	createQueueStateTableSQL(string) string
	createReplayIndexSQL(string) string
	selectForUpdateSuffix() string
}

type sqliteDialect struct{}

func (sqliteDialect) name() dialectName          { return dialectSQLite }
func (sqliteDialect) insertIgnorePrefix() string { return "INSERT OR IGNORE" }
func (sqliteDialect) insertIgnoreSuffix() string { return "" }
func (sqliteDialect) bind(int) string            { return "?" }
func (sqliteDialect) selectForUpdateSuffix() string {
	return ""
}
func (sqliteDialect) createTableSQL(table string) string {
	return `
		CREATE TABLE IF NOT EXISTS ` + table + ` (
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
			UNIQUE(queue_id, stream, seq, direction)
		)
		`
}
func (sqliteDialect) createQueueStateTableSQL(table string) string {
	return `
			CREATE TABLE IF NOT EXISTS ` + table + ` (
				queue_id TEXT NOT NULL,
				stream TEXT NOT NULL,
				next_outbound_seq INTEGER NOT NULL DEFAULT 1,
				inbound_applied_through INTEGER NOT NULL DEFAULT 0,
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL,
				PRIMARY KEY (queue_id, stream)
			)
		`
}
func (sqliteDialect) createReplayIndexSQL(table string) string {
	return `
		CREATE INDEX IF NOT EXISTS ` + table + `_replay_idx
		ON ` + table + ` (queue_id, stream, direction, status, seq)
	`
}

type postgresDialect struct{}

func (postgresDialect) name() dialectName          { return dialectPostgres }
func (postgresDialect) insertIgnorePrefix() string { return "INSERT" }
func (postgresDialect) insertIgnoreSuffix() string {
	return "ON CONFLICT (queue_id, stream, seq, direction) DO NOTHING"
}
func (postgresDialect) bind(n int) string { return fmt.Sprintf("$%d", n) }
func (postgresDialect) selectForUpdateSuffix() string {
	return " FOR UPDATE"
}
func (postgresDialect) createTableSQL(table string) string {
	return `
		CREATE TABLE IF NOT EXISTS ` + table + ` (
			id BIGSERIAL PRIMARY KEY,
			queue_id TEXT NOT NULL,
			stream TEXT NOT NULL,
			seq BIGINT NOT NULL,
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
		`
}
func (postgresDialect) createQueueStateTableSQL(table string) string {
	return `
			CREATE TABLE IF NOT EXISTS ` + table + ` (
				queue_id TEXT NOT NULL,
				stream TEXT NOT NULL,
				next_outbound_seq BIGINT NOT NULL DEFAULT 1,
				inbound_applied_through BIGINT NOT NULL DEFAULT 0,
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL,
				PRIMARY KEY (queue_id, stream)
			)
		`
}
func (postgresDialect) createReplayIndexSQL(table string) string {
	return `
		CREATE INDEX IF NOT EXISTS ` + table + `_replay_idx
		ON ` + table + ` (queue_id, stream, direction, status, seq)
	`
}

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func NewSQLite(db *sql.DB, opts ...Option) (*Store, error) {
	return newStore(db, sqliteDialect{}, opts...)
}

func NewPostgres(db *sql.DB, opts ...Option) (*Store, error) {
	return newStore(db, postgresDialect{}, opts...)
}

func newStore(db *sql.DB, d dialect, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("sqlstore: db is required")
	}
	cfg := config{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	tableName := cfg.TableName
	if tableName == "" {
		tableName = DefaultTableName
	}
	if !identifierPattern.MatchString(tableName) {
		return nil, fmt.Errorf("sqlstore: invalid table name %q", tableName)
	}
	store := &Store{
		db:                  db,
		tableName:           tableName,
		queueStateTableName: queueStateTableName(tableName),
		dialect:             d,
	}
	if err := store.migrate(context.Background()); err != nil {
		return nil, err
	}
	return store, nil
}

func queueStateTableName(tableName string) string {
	if tableName == "transport_journal" {
		return "transport_queue_state"
	}
	if tableName == DefaultTableName {
		return DefaultQueueStateTableName
	}
	return tableName + "_queue_state"
}

func (s *Store) AppendOutboundData(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	payload json.RawMessage,
	metadata reliablemq.Metadata,
) (reliablemq.Frame, error) {
	if !json.Valid(payload) {
		return reliablemq.Frame{}, fmt.Errorf("%w: payload must be valid JSON", reliablemq.ErrInvalidFrame)
	}
	return s.appendOutbound(ctx, queueID, stream, reliablemq.FrameKindData, payload, "", metadata)
}

func (s *Store) AppendOutboundTombstone(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	errorMessage string,
	metadata reliablemq.Metadata,
) (reliablemq.Frame, error) {
	return s.appendOutbound(ctx, queueID, stream, reliablemq.FrameKindTombstone, nil, errorMessage, metadata)
}

func (s *Store) SaveInboundIfAbsent(
	ctx context.Context,
	frame reliablemq.Frame,
) (bool, reliablemq.Frame, error) {
	if err := reliablemq.ValidateFrame(frame); err != nil {
		return false, reliablemq.Frame{}, err
	}
	if frame.Key.Direction != reliablemq.DirectionInbound {
		return false, reliablemq.Frame{}, fmt.Errorf("%w: inbound frame direction is %q", reliablemq.ErrInvalidFrame, frame.Key.Direction)
	}
	now := time.Now().UTC()
	frame = frame.Clone()
	frame.Status = reliablemq.StatusReceived
	frame.CreatedAt = now
	frame.UpdatedAt = now

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, reliablemq.Frame{}, err
	}
	defer rollback(tx)

	applied, err := s.inboundAppliedThroughTx(ctx, tx, frame.Key.QueueID, frame.Key.Stream)
	if err != nil {
		return false, reliablemq.Frame{}, err
	}
	if frame.Key.Seq <= applied {
		frame.Status = reliablemq.StatusApplied
		if err := tx.Commit(); err != nil {
			return false, reliablemq.Frame{}, err
		}
		return false, frame, nil
	}

	inserted, err := s.insertFrame(ctx, tx, frame)
	if err != nil {
		return false, reliablemq.Frame{}, err
	}
	var stored reliablemq.Frame
	if inserted {
		stored = frame
	} else {
		stored, err = s.getTx(ctx, tx, frame.Key)
		if err != nil {
			return false, reliablemq.Frame{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, reliablemq.Frame{}, err
	}
	return inserted, stored, nil
}

func (s *Store) ListOutboundReplay(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	limit int,
) ([]reliablemq.Frame, error) {
	return s.list(ctx, queueID, stream, reliablemq.DirectionOutbound, limit, []reliablemq.Status{
		reliablemq.StatusPending,
		reliablemq.StatusSent,
	})
}

func (s *Store) ListOutboundReplayFrom(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	fromSeq int64,
	limit int,
) ([]reliablemq.Frame, error) {
	if fromSeq <= 0 {
		return nil, fmt.Errorf("%w: from seq must be positive", reliablemq.ErrInvalidFrame)
	}
	return s.listFrom(ctx, queueID, stream, reliablemq.DirectionOutbound, fromSeq, limit, []reliablemq.Status{
		reliablemq.StatusPending,
		reliablemq.StatusSent,
	})
}

func (s *Store) ListInboundReplay(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	limit int,
) ([]reliablemq.Frame, error) {
	return s.list(ctx, queueID, stream, reliablemq.DirectionInbound, limit, []reliablemq.Status{
		reliablemq.StatusReceived,
	})
}

func (s *Store) LoadQueueState(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
) (reliablemq.QueueState, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return reliablemq.QueueState{}, err
	}
	defer rollback(tx)
	if err := s.ensureQueueStateTx(ctx, tx, queueID, stream); err != nil {
		return reliablemq.QueueState{}, err
	}
	b := s.dialect.bind
	var state reliablemq.QueueState
	err = tx.QueryRowContext(ctx, `
		SELECT next_outbound_seq, inbound_applied_through
		FROM `+s.queueStateTableName+`
		WHERE queue_id = `+b(1)+` AND stream = `+b(2)+`
	`, queueID, string(stream)).Scan(&state.NextOutboundSeq, &state.InboundAppliedThrough)
	if err != nil {
		return reliablemq.QueueState{}, err
	}
	if err := tx.Commit(); err != nil {
		return reliablemq.QueueState{}, err
	}
	return state, nil
}

func (s *Store) LoadProducerReconcileCheckpoint(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
) (reliablemq.ProducerReconcileCheckpoint, error) {
	state, err := s.LoadQueueState(ctx, queueID, stream)
	if err != nil {
		return reliablemq.ProducerReconcileCheckpoint{}, err
	}
	frames, err := s.ListOutboundReplay(ctx, queueID, stream, 1_000_000)
	if err != nil {
		return reliablemq.ProducerReconcileCheckpoint{}, err
	}
	var replayFrom int64
	var replayThrough int64
	for _, frame := range frames {
		if replayFrom == 0 || frame.Key.Seq < replayFrom {
			replayFrom = frame.Key.Seq
		}
		if frame.Key.Seq > replayThrough {
			replayThrough = frame.Key.Seq
		}
	}
	return reliablemq.ProducerReconcileCheckpoint{
		QueueID:         queueID,
		Stream:          stream,
		ProducerNextSeq: state.NextOutboundSeq,
		ReplayFrom:      replayFrom,
		ReplayThrough:   replayThrough,
	}, nil
}

func (s *Store) AdvanceProducerNextSeq(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	nextSeq int64,
) error {
	if nextSeq <= 0 {
		return fmt.Errorf("%w: next seq must be positive", reliablemq.ErrInvalidFrame)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := s.updateNextOutboundSeqAtLeastTx(ctx, tx, queueID, stream, nextSeq); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ConsumerAckedThrough(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
) (int64, error) {
	b := s.dialect.bind
	rows, err := s.db.QueryContext(ctx, `
		SELECT seq
		FROM `+s.tableName+`
		WHERE queue_id = `+b(1)+` AND stream = `+b(2)+`
			AND direction = `+b(3)+`
			AND status IN (`+b(4)+`, `+b(5)+`, `+b(6)+`)
		ORDER BY seq ASC
	`, queueID, string(stream), string(reliablemq.DirectionInbound),
		string(reliablemq.StatusReceived), string(reliablemq.StatusApplied), string(reliablemq.StatusRejected))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var through int64
	next := int64(1)
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return 0, err
		}
		if seq < next {
			continue
		}
		if seq != next {
			break
		}
		through = seq
		next++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return through, nil
}

func (s *Store) ApplyBatch(ctx context.Context, batch reliablemq.StoreBatch) error {
	if len(batch.Frames) == 0 && len(batch.Patches) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	for _, frame := range batch.Frames {
		if err := reliablemq.ValidateFrame(frame); err != nil {
			return err
		}
		now := time.Now().UTC()
		frame = frame.Clone()
		if frame.CreatedAt.IsZero() {
			frame.CreatedAt = now
		}
		if frame.UpdatedAt.IsZero() {
			frame.UpdatedAt = frame.CreatedAt
		}
		if frame.Key.Direction == reliablemq.DirectionOutbound {
			if err := s.updateNextOutboundSeqAtLeastTx(ctx, tx, frame.Key.QueueID, frame.Key.Stream, frame.Key.Seq+1); err != nil {
				return err
			}
		} else if err := s.ensureQueueStateTx(ctx, tx, frame.Key.QueueID, frame.Key.Stream); err != nil {
			return err
		}
		if _, err := s.insertFrame(ctx, tx, frame); err != nil {
			return err
		}
		if err := s.applyFrameStateTx(ctx, tx, frame); err != nil {
			return err
		}
		if frame.Key.Direction == reliablemq.DirectionInbound &&
			(frame.Status == reliablemq.StatusApplied || frame.Status == reliablemq.StatusRejected) {
			if err := s.updateInboundAppliedThroughTx(ctx, tx, frame.Key.QueueID, frame.Key.Stream, frame.Key.Seq); err != nil {
				return err
			}
		}
	}
	for _, patch := range batch.Patches {
		if err := s.applyPatchTx(ctx, tx, patch); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) MarkSent(ctx context.Context, key reliablemq.FrameKey) error {
	return s.updateStatus(ctx, key, reliablemq.StatusSent, "")
}

func (s *Store) AckOutboundThrough(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	throughSeq int64,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := s.ackOutboundThroughTx(ctx, tx, queueID, stream, throughSeq); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) MarkApplied(ctx context.Context, key reliablemq.FrameKey) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := s.updateStatusTx(ctx, tx, key, reliablemq.StatusApplied, ""); err != nil {
		return err
	}
	if key.Direction == reliablemq.DirectionInbound {
		if err := s.updateInboundAppliedThroughTx(ctx, tx, key.QueueID, key.Stream, key.Seq); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) MarkRejected(ctx context.Context, key reliablemq.FrameKey, errorMessage string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := s.updateStatusTx(ctx, tx, key, reliablemq.StatusRejected, errorMessage); err != nil {
		return err
	}
	if key.Direction == reliablemq.DirectionInbound {
		if err := s.updateInboundAppliedThroughTx(ctx, tx, key.QueueID, key.Stream, key.Seq); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) RecordSendFailure(ctx context.Context, key reliablemq.FrameKey, errorMessage string) error {
	return s.updateError(ctx, key, errorMessage)
}

func (s *Store) RecordDispatchFailure(ctx context.Context, key reliablemq.FrameKey, errorMessage string) error {
	return s.updateError(ctx, key, errorMessage)
}

func (s *Store) UpdateMetadata(ctx context.Context, key reliablemq.FrameKey, metadata reliablemq.Metadata) error {
	data, err := marshalMetadata(metadata)
	if err != nil {
		return err
	}
	b := s.dialect.bind
	result, err := s.db.ExecContext(ctx, `
		UPDATE `+s.tableName+`
		SET metadata_json = `+b(1)+`, updated_at = `+b(2)+`
		WHERE queue_id = `+b(3)+` AND stream = `+b(4)+`
			AND seq = `+b(5)+` AND direction = `+b(6)+`
	`, string(data), formatTime(time.Now().UTC()), key.QueueID, string(key.Stream), key.Seq, string(key.Direction))
	if err != nil {
		return err
	}
	return requireAffected(result)
}

func (s *Store) Get(ctx context.Context, key reliablemq.FrameKey) (reliablemq.Frame, bool, error) {
	frame, err := s.get(ctx, key)
	if err == sql.ErrNoRows {
		return reliablemq.Frame{}, false, nil
	}
	if err != nil {
		return reliablemq.Frame{}, false, err
	}
	return frame, true, nil
}

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, s.dialect.createTableSQL(s.tableName))
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, s.dialect.createQueueStateTableSQL(s.queueStateTableName)); err != nil {
		return err
	}
	if err := s.validateSchema(ctx); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, s.dialect.createReplayIndexSQL(s.tableName))
	return err
}

func (s *Store) validateSchema(ctx context.Context) error {
	if s.dialect.name() == dialectPostgres {
		return s.postgresValidateSchema(ctx)
	}
	columns, err := s.sqliteColumns(ctx)
	if err != nil {
		return err
	}
	if err := validateColumns(s.tableName, columns); err != nil {
		return err
	}
	if err := s.sqliteValidateUniqueKey(ctx); err != nil {
		return err
	}
	return nil
}

func requiredColumns() map[string]bool {
	return map[string]bool{
		"id": true, "queue_id": true, "stream": true, "seq": true,
		"direction": true, "kind": true, "payload_json": false,
		"metadata_json": true, "status": true, "error_message": true,
		"created_at": true, "updated_at": true,
	}
}

func validateColumns(tableName string, columns map[string]schemaColumn) error {
	for name, notNull := range requiredColumns() {
		col, ok := columns[name]
		if !ok {
			return fmt.Errorf("sqlstore: table %s missing required column %s", tableName, name)
		}
		if notNull && !col.notNull && name != "id" {
			return fmt.Errorf("sqlstore: table %s column %s must be NOT NULL", tableName, name)
		}
	}
	return nil
}

type schemaColumn struct {
	notNull bool
}

func (s *Store) sqliteColumns(ctx context.Context) (map[string]schemaColumn, error) {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+s.tableName+`)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := make(map[string]schemaColumn)
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return nil, err
		}
		columns[name] = schemaColumn{notNull: notNull == 1}
	}
	return columns, rows.Err()
}

func (s *Store) postgresValidateSchema(ctx context.Context) error {
	columns, err := s.postgresColumns(ctx)
	if err != nil {
		return err
	}
	if err := validateColumns(s.tableName, columns); err != nil {
		return err
	}
	if err := s.postgresValidateUniqueKey(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) postgresColumns(ctx context.Context) (map[string]schemaColumn, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT column_name, is_nullable
		FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = $1
	`, s.tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := make(map[string]schemaColumn)
	for rows.Next() {
		var name string
		var nullable string
		if err := rows.Scan(&name, &nullable); err != nil {
			return nil, err
		}
		columns[name] = schemaColumn{notNull: nullable == "NO"}
	}
	return columns, rows.Err()
}

func (s *Store) postgresValidateUniqueKey(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT i.relname AS index_name, a.attname AS column_name
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
		WHERE n.nspname = current_schema()
			AND t.relname = $1
			AND ix.indisunique
		ORDER BY i.relname, k.ord
	`, s.tableName)
	if err != nil {
		return err
	}
	defer rows.Close()
	constraints := make(map[string][]string)
	for rows.Next() {
		var constraintName string
		var columnName string
		if err := rows.Scan(&constraintName, &columnName); err != nil {
			return err
		}
		constraints[constraintName] = append(constraints[constraintName], columnName)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, cols := range constraints {
		if sameColumns(cols, []string{"queue_id", "stream", "seq", "direction"}) {
			return nil
		}
	}
	return fmt.Errorf("sqlstore: table %s missing unique key queue_id,stream,seq,direction", s.tableName)
}

func (s *Store) sqliteValidateUniqueKey(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA index_list(`+s.tableName+`)`)
	if err != nil {
		return err
	}
	var uniqueIndexes []string
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return err
		}
		if unique != 1 {
			continue
		}
		uniqueIndexes = append(uniqueIndexes, name)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, indexName := range uniqueIndexes {
		cols, err := s.sqliteIndexColumns(ctx, indexName)
		if err != nil {
			return err
		}
		if sameColumns(cols, []string{"queue_id", "stream", "seq", "direction"}) {
			return nil
		}
	}
	return fmt.Errorf("sqlstore: table %s missing unique key queue_id,stream,seq,direction", s.tableName)
}

func sameColumns(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]int, len(got))
	for _, col := range got {
		seen[col]++
	}
	for _, col := range want {
		if seen[col] == 0 {
			return false
		}
		seen[col]--
	}
	return true
}

func placeholders(d dialect, start int, count int) string {
	var builder strings.Builder
	for i := 0; i < count; i++ {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(d.bind(start + i))
	}
	return builder.String()
}

func (s *Store) sqliteIndexColumns(ctx context.Context, indexName string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `PRAGMA index_info(`+indexName+`)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var seqno int
		var cid int
		var name string
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	return cols, rows.Err()
}

func (s *Store) appendOutbound(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	kind reliablemq.FrameKind,
	payload json.RawMessage,
	errorMessage string,
	metadata reliablemq.Metadata,
) (reliablemq.Frame, error) {
	if queueID == "" {
		return reliablemq.Frame{}, fmt.Errorf("%w: queue_id is required", reliablemq.ErrInvalidFrame)
	}
	if stream == "" {
		return reliablemq.Frame{}, fmt.Errorf("%w: stream is required", reliablemq.ErrInvalidFrame)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return reliablemq.Frame{}, err
	}
	defer rollback(tx)

	seq, err := s.allocateOutboundSeqTx(ctx, tx, queueID, stream)
	if err != nil {
		return reliablemq.Frame{}, err
	}
	now := time.Now().UTC()
	frame := reliablemq.Frame{
		Key: reliablemq.FrameKey{
			QueueID:   queueID,
			Stream:    stream,
			Seq:       seq,
			Direction: reliablemq.DirectionOutbound,
		},
		Kind:         kind,
		Payload:      append(json.RawMessage(nil), payload...),
		Metadata:     metadata.Clone(),
		Status:       reliablemq.StatusPending,
		ErrorMessage: errorMessage,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := reliablemq.ValidateFrame(frame); err != nil {
		return reliablemq.Frame{}, err
	}
	if _, err := s.insertFrame(ctx, tx, frame); err != nil {
		return reliablemq.Frame{}, err
	}
	if err := tx.Commit(); err != nil {
		return reliablemq.Frame{}, err
	}
	return frame, nil
}

func (s *Store) allocateOutboundSeqTx(
	ctx context.Context,
	tx *sql.Tx,
	queueID string,
	stream reliablemq.Stream,
) (int64, error) {
	if err := s.ensureQueueStateTx(ctx, tx, queueID, stream); err != nil {
		return 0, err
	}
	b := s.dialect.bind
	var seq int64
	if err := tx.QueryRowContext(ctx, `
		SELECT next_outbound_seq
		FROM `+s.queueStateTableName+`
		WHERE queue_id = `+b(1)+` AND stream = `+b(2)+s.dialect.selectForUpdateSuffix()+`
	`, queueID, string(stream)).Scan(&seq); err != nil {
		return 0, err
	}
	_, err := tx.ExecContext(ctx, `
		UPDATE `+s.queueStateTableName+`
		SET next_outbound_seq = next_outbound_seq + 1, updated_at = `+b(1)+`
		WHERE queue_id = `+b(2)+` AND stream = `+b(3)+`
	`, formatTime(time.Now().UTC()), queueID, string(stream))
	if err != nil {
		return 0, err
	}
	return seq, nil
}

func (s *Store) updateNextOutboundSeqAtLeastTx(
	ctx context.Context,
	tx *sql.Tx,
	queueID string,
	stream reliablemq.Stream,
	nextSeq int64,
) error {
	if err := s.ensureQueueStateTx(ctx, tx, queueID, stream); err != nil {
		return err
	}
	b := s.dialect.bind
	_, err := tx.ExecContext(ctx, `
		UPDATE `+s.queueStateTableName+`
		SET next_outbound_seq = CASE
				WHEN next_outbound_seq < `+b(1)+` THEN `+b(2)+`
				ELSE next_outbound_seq
			END,
			updated_at = `+b(3)+`
		WHERE queue_id = `+b(4)+` AND stream = `+b(5)+`
	`, nextSeq, nextSeq, formatTime(time.Now().UTC()), queueID, string(stream))
	return err
}

func (s *Store) ensureQueueStateTx(
	ctx context.Context,
	tx *sql.Tx,
	queueID string,
	stream reliablemq.Stream,
) error {
	now := formatTime(time.Now().UTC())
	b := s.dialect.bind
	_, err := tx.ExecContext(ctx, `
		`+s.dialect.insertIgnorePrefix()+` INTO `+s.queueStateTableName+` (
			queue_id, stream, next_outbound_seq, inbound_applied_through, created_at, updated_at
		)
		SELECT `+b(1)+`, `+b(2)+`, COALESCE(MAX(seq), 0) + 1, 0, `+b(3)+`, `+b(4)+`
		FROM `+s.tableName+`
		WHERE queue_id = `+b(5)+` AND stream = `+b(6)+` AND direction = `+b(7)+`
		`+queueStateInsertIgnoreSuffix(s.dialect)+`
	`, queueID, string(stream), now, now, queueID, string(stream), string(reliablemq.DirectionOutbound))
	return err
}

func queueStateInsertIgnoreSuffix(d dialect) string {
	if d.name() == dialectPostgres {
		return "ON CONFLICT (queue_id, stream) DO NOTHING"
	}
	return ""
}

func (s *Store) inboundAppliedThroughTx(
	ctx context.Context,
	tx *sql.Tx,
	queueID string,
	stream reliablemq.Stream,
) (int64, error) {
	if err := s.ensureQueueStateTx(ctx, tx, queueID, stream); err != nil {
		return 0, err
	}
	b := s.dialect.bind
	var applied int64
	if err := tx.QueryRowContext(ctx, `
		SELECT inbound_applied_through
		FROM `+s.queueStateTableName+`
		WHERE queue_id = `+b(1)+` AND stream = `+b(2)+`
	`, queueID, string(stream)).Scan(&applied); err != nil {
		return 0, err
	}
	return applied, nil
}

func (s *Store) updateInboundAppliedThroughTx(
	ctx context.Context,
	tx *sql.Tx,
	queueID string,
	stream reliablemq.Stream,
	seq int64,
) error {
	if err := s.ensureQueueStateTx(ctx, tx, queueID, stream); err != nil {
		return err
	}
	b := s.dialect.bind
	_, err := tx.ExecContext(ctx, `
		UPDATE `+s.queueStateTableName+`
		SET inbound_applied_through = CASE
				WHEN inbound_applied_through < `+b(1)+` THEN `+b(2)+`
				ELSE inbound_applied_through
			END,
			updated_at = `+b(3)+`
		WHERE queue_id = `+b(4)+` AND stream = `+b(5)+`
	`, seq, seq, formatTime(time.Now().UTC()), queueID, string(stream))
	return err
}

func (s *Store) insertFrame(ctx context.Context, tx *sql.Tx, frame reliablemq.Frame) (bool, error) {
	metadataJSON, err := marshalMetadata(frame.Metadata)
	if err != nil {
		return false, err
	}
	payload := ""
	if len(frame.Payload) > 0 {
		payload = string(frame.Payload)
	}
	b := s.dialect.bind
	result, err := tx.ExecContext(ctx, `
		`+s.dialect.insertIgnorePrefix()+` INTO `+s.tableName+` (
			queue_id, stream, seq, direction, kind, payload_json, metadata_json,
			status, error_message, created_at, updated_at
		)
		VALUES (`+b(1)+`, `+b(2)+`, `+b(3)+`, `+b(4)+`, `+b(5)+`,
			NULLIF(`+b(6)+`, ''), `+b(7)+`, `+b(8)+`, `+b(9)+`, `+b(10)+`, `+b(11)+`)
		`+s.dialect.insertIgnoreSuffix()+`
	`, frame.Key.QueueID, string(frame.Key.Stream), frame.Key.Seq, string(frame.Key.Direction),
		string(frame.Kind), payload, string(metadataJSON), string(frame.Status),
		frame.ErrorMessage, formatTime(frame.CreatedAt), formatTime(frame.UpdatedAt))
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (s *Store) list(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	direction reliablemq.Direction,
	limit int,
	statuses []reliablemq.Status,
) ([]reliablemq.Frame, error) {
	return s.listFrom(ctx, queueID, stream, direction, 1, limit, statuses)
}

func (s *Store) listFrom(
	ctx context.Context,
	queueID string,
	stream reliablemq.Stream,
	direction reliablemq.Direction,
	fromSeq int64,
	limit int,
	statuses []reliablemq.Status,
) ([]reliablemq.Frame, error) {
	if limit <= 0 {
		limit = 100
	}
	b := s.dialect.bind
	args := []any{queueID, string(stream), string(direction), fromSeq}
	for _, status := range statuses {
		args = append(args, string(status))
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `
		SELECT queue_id, stream, seq, direction, kind, COALESCE(payload_json, ''),
			metadata_json, status, error_message, created_at, updated_at
			FROM `+s.tableName+`
			WHERE queue_id = `+b(1)+` AND stream = `+b(2)+` AND direction = `+b(3)+`
				AND seq >= `+b(4)+`
				AND status IN (`+placeholders(s.dialect, 5, len(statuses))+`)
			ORDER BY seq
			LIMIT `+b(5+len(statuses))+`
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var frames []reliablemq.Frame
	for rows.Next() {
		frame, err := scanFrame(rows)
		if err != nil {
			return nil, err
		}
		frames = append(frames, frame)
	}
	return frames, rows.Err()
}

func (s *Store) updateStatus(ctx context.Context, key reliablemq.FrameKey, status reliablemq.Status, errorMessage string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := s.updateStatusTx(ctx, tx, key, status, errorMessage); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) updateStatusTx(
	ctx context.Context,
	tx *sql.Tx,
	key reliablemq.FrameKey,
	status reliablemq.Status,
	errorMessage string,
) error {
	b := s.dialect.bind
	result, err := tx.ExecContext(ctx, `
		UPDATE `+s.tableName+`
		SET status = `+b(1)+`, error_message = `+b(2)+`, updated_at = `+b(3)+`
		WHERE queue_id = `+b(4)+` AND stream = `+b(5)+`
			AND seq = `+b(6)+` AND direction = `+b(7)+`
	`, string(status), errorMessage, formatTime(time.Now().UTC()), key.QueueID, string(key.Stream), key.Seq, string(key.Direction))
	if err != nil {
		return err
	}
	return requireAffected(result)
}

func (s *Store) applyPatchTx(ctx context.Context, tx *sql.Tx, patch reliablemq.StorePatch) error {
	if patch.HasMetadata {
		metadata, err := marshalMetadata(patch.Metadata)
		if err != nil {
			return err
		}
		b := s.dialect.bind
		result, err := tx.ExecContext(ctx, `
			UPDATE `+s.tableName+`
			SET metadata_json = `+b(1)+`, updated_at = `+b(2)+`
			WHERE queue_id = `+b(3)+` AND stream = `+b(4)+`
				AND seq = `+b(5)+` AND direction = `+b(6)+`
		`, string(metadata), formatTime(time.Now().UTC()), patch.Key.QueueID,
			string(patch.Key.Stream), patch.Key.Seq, string(patch.Key.Direction))
		if err != nil {
			return err
		}
		if err := requireAffected(result); err != nil {
			return err
		}
	}
	switch patch.Status {
	case "":
	case reliablemq.StatusSent:
		if err := s.updateStatusTx(ctx, tx, patch.Key, reliablemq.StatusSent, ""); err != nil {
			return err
		}
	case reliablemq.StatusAcked:
		if err := s.ackOutboundThroughTx(ctx, tx, patch.Key.QueueID, patch.Key.Stream, patch.Key.Seq); err != nil {
			return err
		}
	case reliablemq.StatusApplied:
		if err := s.updateStatusTx(ctx, tx, patch.Key, reliablemq.StatusApplied, ""); err != nil {
			return err
		}
		if patch.Key.Direction == reliablemq.DirectionInbound {
			if err := s.updateInboundAppliedThroughTx(ctx, tx, patch.Key.QueueID, patch.Key.Stream, patch.Key.Seq); err != nil {
				return err
			}
		}
	case reliablemq.StatusRejected:
		if err := s.updateStatusTx(ctx, tx, patch.Key, reliablemq.StatusRejected, patch.ErrorMessage); err != nil {
			return err
		}
		if patch.Key.Direction == reliablemq.DirectionInbound {
			if err := s.updateInboundAppliedThroughTx(ctx, tx, patch.Key.QueueID, patch.Key.Stream, patch.Key.Seq); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%w: invalid status %q", reliablemq.ErrInvalidFrame, patch.Status)
	}
	if patch.ErrorMessage != "" && patch.Status != reliablemq.StatusRejected {
		return s.updateErrorTx(ctx, tx, patch.Key, patch.ErrorMessage)
	}
	return nil
}

func (s *Store) applyFrameStateTx(ctx context.Context, tx *sql.Tx, frame reliablemq.Frame) error {
	if len(frame.Metadata) > 0 {
		metadata, err := marshalMetadata(frame.Metadata)
		if err != nil {
			return err
		}
		b := s.dialect.bind
		if _, err := tx.ExecContext(ctx, `
			UPDATE `+s.tableName+`
			SET metadata_json = `+b(1)+`, updated_at = `+b(2)+`
			WHERE queue_id = `+b(3)+` AND stream = `+b(4)+`
				AND seq = `+b(5)+` AND direction = `+b(6)+`
		`, string(metadata), formatTime(time.Now().UTC()), frame.Key.QueueID,
			string(frame.Key.Stream), frame.Key.Seq, string(frame.Key.Direction)); err != nil {
			return err
		}
	}
	switch frame.Status {
	case reliablemq.StatusPending, reliablemq.StatusReceived:
	case reliablemq.StatusSent:
		if err := s.updateStatusTx(ctx, tx, frame.Key, reliablemq.StatusSent, ""); err != nil {
			return err
		}
	case reliablemq.StatusAcked:
		if err := s.ackOutboundThroughTx(ctx, tx, frame.Key.QueueID, frame.Key.Stream, frame.Key.Seq); err != nil {
			return err
		}
	case reliablemq.StatusApplied:
		if err := s.updateStatusTx(ctx, tx, frame.Key, reliablemq.StatusApplied, ""); err != nil {
			return err
		}
	case reliablemq.StatusRejected:
		if err := s.updateStatusTx(ctx, tx, frame.Key, reliablemq.StatusRejected, frame.ErrorMessage); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: invalid status %q", reliablemq.ErrInvalidFrame, frame.Status)
	}
	if frame.ErrorMessage != "" && frame.Status != reliablemq.StatusRejected {
		return s.updateErrorTx(ctx, tx, frame.Key, frame.ErrorMessage)
	}
	return nil
}

func (s *Store) ackOutboundThroughTx(
	ctx context.Context,
	tx *sql.Tx,
	queueID string,
	stream reliablemq.Stream,
	throughSeq int64,
) error {
	now := formatTime(time.Now().UTC())
	b := s.dialect.bind
	_, err := tx.ExecContext(ctx, `
		UPDATE `+s.tableName+`
		SET status = `+b(1)+`, error_message = '', updated_at = `+b(2)+`
		WHERE queue_id = `+b(3)+` AND stream = `+b(4)+`
			AND direction = `+b(5)+` AND seq <= `+b(6)+`
	`, string(reliablemq.StatusAcked), now, queueID, string(stream), string(reliablemq.DirectionOutbound), throughSeq)
	return err
}

func (s *Store) updateError(ctx context.Context, key reliablemq.FrameKey, errorMessage string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := s.updateErrorTx(ctx, tx, key, errorMessage); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) updateErrorTx(ctx context.Context, tx *sql.Tx, key reliablemq.FrameKey, errorMessage string) error {
	b := s.dialect.bind
	result, err := tx.ExecContext(ctx, `
		UPDATE `+s.tableName+`
		SET error_message = `+b(1)+`, updated_at = `+b(2)+`
		WHERE queue_id = `+b(3)+` AND stream = `+b(4)+`
			AND seq = `+b(5)+` AND direction = `+b(6)+`
	`, errorMessage, formatTime(time.Now().UTC()), key.QueueID, string(key.Stream), key.Seq, string(key.Direction))
	if err != nil {
		return err
	}
	return requireAffected(result)
}

func (s *Store) get(ctx context.Context, key reliablemq.FrameKey) (reliablemq.Frame, error) {
	b := s.dialect.bind
	return s.scanOne(ctx, `
		SELECT queue_id, stream, seq, direction, kind, COALESCE(payload_json, ''),
			metadata_json, status, error_message, created_at, updated_at
		FROM `+s.tableName+`
		WHERE queue_id = `+b(1)+` AND stream = `+b(2)+`
			AND seq = `+b(3)+` AND direction = `+b(4)+`
	`, key.QueueID, string(key.Stream), key.Seq, string(key.Direction))
}

func (s *Store) getTx(ctx context.Context, tx *sql.Tx, key reliablemq.FrameKey) (reliablemq.Frame, error) {
	b := s.dialect.bind
	row := tx.QueryRowContext(ctx, `
		SELECT queue_id, stream, seq, direction, kind, COALESCE(payload_json, ''),
			metadata_json, status, error_message, created_at, updated_at
		FROM `+s.tableName+`
		WHERE queue_id = `+b(1)+` AND stream = `+b(2)+`
			AND seq = `+b(3)+` AND direction = `+b(4)+`
	`, key.QueueID, string(key.Stream), key.Seq, string(key.Direction))
	return scanFrame(row)
}

func (s *Store) scanOne(ctx context.Context, query string, args ...any) (reliablemq.Frame, error) {
	row := s.db.QueryRowContext(ctx, query, args...)
	return scanFrame(row)
}

type scanner interface {
	Scan(dest ...any) error
}

func scanFrame(row scanner) (reliablemq.Frame, error) {
	var frame reliablemq.Frame
	var stream string
	var direction string
	var kind string
	var payload string
	var metadataJSON string
	var status string
	var createdAt string
	var updatedAt string
	if err := row.Scan(
		&frame.Key.QueueID,
		&stream,
		&frame.Key.Seq,
		&direction,
		&kind,
		&payload,
		&metadataJSON,
		&status,
		&frame.ErrorMessage,
		&createdAt,
		&updatedAt,
	); err != nil {
		return reliablemq.Frame{}, err
	}
	frame.Key.Stream = reliablemq.Stream(stream)
	frame.Key.Direction = reliablemq.Direction(direction)
	frame.Kind = reliablemq.FrameKind(kind)
	frame.Status = reliablemq.Status(status)
	if payload != "" {
		frame.Payload = json.RawMessage(payload)
	}
	if err := json.Unmarshal([]byte(metadataJSON), &frame.Metadata); err != nil {
		return reliablemq.Frame{}, err
	}
	frame.CreatedAt = parseTime(createdAt)
	frame.UpdatedAt = parseTime(updatedAt)
	return frame, nil
}

func marshalMetadata(metadata reliablemq.Metadata) ([]byte, error) {
	if metadata == nil {
		metadata = reliablemq.Metadata{}
	}
	return json.Marshal(metadata)
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		t = time.Now().UTC()
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(value string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, value)
	return t
}

func requireAffected(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func rollback(tx *sql.Tx) {
	_ = tx.Rollback()
}
