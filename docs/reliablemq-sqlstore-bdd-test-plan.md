# reliablemq/sqlstore BDD/TDD Test Plan

`reliablemq/sqlstore` is a reusable `database/sql` implementation of
`reliablemq.DurableStore`.

It owns the durable frame table, schema validation, replay queries, status
transitions, and metadata round trips. It must stay independent from paxd,
pax-manager, and any ORM.

Implementation flow:

1. Write/update this behavior plan.
2. Write failing tests.
3. Implement only enough production code to satisfy tests.
4. Run the coverage gate.

Coverage target: **at least 90% statement coverage** for
`github.com/pax-beehive/paxkit/reliablemq/sqlstore`.

Coverage command:

```sh
go test ./reliablemq/sqlstore -coverprofile=sqlstore.coverage.out
go tool cover -func=sqlstore.coverage.out
```

The final `total:` line must be `>= 90.0%`.

## Public API

```go
package sqlstore

type Option func(*config)

const DefaultTableName = "reliablemq_frames"

func WithTableName(tableName string) Option
func NewSQLite(db *sql.DB, opts ...Option) (*Store, error)
func NewPostgres(db *sql.DB, opts ...Option) (*Store, error)
```

`TableName` is configurable and must be validated as a single SQL identifier:

```text
^[A-Za-z_][A-Za-z0-9_]*$
```

No quoting arbitrary SQL fragments. Invalid names return an error.

SQLite and Postgres constructors are implemented. Dialect selection is kept
internal; callers select the database by choosing `NewSQLite` or `NewPostgres`,
which mirrors the `database/sql` driver they opened.

The package stays on `database/sql` and does not import a concrete Postgres
driver; callers choose and register their own driver, such as `pgx` or `lib/pq`.

Unsupported databases have no constructor. For example, MySQL should be added
later as `NewMySQL` only when its SQL and schema validation are implemented.

## Schema

The store owns one frame table:

```sql
CREATE TABLE <table> (
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
);
```

Replay index:

```sql
CREATE INDEX <table>_replay_idx
ON <table>(queue_id, stream, direction, status, seq);
```

If the table already exists, `New` must validate compatibility before using it:

- all required columns exist;
- required columns are `NOT NULL`;
- the reliable identity has a unique key on
  `queue_id, stream, seq, direction`;
- extra columns are allowed;
- ordinary non-unique indexes do not satisfy the unique-key requirement.

## BDD Specs

### Config and Migration

#### Given empty config, when New is called, then it creates the default table

Expected:

- `DefaultTableName` exists.
- replay index exists.

#### Given a custom table name, when New is called, then it creates that table

Expected:

- custom table exists.
- default table is not required.

#### Given invalid table names, when New is called, then it returns an error

Invalid examples:

- `frames;drop`
- `frames-name`
- `frames.name`
- `123frames`

#### Given nil db, when New is called, then it returns an error

#### Given NewPostgres, when the store is created, then it uses Postgres SQL

Expected:

- placeholders use `$1`, `$2`, ...
- table migration uses Postgres-compatible DDL.
- duplicate inserts use `ON CONFLICT (queue_id, stream, seq, direction) DO NOTHING`.
- schema validation uses `information_schema` and the Postgres catalog.

#### Given an unsupported dialect, when New is called, then it returns an error

Expected:

- no fallback to a different dialect.

#### Given an existing table, when New is called, then schema compatibility is validated

Expected:

- compatible table with extra columns passes.
- missing required columns fail.
- nullable required columns fail.
- missing unique key fails.

### Append Outbound

#### Given data appends for one queue and stream, when AppendOutboundData is called repeatedly, then seq increments monotonically

Expected:

- seq 1, 2, 3.
- direction `outbound`.
- kind `data`.
- status `pending`.
- payload and metadata persist.

#### Given tombstone append, when AppendOutboundTombstone is called, then it persists kind tombstone without payload

Expected:

- kind `tombstone`.
- payload empty.
- `error_message` persists.
- status `pending`.

#### Given different queue_id or stream, when outbound frames are appended, then seq lists are independent

Expected:

- each `queue_id + stream + outbound` list starts at 1.

### Inbound Dedup

#### Given a new inbound data frame, when SaveInboundIfAbsent is called, then it inserts received

Expected:

- `inserted=true`.
- status `received`.
- payload and metadata persist.

#### Given duplicate inbound key, when SaveInboundIfAbsent is called again, then it returns inserted=false and original frame

Expected:

- original payload and metadata are preserved.
- duplicate does not overwrite.

### Status Transitions

#### Given outbound frames, when AckOutboundThrough is called, then seq <= throughSeq are acked

Expected:

- cumulative ACK behavior scoped by queue_id, stream, outbound direction.

#### Given a frame, when MarkSent/MarkApplied/MarkRejected is called, then status and error_message update correctly

Expected:

- `MarkSent` clears prior error.
- `MarkApplied` clears prior error.
- `MarkRejected` stores `error_message`.

#### Given a frame, when RecordSendFailure or RecordDispatchFailure is called, then status remains replayable and error_message updates

Expected:

- outbound frame remains `pending` or `sent`.
- inbound frame remains `received`.

#### Given metadata update, when UpdateMetadata is called, then metadata_json is replaced and round trips

Expected:

- old metadata removed.
- new metadata persists.

### Replay Lists

#### Given outbound frames in pending, sent, acked, rejected, when ListOutboundReplay is called, then only pending and sent return in seq order

Expected:

- ordered by seq ascending.
- respects limit.

#### Given inbound frames in received, applied, rejected, when ListInboundReplay is called, then only received returns in seq order

Expected:

- ordered by seq ascending.
- respects limit.

### Errors

#### Given invalid frames or messages, when store methods are called, then they return errors

Expected:

- invalid outbound payload rejected.
- invalid inbound frame rejected.
- missing frame for transition returns error.
- corrupt stored metadata returns error on read.

## Implementation Gate

Before merging implementation:

- All sqlstore tests pass.
- `go test ./reliablemq/sqlstore -coverprofile=sqlstore.coverage.out` passes.
- `go tool cover -func=sqlstore.coverage.out` reports total coverage
  `>= 90.0%`.
