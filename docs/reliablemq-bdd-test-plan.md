# reliablemq BDD/TDD Test Plan

`reliablemq` must be implemented spec-first. No production behavior should be
added until the matching BDD-style test case is written and failing.

Coverage target: **at least 90% statement coverage** for
`github.com/pax-beehive/paxkit/reliablemq/...`.

Coverage command:

```sh
go test ./reliablemq/... -coverprofile=coverage.out
go tool cover -func=coverage.out
```

The final `total:` line must be `>= 90.0%`.

## Test Style

Use Go's standard `testing` package with explicit BDD naming and structure. Avoid
external test frameworks in v1 unless there is a strong reason to add one.

Test names should be short and name the behavior area:

```go
func TestEngineSendDataOrdering(t *testing.T)
```

Use comments and subtest names for the full BDD detail. Each test should be
structured as:

```go
// Given

// When

// Then
```

Table tests are allowed when they make the behavior clearer. Each row should
still describe a behavior, not only an input/output pair.

## Coverage Scope

Coverage must include:

- envelope validation and marshal/unmarshal
- stream and frame validation
- hook middleware ordering and short-circuiting
- outbound data send state machine
- outbound tombstone state machine
- inbound data receive state machine
- inbound tombstone receive state machine
- ACK handling
- duplicate inbound handling
- reconnect replay
- error paths that should not store or ACK
- store reference behavior using the memory store

Coverage does not need to include:

- paxd adapter code
- pax-manager adapter code
- concrete SQLite/Postgres migrations

Those will have their own package tests when implemented.

## BDD Specs

### Envelope

#### Given a valid data envelope, when it is marshaled and unmarshaled, then all wire fields round trip

Expected:

- `type=data`
- `queue_id`
- `stream`
- `seq`
- `metadata`
- `payload`

#### Given a valid ACK envelope, when it is validated, then payload is not required

Expected:

- validation succeeds without payload
- `seq` must still be positive

#### Given a valid tombstone envelope, when it is marshaled, then `error_message` is preserved

Expected:

- `type=tombstone`
- no payload required
- `error_message` round trips

#### Given an unknown envelope type, when Receive validates it, then it returns an error without storing or ACKing

Expected:

- `Receive` returns error
- store is not called
- sender is not called

#### Given invalid seq, empty queue_id, empty stream, or invalid data payload, when Receive validates it, then it returns an error without storing or ACKing

Expected:

- one subtest per invalid field
- no durable write
- no ACK

### Hook Middleware

#### Given multiple outbound hooks, when all call next, then they run in onion order

Expected order:

```text
h1 before -> h2 before -> terminal -> h2 after -> h1 after
```

#### Given an outbound hook returns tombstone without calling next, when Send runs, then downstream hooks are not called

Expected:

- first hook called
- downstream hook not called
- tombstone decision returned

#### Given an outbound hook mutates metadata before next, when Send appends the frame, then appended metadata contains the mutation

Expected:

- metadata mutation reaches store append

#### Given multiple inbound hooks, when all call next, then they run in onion order

Expected order:

```text
h1 before -> h2 before -> terminal -> h2 after -> h1 after
```

#### Given an inbound hook rejects without calling next, when Receive dispatches a data frame, then downstream hooks and dispatcher are not called

Expected:

- `MarkRejected` called
- dispatcher not called

### Outbound Send

#### Given the journal sink and socket writer are blocked, when Send is called, then acceptance still returns

Expected:

- `Send` returns after validation, local hooks, and MPSC acceptance
- `Send` does not wait for journal flush, socket write, reconnect, or ACK
- success does not promise that the accepted tail survives a process crash

#### Given concurrent callers, when Send accepts their messages, then the owner assigns unique contiguous sequence numbers

Expected:

- no caller blocks on a bounded ingress channel
- one owner goroutine sequences all accepted entries
- resulting sequence numbers are unique and contiguous

#### Given journal flush is delayed and a socket is healthy, when a message is accepted, then network send does not wait for persistence

Expected:

- network cursor may advance ahead of `persistedThrough`
- the frame remains in the hot log until it is persisted or ACKed
- this send-first interval is an explicit crash-loss window

#### Given an outbound hook returns tombstone or a runtime error, when Send accepts the decision, then the owner sequences a tombstone

Expected:

- tombstone consumes the next sequence number
- tombstone has no data payload
- runtime error is stored as the tombstone `error_message`
- sequencing, journal flush, and network write happen after acceptance

#### Given unpersisted bytes or age exceeds its configured limit, when producer maintenance runs, then the producer fails explicitly

Expected:

- `OnError` receives `ErrProducerJournalLimit`
- later `Send` calls return `ErrProducerNotReady`
- no accepted frame is silently discarded to stay under the limit

#### Given the head socket write fails, when the producer reconnects, then later frames cannot bypass the failed head

Expected:

- the failed binding is disabled
- the next generation resumes from the same head
- later data remains ordered behind it

#### Given socket writes succeed, when cumulative ACK advances, then no per-frame sent patch is required

Expected:

- data and ACK envelopes share one connection-owned writer
- network success advances only the in-memory cursor
- cumulative `AckOutboundThrough` is coalesced into journal persistence
- stale `pending` and legacy `sent` rows are equally replayable until ACKed

### Inbound Receive

#### Given inbound data is new, when Receive is called, then it stores before ACK submission and dispatches before MarkApplied

Expected call order:

```text
SaveInboundIfAbsent -> Producer.submitInboundACK -> BeforeDispatch -> Dispatcher.Dispatch -> MarkApplied
```

ACK submission is non-blocking and the producer coalesces it onto the same
connection-owned writer used for data.

#### Given inbound data is duplicate, when Receive is called, then it ACKs but does not run hooks or dispatch

Expected:

- `SaveInboundIfAbsent(inserted=false)`
- ACK sent
- no `BeforeDispatch`
- no dispatcher
- no `MarkApplied`

#### Given inbound hook rejects, when Receive is called, then it ACKs and marks rejected without dispatch

Expected:

- inbound is stored
- ACK sent
- `MarkRejected(error_message)`
- dispatcher not called

#### Given inbound hook returns runtime error, when Receive is called, then it records dispatch failure and keeps the frame received

Expected:

- inbound is stored
- ACK sent
- `RecordDispatchFailure(error_message=err.Error())`
- dispatcher not called
- no `MarkApplied`

#### Given dispatcher returns error, when Receive is called, then it records dispatch failure and keeps the frame received

Expected:

- inbound is stored
- ACK sent
- hook continue
- dispatcher called and returns error
- `RecordDispatchFailure`
- no `MarkApplied`

#### Given inbound tombstone is new, when Receive is called, then it stores, ACKs, and marks applied without dispatch

Expected:

- `SaveInboundIfAbsent`
- ACK sent
- `MarkApplied`
- no hook
- no dispatcher

#### Given inbound tombstone is duplicate, when Receive is called, then it ACKs and does nothing else

Expected:

- `SaveInboundIfAbsent(inserted=false)`
- ACK sent
- no hook
- no dispatcher
- no `MarkApplied`

### ACK Handling

#### Given an ACK envelope, when Receive is called, then outbound frames through seq are ACKed

Expected:

- producer owner advances `ackedThrough`
- journal persistence coalesces `AckOutboundThrough(queue_id, stream, seq)`
- no ACK response sent
- no dispatcher

#### Given cumulative ACKs advance repeatedly, when write-behind flushes, then only the highest ACK is persisted per queue

Expected:

- duplicate and out-of-order ACKs do not create additional journal patches
- one flush contains at most one cumulative ACK per `queue_id + stream`
- the persisted ACK watermark advances monotonically

#### Given old outbound frames are already ACKed, when a newer cumulative ACK arrives, then old rows are not rewritten

Expected:

- only rows in `(old_acked_through, new_acked_through]` transition to ACKed
- old ACKed rows keep their original `updated_at`
- a duplicate or lower ACK performs no frame updates

#### Given a journal with thousands of ACKed rows, when ACK advances by one, then exactly the new row is updated

Expected:

- historical row count does not affect the number of updated frame rows
- restart reloads the durable ACK watermark before accepting another ACK
- upgrading an existing queue-state table backfills the watermark from ACKed history
- an invalid ACK beyond the current outbound tail is capped at the durable tail

#### Given one durable batch contains several ACKed frames or patches, when ApplyBatch commits, then each queue watermark advances once

Expected:

- frame inserts and ACK advancement remain in the same transaction
- each `queue_id + stream` contributes only its maximum `through_seq`
- batch cost is not multiplied by the number of cumulative ACK observations

### Replay

#### Given more frames than one cursor page, when a producer binds after reconnect, then every replayable frame is sent in seq order

Expected:

- producer pages from the exact `nextToSend`
- all pages are drained, not only the first configured batch
- pending and legacy sent rows are treated identically
- no per-frame `MarkSent` patch is written

#### Given live output arrives while backlog replay is blocked, when the writer resumes, then live output cannot overtake backlog

Expected:

- backlog and live output use the same cursor
- sequence order is preserved across the replay/live boundary

#### Given an outbound replay write fails, when the connection generation is disabled, then the cursor remains at the failed head

Expected:

- later frames are not sent on that binding
- a new binding resumes from peer `ackedThrough + 1`
- old-generation cleanup cannot detach the new binding

#### Given received inbound frames, when ReplayInbound is called, then data frames are dispatched in seq order

Expected:

- hooks run for data frames
- dispatcher called in seq order
- `MarkApplied` after successful dispatch

#### Given received inbound tombstone frames, when ReplayInbound is called, then they are marked applied without dispatch

Expected:

- no hook
- no dispatcher
- `MarkApplied`

### Memory Store Reference Behavior

#### Given outbound appends for the same queue and stream, when frames are appended, then seq increments monotonically

Expected:

- seq 1, 2, 3

#### Given different queue_id, stream, or direction, when frames are appended, then seq lists are independent

Expected:

- each independent list starts at seq 1

#### Given an inbound duplicate key, when SaveInboundIfAbsent is called twice, then the second call returns inserted=false and the original frame

Expected:

- no overwrite
- original payload/metadata preserved

#### Given AckOutboundThrough seq N, when frames exist before and after N, then only seq <= N are acked

Expected:

- cumulative ACK behavior

#### Given completed frames, when replay lists are requested, then acked/applied/rejected frames are excluded

Expected:

- outbound replay includes pending/sent only
- inbound replay includes received only

### Retention

#### Given ACKed outbound history exceeds both retention windows, when pruning runs, then only old rows outside the debug tail are deleted

Expected:

- ACK processing itself never deletes journal rows
- rows newer than the time cutoff are retained
- at least the latest configured sequence window per queue is retained
- pending/sent outbound rows and all inbound rows are retained
- one prune call deletes no more than its configured batch limit

## Implementation Gate

Before implementation begins:

- This BDD plan must be reviewed.
- Any changed behavior must be reflected here first.

Before merging implementation:

- All BDD tests pass.
- `go test ./reliablemq/... -coverprofile=coverage.out` passes.
- `go tool cover -func=coverage.out` reports total coverage `>= 90.0%`.
