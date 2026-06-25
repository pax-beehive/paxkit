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

#### Given outbound hook continues, when Send is called, then data is durably appended before network send

Expected call order:

```text
BeforeSend -> AppendOutboundData -> Sender.Send(data) -> MarkSent
```

#### Given outbound hook returns tombstone, when Send is called, then a tombstone is durably appended and sent

Expected:

- `AppendOutboundTombstone`
- `Sender.Send(tombstone)`
- no data payload in envelope
- `MarkSent`

#### Given outbound hook returns runtime error, when policy is OutboundHookErrorTombstone, then Send appends and sends a tombstone with err.Error()

Expected:

- `AppendOutboundTombstone(error_message=err.Error())`
- `Sender.Send(tombstone)`
- `MarkSent`

#### Given network send fails after durable append, when Send is called, then send failure is recorded and the frame remains replayable

Expected:

- append called
- sender called and returns error
- `RecordSendFailure`
- `MarkSent` not called
- `Send` returns error

#### Given MarkSent fails after network send succeeds, when Send is called, then Send returns error

Expected:

- append called
- sender called
- `MarkSent` called and returns error
- caller receives error
- at-least-once replay may resend later

### Inbound Receive

#### Given inbound data is new, when Receive is called, then it stores before ACK and dispatches before MarkApplied

Expected call order:

```text
SaveInboundIfAbsent -> Sender.Send(ack) -> BeforeDispatch -> Dispatcher.Dispatch -> MarkApplied
```

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

- `AckOutboundThrough(queue_id, stream, seq)`
- no ACK response sent
- no dispatcher

### Replay

#### Given pending and sent outbound frames, when ReplayOutbound is called, then frames are sent in seq order

Expected:

- store returns ordered frames
- sender receives ordered data/tombstone envelopes
- `MarkSent` called for each successful send

#### Given outbound replay send fails, when ReplayOutbound is called, then failure is recorded and replay stops

Expected:

- `RecordSendFailure`
- returns error
- later frames are not sent in that run

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

## Implementation Gate

Before implementation begins:

- This BDD plan must be reviewed.
- Any changed behavior must be reflected here first.

Before merging implementation:

- All BDD tests pass.
- `go test ./reliablemq/... -coverprofile=coverage.out` passes.
- `go tool cover -func=coverage.out` reports total coverage `>= 90.0%`.
