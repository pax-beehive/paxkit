# reliablemq Design Notes

`reliablemq` is a shared Pax transport package for durable ordered
at-least-once duplex JSON streams over an unreliable link such as WebSocket.

The package owns protocol semantics, envelopes, state transitions, hooks, and
storage interfaces. It must not depend on paxd internals, pax-manager internals,
any concrete database, ORM, or WebSocket library.

## Goals

- At-least-once delivery. End-to-end exactly-once is explicitly out of scope.
- Ordered delivery per `queue_id + stream`.
- Deduplication by `queue_id + stream + seq + direction`.
- Monotonic `seq` within `queue_id + stream + direction`.
- Outbound payloads must be durably persisted before network send.
- ACK is cumulative through `seq`.
- ACK only means the peer durably recorded the frame. It does not mean business
  processing completed.
- Inbound duplicates are ACKed but are not dispatched twice.
- Reconnect can replay outbound pending/sent frames.
- Restart/reconnect can replay inbound received-but-not-applied frames.
- Hooks can inspect/update metadata and reject work at defined boundaries
  without breaking the reliability state machine.

## Identity Model

The reliable transport identity is:

```text
queue_id + stream + seq + direction
```

`queue_id + stream + direction` defines one local sequence list.

- `queue_id` identifies the durable reliable queue instance.
- `stream` identifies an ordered logical lane inside that queue.
- `seq` is monotonically increasing within one local sequence list.
- `direction` is local-relative journal direction: `outbound` or `inbound`.

`agent_id`, `cloud_agent_id`, `node_id`, `session_id`, and similar business
routing fields are metadata. They are not reliable transport primary keys.

Examples:

```text
queue_id = connection_id
stream   = acp
metadata = agent_id, node_id, session_id
```

For agent-to-agent traffic:

```text
queue_id = a2a:<channel_id>
stream   = acp
metadata = src_agent_id, src_session_id, dst_agent_id, dst_session_id
```

Do not create a new stream just because the source or destination changed.
Create a new stream only when messages should form a separate ordered lane, for
example `acp`, `control`, or `debug`.

The package may provide a small set of default streams for shared Pax protocol
lanes:

```go
type Stream string

const (
    StreamACP     Stream = "acp"
    StreamControl Stream = "control"
)
```

Stream identity is the string value. APIs accept `Stream`, which is a named
string type, so callers can define custom lanes with
`reliablemq.Stream("debug.capture")` or their own constants. The package should
not provide endpoint-directional stream names such as `manager_to_paxd` or
`paxd_to_manager`; direction belongs to local journal state, not stream identity.

## Envelope vs Frame

`Envelope` is the wire format sent across WebSocket or another network link.

```go
type Envelope struct {
    Type     EnvelopeType    `json:"type"` // "data", "ack", or "tombstone"
    QueueID  string          `json:"queue_id"`
    Stream   Stream          `json:"stream"`
    Seq      int64           `json:"seq"`
    Metadata Metadata        `json:"metadata,omitempty"`
    Payload  json.RawMessage `json:"payload,omitempty"`
    ErrorMessage string       `json:"error_message,omitempty"`
}
```

Data envelope:

```json
{
  "type": "data",
  "queue_id": "conn_123",
  "stream": "acp",
  "seq": 12,
  "metadata": {"agent_id": "agent_1"},
  "payload": {"jsonrpc": "2.0", "method": "example"}
}
```

ACK envelope:

```json
{
  "type": "ack",
  "queue_id": "conn_123",
  "stream": "acp",
  "seq": 12
}
```

Tombstone envelope:

```json
{
  "type": "tombstone",
  "queue_id": "conn_123",
  "stream": "acp",
  "seq": 13,
  "metadata": {
    "agent_id": "agent_1"
  },
  "error_message": "outbound payload rejected by policy"
}
```

Tombstones occupy a sequence number and participate in replay and cumulative ACK.
They are durably recorded by both sides, ACKed like data frames, and never
dispatched to business logic. They exist to make the stream log explicit and
debuggable when an outbound payload is intentionally not sent as data.
`error_message` is usually `err.Error()`. If a caller is concerned about leaking
sensitive details to the peer, the hook should return a sanitized error.

`Frame` is the local durable journal record. It includes local state that is not
part of the wire contract, such as `direction`, `status`, and error details.

```go
type FrameKind string

const (
    FrameKindData      FrameKind = "data"
    FrameKindTombstone FrameKind = "tombstone"
)

type FrameKey struct {
    QueueID   string
    Stream    Stream
    Seq       int64
    Direction Direction
}

type Frame struct {
    Key      FrameKey
    Kind     FrameKind
    Payload  json.RawMessage
    Metadata Metadata
    Status   Status
    ErrorMessage string
}
```

## Store Interface

`DurableStore` is implemented by the host service using its own database and
transaction model.

The core package does not bind to a database. Reusable adapters can live beside
it, for example:

- `reliablemq/memory`: in-memory test adapter, not durable.
- `reliablemq/sqlstore`: `database/sql` adapter with SQLite and Postgres
  constructors plus schema validation.

```go
type DurableStore interface {
    AppendOutboundData(ctx context.Context, queueID string, stream Stream, payload json.RawMessage, metadata Metadata) (Frame, error)
    AppendOutboundTombstone(ctx context.Context, queueID string, stream Stream, errorMessage string, metadata Metadata) (Frame, error)
    SaveInboundIfAbsent(ctx context.Context, frame Frame) (inserted bool, stored Frame, err error)

    ListOutboundReplay(ctx context.Context, queueID string, stream Stream, limit int) ([]Frame, error)
    ListInboundReplay(ctx context.Context, queueID string, stream Stream, limit int) ([]Frame, error)

    MarkSent(ctx context.Context, key FrameKey) error
    AckOutboundThrough(ctx context.Context, queueID string, stream Stream, throughSeq int64) error
    MarkApplied(ctx context.Context, key FrameKey) error
    MarkRejected(ctx context.Context, key FrameKey, errorMessage string) error
    RecordSendFailure(ctx context.Context, key FrameKey, errorMessage string) error
    RecordDispatchFailure(ctx context.Context, key FrameKey, errorMessage string) error
    UpdateMetadata(ctx context.Context, key FrameKey, metadata Metadata) error
}
```

### AppendOutboundData

Called after outbound hooks decide that the local payload should enter the
reliable stream as data.

The store must atomically:

1. Allocate the next outbound `seq` for `queue_id + stream`.
2. Insert the frame durably with `direction=outbound` and `status=pending`.
3. Return the complete frame.

This method is the core persist-before-send guarantee.

### AppendOutboundTombstone

Called after outbound hooks decide that the next outbound sequence should exist
as a tombstone instead of data.

The store must atomically:

1. Allocate the next outbound `seq` for `queue_id + stream`.
2. Insert a durable tombstone frame with `direction=outbound`,
   `status=pending`, `error_message`, and metadata.
3. Return the complete frame.

Tombstones are sent over the wire, replayed, and ACKed like data frames, but
they are never dispatched to business logic.

### SaveInboundIfAbsent

Called when a peer data envelope is received.

The store deduplicates by `queue_id + stream + seq + inbound`.

- If new, insert with `status=received` and return `inserted=true`.
- If duplicate, return the existing frame with `inserted=false`.

The caller may ACK after this method succeeds. Only `inserted=true` frames may be
dispatched.

### ListOutboundReplay

Lists outbound frames that should be resent after reconnect, ordered by `seq`
ascending.

Typical statuses: `pending` and `sent`.

### ListInboundReplay

Lists inbound frames that were durably recorded but not applied, ordered by
`seq` ascending.

Typical status: `received`.

### MarkSent

Called after a network write succeeds. This changes an outbound frame to `sent`.

`sent` does not mean the peer received or persisted the frame.

### AckOutboundThrough

Called when an ACK envelope is received. This marks outbound frames with
`seq <= throughSeq` as `acked`.

### MarkApplied

Called after the local dispatcher accepts an inbound frame. This changes the
frame to `applied`.

`applied` means reliablemq handed the payload to the local business boundary. It
does not mean the business operation completed.

### MarkRejected

Called when an inbound dispatch hook makes a terminal decision not to dispatch a
durably recorded frame. Rejected inbound frames are not replayed and do not block
later sequence numbers.

### RecordSendFailure

Called when a network send attempt fails. This records error details and may
leave the frame in its existing replayable status. This is transport recovery,
not application retry: a frame already accepted into the reliable stream remains
eligible for reconnect replay until ACKed.

### RecordDispatchFailure

Called when an inbound hook or dispatcher has a runtime delivery error. This
records `error_message` and leaves the frame `received` so `ReplayInbound` can
try delivery again later.

### UpdateMetadata

Called after hooks mutate metadata and before send/dispatch continues. This
keeps hook-injected trace/auth/context metadata durable for replay.

## Runtime Interfaces

`Sender` is the network boundary. It sends wire envelopes.

```go
type Sender interface {
    Send(ctx context.Context, env Envelope) error
}
```

`Dispatcher` is the local business boundary. It receives durable frames.

```go
type Dispatcher interface {
    Dispatch(ctx context.Context, frame Frame) error
}
```

The split is intentional:

- `Envelope` is wire-level and includes both data and ACK messages.
- `Frame` is local durable state and is only dispatched for non-duplicate data.

## Hooks

```go
type OutboundAction string

const (
    OutboundContinue  OutboundAction = "continue"
    OutboundTombstone OutboundAction = "tombstone"
)

type OutboundMessage struct {
    QueueID  string
    Stream   Stream
    Payload  json.RawMessage
    Metadata Metadata
}

type OutboundDecision struct {
    Action       OutboundAction
    ErrorMessage string
    Metadata     Metadata
}

type InboundAction string

const (
    InboundContinue InboundAction = "continue"
    InboundReject   InboundAction = "reject"
)

type InboundDecision struct {
    Action       InboundAction
    ErrorMessage string
    Metadata     Metadata
}

type OutboundHandler func(context.Context, *OutboundMessage) (OutboundDecision, error)
type InboundHandler func(context.Context, *Frame) (InboundDecision, error)

type OutboundMiddleware func(OutboundHandler) OutboundHandler
type InboundMiddleware func(InboundHandler) InboundHandler

func WithOutboundMiddleware(middlewares ...OutboundMiddleware) Option
func WithInboundMiddleware(middlewares ...InboundMiddleware) Option

type OutboundHookErrorPolicy string

const (
    OutboundHookErrorTombstone OutboundHookErrorPolicy = "tombstone"
)

type Config struct {
    OutboundHookErrorPolicy OutboundHookErrorPolicy
}
```

Hook boundaries:

- `BeforeSend`: outbound payload is ready to enter the reliable stream, but no
  sequence number has been allocated yet. The hook returns a decision: continue
  as data, append/send a tombstone, or fail without entering the stream.
- `BeforeDispatch`: inbound payload has already been durably recorded, but has
  not been handed to local business logic.

Hooks use an onion-style middleware model. Outbound and inbound middleware are
separate types. Middleware receives the next handler and returns a wrapped
handler, so it can run logic before and/or after downstream middleware. A
middleware may short-circuit by returning a decision without calling `next`.

Hooks may read and update metadata. v1 hooks should treat payload as read-only.

Decision and error have different meanings:

- Decision is the policy/business outcome: continue, tombstone, or reject.
- Error means the hook itself could not run correctly, for example because a
  dependency is unavailable.

The first implementation uses `OutboundHookErrorTombstone`: an outbound hook
runtime error is converted into a durable tombstone frame whose `error_message`
is `err.Error()`. This keeps the peer-visible sequence log explicit. The config
keeps room for future policies, but the first version does not implement local
preflight retry.

An outbound tombstone decision means a tombstone frame is durably appended,
sent, replayed, and ACKed.

An inbound hook runtime error is a delivery failure, not a rejection. It records
`error_message` and leaves the frame `received` so `ReplayInbound` can try again.
An inbound reject decision marks the frame rejected and does not dispatch it.

Inbound duplicates do not run `BeforeDispatch`.

## Engine

`Engine` is the state machine coordinator used by paxd, pax-manager, or any
other host process.

```go
type Engine struct {
    Store      DurableStore
    Sender     Sender
    Dispatcher Dispatcher

    // internal:
    outboundHandler OutboundHandler
    inboundHandler  InboundHandler
}
```

Public methods:

```go
func NewEngine(config Config, store DurableStore, sender Sender, dispatcher Dispatcher, opts ...Option) *Engine
func (e *Engine) Send(ctx context.Context, msg OutboundMessage) (Frame, error)
func (e *Engine) Receive(ctx context.Context, env Envelope) error
func (e *Engine) ReplayOutbound(ctx context.Context, queueID string, stream Stream, limit int) error
func (e *Engine) ReplayInbound(ctx context.Context, queueID string, stream Stream, limit int) error
```

### Send

Used when local code produces an outbound payload.

Flow:

1. `BeforeSend`
2. If decision is data: `AppendOutboundData`
3. If decision is tombstone: `AppendOutboundTombstone`
4. `Sender.Send(data or tombstone envelope)`
5. `MarkSent`

If `BeforeSend` returns a runtime error, the first implementation follows
`OutboundHookErrorTombstone`: append and send a tombstone with
`error_message=err.Error()`. If the hook decides tombstone, the engine also
appends and sends a tombstone frame. Tombstones consume the next seq, preserve
the shared stream log, and give both sides a durable debug record for the skipped
payload.

### Receive

Used by the WebSocket read loop after decoding an envelope.

ACK flow:

1. Validate ACK envelope.
2. `AckOutboundThrough`.

Invalid envelope type, invalid seq, or invalid payload is a protocol error:
`Receive` returns an error. The frame is not stored and no ACK is sent. Callers
should log the error and decide whether to close the connection.

Data flow:

1. Validate data envelope.
2. `SaveInboundIfAbsent`.
3. Send ACK after durable record succeeds.
4. If duplicate, stop.
5. `BeforeDispatch`.
6. If decision is reject: `UpdateMetadata`, `MarkRejected`, stop.
7. If decision is continue: `UpdateMetadata`.
8. `Dispatcher.Dispatch`.
9. `MarkApplied`.

If `BeforeDispatch` returns a runtime error, it is treated as a delivery failure:
`RecordDispatchFailure(error_message=err.Error())`, keep the frame `received`,
and let `ReplayInbound` try again later.

If `Dispatcher.Dispatch` returns an error, it is also treated as a delivery
failure: `RecordDispatchFailure(error_message=err.Error())`, keep the frame
`received`, and let `ReplayInbound` try again later. This is local delivery
recovery, not business-level retry.

Tombstone flow:

1. Validate tombstone envelope.
2. `SaveInboundIfAbsent`.
3. Send ACK after durable record succeeds.
4. Mark `applied` locally without dispatching to business logic.

### ReplayOutbound

Used after reconnect. Lists outbound replay frames and resends them in `seq`
order.

### ReplayInbound

Used after restart/reconnect. Lists received-but-not-applied inbound frames and
dispatches them in `seq` order.

## Typical paxd Usage

```go
engine := reliablemq.NewEngine(storeAdapter, wsSender, acpStdinDispatcher, hooks...)

// WebSocket read loop.
env, err := reliablemq.UnmarshalEnvelope(raw)
if err == nil {
    err = engine.Receive(ctx, env)
}

// ACP stdout loop.
_, err = engine.Send(ctx, connectionID, reliablemq.StreamACP, payload, metadata)

// Reconnect.
_ = engine.ReplayOutbound(ctx, connectionID, reliablemq.StreamACP, 1000)
_ = engine.ReplayInbound(ctx, connectionID, reliablemq.StreamACP, 1000)
```

## Typical pax-manager Usage

```go
engine := reliablemq.NewEngine(storeAdapter, wsSender, localPipelineDispatcher, hooks...)

// paxd WebSocket read loop.
env, err := reliablemq.UnmarshalEnvelope(raw)
if err == nil {
    err = engine.Receive(ctx, env)
}

// user/API/manager-produced payload.
_, err = engine.Send(ctx, connectionID, reliablemq.StreamACP, payload, metadata)
```

## Open Design Questions

- Should metadata mutation be the only mutation hooks can perform? Current v1
  proposal says hooks may mutate metadata and should treat payload as read-only.
