// Package reliablemq provides a durable ordered at-least-once duplex stream
// abstraction for JSON payloads sent over an unreliable transport such as
// WebSocket.
//
// The package owns protocol semantics, envelopes, state transitions, hooks, and
// storage interfaces. It does not depend on any concrete database, ORM,
// WebSocket library, or paxd/pax-manager internal package.
//
// Reliability identity is queue_id + stream + seq + direction. queue_id is the
// durable queue identity shared by peers. Business routing fields such as
// agent_id or cloud_agent_id belong in frame metadata and are not part of the
// reliable transport key.
//
// ACKs are cumulative through seq and mean only that the peer has durably
// recorded the frame. They do not mean business processing has completed.
package reliablemq
