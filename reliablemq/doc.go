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
//
// Send accepts an owned copy into a process-owned producer. A successful Send
// does not mean that the frame has been flushed to the durable journal, written
// to the network, or ACKed. The network cursor may send ahead of the journal
// flusher; a process crash can therefore lose an accepted but unflushed tail.
package reliablemq
