package reliablemq

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrProducerRegistryClosed = errors.New("reliablemq: producer registry is closed")

type ProducerRegistry struct {
	mu      sync.Mutex
	store   DurableStore
	config  ProducerConfig
	entries map[producerRegistryKey]*producerRegistryEntry
	closed  bool
}

type producerRegistryKey struct {
	queueID string
	stream  Stream
}

type producerRegistryEntry struct {
	ready    chan struct{}
	producer *Producer
	err      error
}

func NewProducerRegistry(store DurableStore, config ProducerConfig) (*ProducerRegistry, error) {
	if store == nil {
		return nil, fmt.Errorf("reliablemq: producer registry store is required")
	}
	config.QueueID = ""
	config.Stream = ""
	return &ProducerRegistry{
		store:   store,
		config:  config,
		entries: make(map[producerRegistryKey]*producerRegistryEntry),
	}, nil
}

func (r *ProducerRegistry) Get(ctx context.Context, queueID string, stream Stream) (*Producer, error) {
	if r == nil {
		return nil, ErrProducerRegistryClosed
	}
	if queueID == "" {
		return nil, fmt.Errorf("%w: queue_id is required", ErrInvalidFrame)
	}
	if stream == "" {
		return nil, fmt.Errorf("%w: stream is required", ErrInvalidFrame)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	key := producerRegistryKey{queueID: queueID, stream: stream}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrProducerRegistryClosed
	}
	if entry := r.entries[key]; entry != nil {
		r.mu.Unlock()
		return waitProducerRegistryEntry(ctx, entry)
	}
	entry := &producerRegistryEntry{ready: make(chan struct{})}
	r.entries[key] = entry
	config := r.config
	config.QueueID = queueID
	config.Stream = stream
	r.mu.Unlock()

	producer, err := NewProducer(ctx, config, r.store)
	r.mu.Lock()
	closed := r.closed
	if err != nil || closed {
		delete(r.entries, key)
		if err == nil {
			err = ErrProducerRegistryClosed
		}
		entry.err = err
		close(entry.ready)
		r.mu.Unlock()
		if producer != nil {
			_ = producer.Close(context.Background())
		}
		return nil, err
	}
	entry.producer = producer
	close(entry.ready)
	r.mu.Unlock()
	return producer, nil
}

func (r *ProducerRegistry) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	entries := make([]*producerRegistryEntry, 0, len(r.entries))
	for _, entry := range r.entries {
		entries = append(entries, entry)
	}
	r.mu.Unlock()

	var closeErr error
	for _, entry := range entries {
		producer, err := waitProducerRegistryEntry(ctx, entry)
		if err != nil {
			if !errors.Is(err, ErrProducerRegistryClosed) {
				closeErr = errors.Join(closeErr, err)
			}
			continue
		}
		closeErr = errors.Join(closeErr, producer.Close(ctx))
	}
	return closeErr
}

func waitProducerRegistryEntry(ctx context.Context, entry *producerRegistryEntry) (*Producer, error) {
	select {
	case <-entry.ready:
		return entry.producer, entry.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
