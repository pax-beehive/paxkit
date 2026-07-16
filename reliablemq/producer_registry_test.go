package reliablemq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProducerRegistryReturnsOneProcessOwnedProducerPerQueue(t *testing.T) {
	// Given
	store := NewProducerWriteBehindStore(
		newProducerWriteBehindSink(),
		WithProducerWriteBehindManualFlush(),
	)
	defer closeProducerWriteBehindStore(t, store)
	registry, err := NewProducerRegistry(store, ProducerConfig{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, registry.Close(context.Background())) })

	const callers = 64
	start := make(chan struct{})
	results := make(chan *Producer, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			producer, getErr := registry.Get(context.Background(), "queue_1", StreamACP)
			results <- producer
			errs <- getErr
		}()
	}

	// When
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	// Then
	for getErr := range errs {
		require.NoError(t, getErr)
	}
	var first *Producer
	for producer := range results {
		if first == nil {
			first = producer
		}
		require.Same(t, first, producer)
	}
}

func TestProducerRegistryRejectsGetAfterClose(t *testing.T) {
	// Given
	store := NewProducerWriteBehindStore(
		newProducerWriteBehindSink(),
		WithProducerWriteBehindManualFlush(),
	)
	defer closeProducerWriteBehindStore(t, store)
	registry, err := NewProducerRegistry(store, ProducerConfig{})
	require.NoError(t, err)
	require.NoError(t, registry.Close(context.Background()))

	// When
	producer, err := registry.Get(context.Background(), "queue_1", StreamACP)

	// Then
	require.ErrorIs(t, err, ErrProducerRegistryClosed)
	require.Nil(t, producer)
}

func TestProducerRegistryCloseDuringBootstrapDoesNotPublishProducer(t *testing.T) {
	// Given
	baseStore := NewProducerWriteBehindStore(
		newProducerWriteBehindSink(),
		WithProducerWriteBehindManualFlush(),
	)
	defer closeProducerWriteBehindStore(t, baseStore)
	store := &blockingProducerBootstrapStore{
		ProducerWriteBehindStore: baseStore,
		started:                  make(chan struct{}),
		release:                  make(chan struct{}),
	}
	registry, err := NewProducerRegistry(store, ProducerConfig{})
	require.NoError(t, err)
	type getResult struct {
		producer *Producer
		err      error
	}
	getDone := make(chan getResult, 1)
	go func() {
		producer, getErr := registry.Get(context.Background(), "queue_1", StreamACP)
		getDone <- getResult{producer: producer, err: getErr}
	}()
	<-store.started

	// When
	closeDone := make(chan error, 1)
	go func() { closeDone <- registry.Close(context.Background()) }()
	require.Eventually(t, func() bool {
		registry.mu.Lock()
		defer registry.mu.Unlock()
		return registry.closed
	}, time.Second, time.Millisecond)
	close(store.release)

	// Then
	result := <-getDone
	require.Nil(t, result.producer)
	require.ErrorIs(t, result.err, ErrProducerRegistryClosed)
	require.NoError(t, <-closeDone)
}

func TestProducerRegistryRemovesFailedBootstrapSoQueueCanRetry(t *testing.T) {
	// Given
	_, err := NewProducerRegistry(nil, ProducerConfig{})
	require.Error(t, err)
	store := NewProducerWriteBehindStore(
		newProducerWriteBehindSink(),
		WithProducerWriteBehindManualFlush(),
	)
	defer closeProducerWriteBehindStore(t, store)
	registry, err := NewProducerRegistry(store, ProducerConfig{})
	require.NoError(t, err)
	_, err = registry.Get(context.Background(), "", StreamACP)
	require.ErrorIs(t, err, ErrInvalidFrame)
	_, err = registry.Get(context.Background(), "queue_1", "")
	require.ErrorIs(t, err, ErrInvalidFrame)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	// When
	producer, err := registry.Get(canceled, "queue_1", StreamACP)

	// Then
	require.Nil(t, producer)
	require.ErrorIs(t, err, context.Canceled)
	producer, err = registry.Get(context.Background(), "queue_1", StreamACP)
	require.NoError(t, err)
	require.NotNil(t, producer)
	require.NoError(t, registry.Close(nil))
	require.NoError(t, registry.Close(context.Background()))
	var nilRegistry *ProducerRegistry
	require.NoError(t, nilRegistry.Close(context.Background()))
	producer, err = nilRegistry.Get(context.Background(), "queue_1", StreamACP)
	require.Nil(t, producer)
	require.ErrorIs(t, err, ErrProducerRegistryClosed)
}

type blockingProducerBootstrapStore struct {
	*ProducerWriteBehindStore
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingProducerBootstrapStore) LoadProducerReconcileCheckpoint(
	ctx context.Context,
	queueID string,
	stream Stream,
) (ProducerReconcileCheckpoint, error) {
	s.once.Do(func() { close(s.started) })
	select {
	case <-ctx.Done():
		return ProducerReconcileCheckpoint{}, ctx.Err()
	case <-s.release:
		return s.ProducerWriteBehindStore.LoadProducerReconcileCheckpoint(ctx, queueID, stream)
	}
}
