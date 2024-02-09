// Copyright (c) 2021 - ClariLab GmbH & Co. KG.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Clarilab/clarimq"
	"github.com/Clarilab/tracygo/v2"
	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/codec/json"
)

const (
	// InfiniteRetries is the maximum number for recovery or event delivery retries.
	InfiniteRetries int64 = math.MaxInt64

	handlerType      string = "eventbus"
	aggregateTypeKey string = "aggregate_type"
	eventTypeKey     string = "event_type"

	errChBuffSize int = 100
)

// EventBus is a local event bus that delegates handling of published events
// to all matching registered handlers, in order of registration.
type EventBus struct {
	appID              string
	exchangeName       string
	topic              string
	addr               string
	clientID           string
	registered         map[eh.EventHandlerType]*clarimq.Consumer
	registeredMu       sync.RWMutex
	errCh              chan error
	ctx                context.Context //nolint:containedctx // intended use
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	eventCodec         eh.EventCodec
	publishConn        *clarimq.Connection
	publisher          *clarimq.Publisher
	consumeConn        *clarimq.Connection
	consumerMu         sync.RWMutex
	useRetry           bool
	maxRetries         int64
	maxRecoveryRetries int64
	queueDelays        []time.Duration
	logger             *logger
	loggers            []clarimq.Logger
	publishingCache    clarimq.PublishingCache
	tracer             *tracygo.TracyGo
}

// NewEventBus creates an EventBus, with optional settings.
func NewEventBus(addr, appID, clientID, exchange, topic string, options ...Option) (*EventBus, error) {
	const errMessage = "failed to create event bus: %w"

	ctx, cancel := context.WithCancel(context.Background())

	bus := &EventBus{
		appID:              appID,
		exchangeName:       exchange,
		addr:               addr,
		topic:              topic,
		clientID:           clientID,
		registered:         make(map[eh.EventHandlerType]*clarimq.Consumer),
		errCh:              make(chan error, errChBuffSize),
		ctx:                ctx,
		cancel:             cancel,
		eventCodec:         &json.EventCodec{},
		maxRetries:         InfiniteRetries,
		maxRecoveryRetries: InfiniteRetries,
		tracer:             tracygo.New(),
	}

	// Apply configuration options.
	for i := range options {
		if options[i] == nil {
			continue
		}

		options[i](bus)
	}

	bus.logger = newLogger(bus.loggers)

	if err := bus.setupConnections(); err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	return bus, nil
}

// HandlerType implements the HandlerType method of the eventhorizon.EventHandler interface.
func (*EventBus) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(handlerType)
}

// HandleEvent implements the HandleEvent method of the eventhorizon.EventHandler interface.
func (b *EventBus) HandleEvent(ctx context.Context, event eh.Event) error {
	return b.PublishEvent(ctx, event)
}

// PublishEvent publishes an event. Same as HandleEvent, but with better naming.
func (b *EventBus) PublishEvent(ctx context.Context, event eh.Event) error {
	const errMessage = "failed to publish event: %w"

	data, err := b.eventCodec.MarshalEvent(ctx, event)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if err = b.publisher.PublishWithOptions(
		ctx,
		[]string{fmt.Sprintf("%s.%s", b.topic, event.EventType().String())},
		data,
		clarimq.WithPublishOptionContentType("application/json"),
		clarimq.WithPublishOptionMandatory(true),
		clarimq.WithPublishOptionDeliveryMode(clarimq.PersistentDelivery),
		clarimq.WithPublishOptionExchange(b.exchangeName),
		clarimq.WithPublishOptionMessageID(uuid.NewString()),
		clarimq.WithPublishOptionTracing(b.tracer.CorrelationIDromContext(ctx)),
		clarimq.WithPublishOptionHeaders(
			map[string]any{
				aggregateTypeKey: event.AggregateType().String(),
				eventTypeKey:     event.EventType().String(),
			},
		),
	); err != nil {
		if errors.Is(err, clarimq.ErrPublishFailedChannelClosedCached) {
			b.errCh <- fmt.Errorf(errMessage, err)

			return nil
		}

		return fmt.Errorf(errMessage, err)
	}

	return nil
}

// AddHandler implements the AddHandler method of the eventhorizon.EventBus interface.
func (b *EventBus) AddHandler(ctx context.Context, matcher eh.EventMatcher, handler eh.EventHandler) error {
	const errMessage = "failed to add handler: %w"

	if matcher == nil {
		return eh.ErrMissingMatcher
	}

	if handler == nil {
		return eh.ErrMissingHandler
	}

	handlerType := handler.HandlerType()

	// Check handler existence.
	b.registeredMu.Lock()
	defer b.registeredMu.Unlock()

	if _, ok := b.registered[handlerType]; ok {
		return eh.ErrHandlerAlreadyAdded
	}

	consumer, err := b.declareConsumer(ctx, matcher, handler)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	// Handle until context is cancelled.
	b.wg.Add(1)
	go b.handleCancel(handlerType)

	// Register handler.
	b.registered[handlerType] = consumer

	return nil
}

// RemoveHandler removes a handler from the event bus by type.
func (b *EventBus) RemoveHandler(handlerType eh.EventHandlerType) error {
	const errMessage = "failed to remove handler: %w"

	// Check handler existence.
	b.registeredMu.RLock()
	if _, ok := b.registered[handlerType]; !ok {
		b.registeredMu.RUnlock()

		return fmt.Errorf(errMessage, ErrHandlerNotRegistered)
	}

	b.registeredMu.RUnlock()

	b.registeredMu.Lock()
	defer b.registeredMu.Unlock()

	if err := b.registered[handlerType].Close(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	// Unregister handler.
	delete(b.registered, handlerType)

	return nil
}

// RegisteredHandlers returns a slice of all registered handler types.
func (b *EventBus) RegisteredHandlers() []eh.EventHandlerType {
	b.registeredMu.RLock()
	defer b.registeredMu.RUnlock()

	handlerTypes := make([]eh.EventHandlerType, 0, len(b.registered))

	for handlerType := range b.registered {
		handlerTypes = append(handlerTypes, handlerType)
	}

	return handlerTypes
}

// Close implements the Close method of the eventhorizon.EventBus interface.
func (b *EventBus) Close() error {
	const errMessage = "failed to close event bus: %w"

	// Stop handling.
	b.cancel()
	b.wg.Wait()

	if err := b.publishConn.Close(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if err := b.consumeConn.Close(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	b.logger.logInfo("eventbus gracefully closed")

	return nil
}

// Errors implements the Errors method of the eventhorizon.EventBus interface.
func (b *EventBus) Errors() <-chan error {
	return b.errCh
}
