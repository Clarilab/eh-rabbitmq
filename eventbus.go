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
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/Clarilab/clarimq"
	ehtracygo "github.com/Clarilab/eh-tracygo"
	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/codec/json"
	"github.com/looplab/eventhorizon/namespace"
)

const (
	// InfiniteRetries is the value to retry without discarding an event.
	InfiniteRetries int64 = math.MaxInt64

	aggregateTypeKey string = "aggregate_type"
	eventTypeKey     string = "event_type"

	errChBuffSize int = 100
)

// EventBus is a local event bus that delegates handling of published events
// to all matching registered handlers, in order of registration.
type EventBus struct {
	appID        string
	exchangeName string
	topic        string
	addr         string
	clientID     string
	registered   map[eh.EventHandlerType]struct{}
	registeredMu sync.RWMutex
	errCh        chan error
	ctx          context.Context //nolint:containedctx // intended use
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	eventCodec   eh.EventCodec
	publishConn  *clarimq.Connection
	publisher    *clarimq.Publisher
	consumeConn  *clarimq.Connection
	consumerMu   sync.RWMutex
	useRetry     bool
	maxRetries   int64
	queueDelays  []time.Duration
	logger       *logger
	loggers      []*slog.Logger
}

// NewEventBus creates an EventBus, with optional settings.
func NewEventBus(addr, appID, clientID, exchange, topic string, options ...Option) (*EventBus, error) {
	const errMessage = "failed to create event bus: %w"

	ctx, cancel := context.WithCancel(context.Background())

	bus := &EventBus{
		appID:        appID,
		exchangeName: exchange,
		addr:         addr,
		topic:        topic,
		clientID:     clientID,
		registered:   map[eh.EventHandlerType]struct{}{},
		errCh:        make(chan error, errChBuffSize),
		ctx:          ctx,
		cancel:       cancel,
		eventCodec:   &json.EventCodec{},
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
	return "eventbus"
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
		clarimq.WithPublishOptionTracing(ehtracygo.FromContext(ctx)),
		clarimq.WithPublishOptionHeaders(
			map[string]any{
				aggregateTypeKey: event.AggregateType().String(),
				eventTypeKey:     event.EventType().String(),
			},
		),
	); err != nil {
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

	// Check handler existence.
	b.registeredMu.Lock()
	defer b.registeredMu.Unlock()

	if _, ok := b.registered[handler.HandlerType()]; ok {
		return eh.ErrHandlerAlreadyAdded
	}

	// Register handler.
	b.registered[handler.HandlerType()] = struct{}{}

	consumer, err := b.declareConsumer(ctx, matcher, handler)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	// Handle until context is cancelled.
	b.wg.Add(1)
	go b.handle(consumer)

	return nil
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

// Handles all events coming in on the channel.
func (b *EventBus) handle(
	consumer *clarimq.Consumer,
) {
	defer b.wg.Done()

	<-b.ctx.Done()

	b.consumerMu.Lock()
	consumer.Close()
	b.consumerMu.Unlock()
}

func (b *EventBus) handler(
	ctx context.Context,
	matcher eh.EventMatcher,
	handler eh.EventHandler,
) func(d *clarimq.Delivery) clarimq.Action {
	return func(msg *clarimq.Delivery) clarimq.Action {
		event, ctx, err := b.eventCodec.UnmarshalEvent(ctx, msg.Body)
		if err != nil {
			b.sendErrToErrChannel(ctx, err, handler, event)

			return clarimq.NackDiscard
		}

		// Ignore non-matching events.
		if !matcher.Match(event) {
			return clarimq.Ack
		}

		// Handle the event if it did match.
		if err := handler.HandleEvent(
			ehtracygo.NewContext(ctx, msg.CorrelationId),
			event,
		); err != nil {
			b.sendErrToErrChannel(ctx, err, handler, event)

			return clarimq.NackDiscard
		}

		return clarimq.Ack
	}
}

func (b *EventBus) returnHandler(rtn clarimq.Return) {
	event, ctx, err := b.eventCodec.UnmarshalEvent(b.ctx, rtn.Body)
	if err != nil {
		b.logger.logDebug("return handler: failed to unmarshal event", "error", err)

		return
	}

	b.logger.logError("return handler: event could not be published",
		"tenant", namespace.FromContext(ctx),
		"eventType", event.EventType(),
		"exchange", rtn.Exchange,
		"routingKey", rtn.RoutingKey,
		"messageId", rtn.MessageId,
		"correlationId", rtn.CorrelationId,
	)
}

func (b *EventBus) sendErrToErrChannel(ctx context.Context, err error, h eh.EventHandler, event eh.Event) {
	err = fmt.Errorf("could not handle event (%s): %w", h.HandlerType(), err)
	select {
	case b.errCh <- &eh.EventBusError{Err: err, Ctx: ctx, Event: event}:
	default:
		b.logger.logError("eventhorizon: missed error in RabbitMQ event bus", "error", err)
	}
}

func createFilter(topic string, m eh.EventMatcher) []string {
	switch m := m.(type) {
	case eh.MatchEvents:
		s := make([]string, len(m))
		for i, et := range m {
			s[i] = fmt.Sprintf(`%s.%s`, topic, et) // Filter event types by key to save space.
		}

		return s
	default:
		return []string{fmt.Sprintf("%s.*", topic)}
	}
}

func (b *EventBus) setupConnections() error {
	const errMessage = "failed to setup eventbus connections: %w"

	var err error

	if b.publishConn, err = setupConnection(b.addr, b.logger,
		clarimq.WithConnectionOptionConnectionName(fmt.Sprintf("%s_publish_connection", b.appID)),
		clarimq.WithConnectionOptionReturnHandler(b.returnHandler),
		clarimq.WithConnectionOptionMultipleLoggers(b.loggers),
	); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if b.consumeConn, err = setupConnection(b.addr, b.logger,
		clarimq.WithConnectionOptionConnectionName(fmt.Sprintf("%s_consume_connection", b.appID)),
		clarimq.WithConnectionOptionMultipleLoggers(b.loggers),
	); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if b.publisher, err = clarimq.NewPublisher(b.publishConn); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func setupConnection(uri string, logger *logger, options ...clarimq.ConnectionOption) (*clarimq.Connection, error) {
	const errMessage = "failed to setup connection: %w"

	conn, err := clarimq.NewConnection(uri, options...)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	go func() {
		for err := range conn.NotifyAutoRecoveryFail() {
			if err == nil {
				return
			}

			logger.logWarn("recovery failed", "error", err)
		}
	}()

	return conn, nil
}

func (b *EventBus) declareConsumer(ctx context.Context, matcher eh.EventMatcher, handler eh.EventHandler) (*clarimq.Consumer, error) {
	const errMessage = "failed to declare consumer: %w"

	queueName := fmt.Sprintf("%s_%s", b.appID, handler.HandlerType())

	optionFuncs := []clarimq.ConsumeOption{
		clarimq.WithConsumerOptionConsumerName(fmt.Sprintf("%s_%s", queueName, b.clientID)),
		clarimq.WithExchangeOptionName(b.exchangeName),
		clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
		clarimq.WithExchangeOptionDeclare(true),
		clarimq.WithExchangeOptionDurable(true),
		clarimq.WithQueueOptionDurable(true),
	}

	if b.useRetry {
		optionFuncs = append(optionFuncs, clarimq.WithConsumerOptionDeadLetterRetry(&clarimq.RetryOptions{
			RetryConn:  b.publishConn,
			Delays:     b.queueDelays,
			MaxRetries: b.maxRetries,
		}))
	}

	for _, routingKey := range createFilter(b.topic, matcher) {
		optionFuncs = append(optionFuncs, clarimq.WithConsumerOptionRoutingKey(routingKey))
	}

	consumer, err := clarimq.NewConsumer(
		b.consumeConn,
		queueName,
		b.handler(ctx, matcher, handler),
		optionFuncs...,
	)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	return consumer, nil
}
