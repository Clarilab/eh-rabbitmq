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
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/codec/json"
	"github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq"
	"log"
	"sync"
	"time"
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
	cctx         context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	codec        eh.EventCodec
	publisher    *rabbitmq.Publisher
	dlxttl       time.Duration
	dlx          bool
}

// NewEventBus creates an EventBus, with optional settings.
func NewEventBus(addr, appID, clientID, exchange, topic string, options ...Option) (*EventBus, error) {
	ctx, cancel := context.WithCancel(context.Background())

	b := &EventBus{
		appID:        appID,
		exchangeName: exchange,
		addr:         addr,
		topic:        topic,
		clientID:     clientID,
		registered:   map[eh.EventHandlerType]struct{}{},
		errCh:        make(chan error, 100),
		cctx:         ctx,
		cancel:       cancel,
		codec:        &json.EventCodec{},
	}

	// Apply configuration options.
	for _, option := range options {
		if option == nil {
			continue
		}

		if err := option(b); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	publisher, err := rabbitmq.NewPublisher(
		addr,
		amqp091.Config{},
	)
	if err != nil {
		return nil, fmt.Errorf("error while creating publisher: %w", err)
	}

	b.publisher = publisher

	return b, nil
}

// Option is an option setter used to configure creation.
type Option func(*EventBus) error

// WithCodec uses the specified codec for encoding events.
func WithCodec(codec eh.EventCodec) Option {
	return func(b *EventBus) error {
		b.codec = codec

		return nil
	}
}

// WithDLX enables/disables a dead letter exchange for events that ran into an error.
func WithDLX(dlx bool) Option {
	return func(b *EventBus) error {
		b.dlx = dlx
		b.dlxttl = time.Minute

		return nil
	}
}

// WithDLXTTL sets the TTL for dead letter events.
func WithDLXTTL(ttl time.Duration) Option {
	return func(b *EventBus) error {
		b.dlxttl = ttl

		return nil
	}
}

// HandlerType implements the HandlerType method of the eventhorizon.EventHandler interface.
func (b *EventBus) HandlerType() eh.EventHandlerType {
	return "eventbus"
}

const (
	aggregateTypeKey = "aggregate_type"
	eventTypeKey     = "event_type"
)

// HandleEvent implements the HandleEvent method of the eventhorizon.EventHandler interface.
func (b *EventBus) HandleEvent(ctx context.Context, event eh.Event) error {
	data, err := b.codec.MarshalEvent(ctx, event)
	if err != nil {
		return fmt.Errorf("could not marshal event: %w", err)
	}

	headers := map[string]interface{}{
		aggregateTypeKey: event.AggregateType().String(),
		eventTypeKey:     event.EventType().String(),
	}

	err = b.publisher.Publish(
		data,
		[]string{b.topic + "." + event.EventType().String()},
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsMandatory,
		rabbitmq.WithPublishOptionsPersistentDelivery,
		rabbitmq.WithPublishOptionsExchange(b.exchangeName),
		rabbitmq.WithPublishOptionsHeaders(headers),
	)
	if err != nil {
		return fmt.Errorf("could not publish event: %w", err)
	}

	return nil
}

// AddHandler implements the AddHandler method of the eventhorizon.EventBus interface.
func (b *EventBus) AddHandler(ctx context.Context, m eh.EventMatcher, h eh.EventHandler) error {
	if m == nil {
		return eh.ErrMissingMatcher
	}

	if h == nil {
		return eh.ErrMissingHandler
	}

	// Check handler existence.
	b.registeredMu.Lock()
	defer b.registeredMu.Unlock()

	if _, ok := b.registered[h.HandlerType()]; ok {
		return eh.ErrHandlerAlreadyAdded
	}

	// Get or create the subscription.
	groupName := fmt.Sprintf("%s_%s", b.appID, h.HandlerType())

	consumer, err := rabbitmq.NewConsumer(b.addr, amqp091.Config{})
	if err != nil {
		return fmt.Errorf("could not declare consumer: %w", err)
	}

	// Register handler.
	b.registered[h.HandlerType()] = struct{}{}

	handler := b.handler(ctx, m, h)

	consumerName := groupName + "_" + b.clientID

	optionFuncs := []func(*rabbitmq.ConsumeOptions){
		rabbitmq.WithConsumeOptionsQueueDurable,
		rabbitmq.WithConsumeOptionsBindingExchangeName(b.exchangeName),
		rabbitmq.WithConsumeOptionsBindingExchangeKind("topic"),
		rabbitmq.WithConsumeOptionsBindingExchangeDurable,
		rabbitmq.WithConsumeOptionsConsumerName(consumerName),
	}

	filters := createFilter(b.topic, m)

	if b.dlx {
		deadLetterExchangeName := "dlx_" + b.exchangeName
		deadLetterQueueName := "dlx_" + groupName
		requeueRoutingKey := groupName + "_requeue"

		// add routing key for re-queued messages
		filters = append(filters, requeueRoutingKey)

		// set dead letter exchange
		optionFuncs = append(optionFuncs, rabbitmq.WithConsumeOptionsQueueArgs(map[string]interface{}{
			"x-dead-letter-exchange":    deadLetterExchangeName,
			"x-dead-letter-routing-key": deadLetterQueueName,
		}))

		err = b.declareDLX(deadLetterExchangeName, deadLetterQueueName, requeueRoutingKey)
		if err != nil {
			return fmt.Errorf("failed to declare dead letter exchange: %w", err)
		}
	}

	if err := consumer.StartConsuming(
		handler,
		groupName,
		filters,
		optionFuncs...,
	); err != nil {
		return fmt.Errorf("failed to start consuming events: %w", err)
	}

	// Handle until context is cancelled.
	b.wg.Add(1)

	go b.handle(consumer, consumerName)

	return nil
}

// Errors implements the Errors method of the eventhorizon.EventBus interface.
func (b *EventBus) Errors() <-chan error {
	return b.errCh
}

// Close implements the Close method of the eventhorizon.EventBus interface.
func (b *EventBus) Close() error {
	// Stop handling.
	b.cancel()
	b.wg.Wait()

	err := b.publisher.StopPublishing()
	if err != nil {
		return fmt.Errorf("failed to stop publishing: %w", err)
	}

	return nil
}

// Handles all events coming in on the channel.
func (b *EventBus) handle(
	consumer rabbitmq.Consumer,
	consumerName string,
) {
	defer b.wg.Done()

	<-b.cctx.Done()

	consumer.StopConsuming(consumerName, false)
	consumer.Disconnect()
}

func (b *EventBus) handler(
	ctx context.Context,
	m eh.EventMatcher,
	h eh.EventHandler,
) func(d rabbitmq.Delivery) rabbitmq.Action {
	return func(msg rabbitmq.Delivery) rabbitmq.Action {
		event, ctx, err := b.codec.UnmarshalEvent(ctx, msg.Body)
		if err != nil {
			err = fmt.Errorf("could not unmarshal event: %w", err)
			select {
			case b.errCh <- &eh.EventBusError{Err: err, Ctx: ctx}:
			default:
				log.Printf("eventhorizon: missed error in RabbitMQ event bus: %s", err)
			}

			if b.dlx {
				return rabbitmq.NackRequeue
			}

			return rabbitmq.NackDiscard
		}

		// Ignore non-matching events.
		if !m.Match(event) {
			return rabbitmq.Ack
		}

		// Handle the event if it did match.
		if err := h.HandleEvent(ctx, event); err != nil {
			err = fmt.Errorf("could not handle event (%s): %w", h.HandlerType(), err)
			select {
			case b.errCh <- &eh.EventBusError{Err: err, Ctx: ctx, Event: event}:
			default:
				log.Printf("eventhorizon: missed error in RabbitMQ event bus: %s", err)
			}

			if b.dlx {
				return rabbitmq.NackRequeue
			}

			return rabbitmq.NackDiscard
		}

		return rabbitmq.Ack
	}
}

func createFilter(topic string, m eh.EventMatcher) []string {
	// TODO: support other event matcher types

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

func (b *EventBus) declareDLX(deadLetterExchangeName, deadLetterQueueName, requeueRoutingKey string) error {
	connection, err := amqp091.Dial(b.addr)
	if err != nil {
		return fmt.Errorf("failed to connect to amqp091: %w", err)
	}

	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to open amqp091 channel: %w", err)
	}

	err = channel.ExchangeDeclare(
		deadLetterExchangeName, // name
		"direct",               // kind
		true,                   // durable
		false,                  // autoDelete
		false,                  // internal
		false,                  // noWait
		nil,                    // args
	)
	if err != nil {
		return fmt.Errorf("failed to declare amqp091 dead letter exchange: %w", err)
	}

	_, err = channel.QueueDeclare(
		deadLetterQueueName, // name
		true,                // durable
		false,               // autoDelete
		false,               // exclusive
		false,               // noWait
		map[string]interface{}{
			"x-dead-letter-exchange":    b.exchangeName,
			"x-dead-letter-routing-key": requeueRoutingKey,
			"x-message-ttl":             b.dlxttl.Milliseconds(),
		}, // args
	)
	if err != nil {
		return fmt.Errorf("failed to declare amqp091 dead letter queue: %w", err)
	}

	err = channel.QueueBind(
		deadLetterQueueName,    // queueName
		deadLetterQueueName,    // routingKey
		deadLetterExchangeName, // exchange
		false,                  // noWait
		nil,                    // args
	)
	if err != nil {
		return fmt.Errorf("failed to bind to amqp091 dead letter exchange: %w", err)
	}

	err = channel.Close()
	if err != nil {
		return fmt.Errorf("failed to close amqp091 channel: %w", err)
	}

	err = connection.Close()
	if err != nil {
		return fmt.Errorf("failed to close amqp091 connection: %w", err)
	}

	return nil
}
