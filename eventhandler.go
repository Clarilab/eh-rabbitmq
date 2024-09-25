package rabbitmq

import (
	"context"
	"fmt"

	eh "github.com/Clarilab/eventhorizon"
)

// EventHandler is an interface for a type that handles events.
type EventHandler interface {
	eh.EventHandler
	// Event returns the event types the handler is able to handle.
	Events() eh.MatchEvents
	// Topic returns the topic the handler is subscribed to.
	Topic() string
}

// SetupEventHandlers sets up the given event handlers.
func (b *EventBus) SetupEventHandlers(ctx context.Context, handlers ...EventHandler) error {
	const errMessage = "failed to add event handlers: %w"

	for i := range handlers {
		handler := handlers[i]

		err := b.setupEventHandler(ctx, handler.Events(), handler, handler.Topic())
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	return nil
}

// SetupEventHandlersWithMiddleware sets up every given handler with the given middlewares.
func (b *EventBus) SetupEventHandlersWithMiddleware(
	ctx context.Context,
	middleware []eh.EventHandlerMiddleware,
	handlers ...EventHandler,
) error {
	const errMessage = "failed to add event handlers: %w"

	for i := range handlers {
		eventHandler := handlers[i]

		handler, ok := eventHandler.(eh.EventHandler)
		if !ok {
			return fmt.Errorf(errMessage, ErrInvalidEventHandler)
		}

		if len(middleware) > 0 {
			handler = eh.UseEventHandlerMiddleware(eventHandler, middleware...)
		}

		err := b.setupEventHandler(ctx, eventHandler.Events(), handler, eventHandler.Topic())
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	return nil
}

func (b *EventBus) setupEventHandler(
	ctx context.Context,
	match eh.EventMatcher,
	handler eh.EventHandler,
	topic string,
) error {
	const errMessage = "failed to setup event handler: %w"

	if err := b.AddHandlerWithOptions(
		ctx,
		match,
		handler,
		WithHandlerTopic(topic),
	); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}
