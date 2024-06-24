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

// SetupEventHandler sets up an Eventhandler to the event bus.
func (b *EventBus) SetupEventHandler(ctx context.Context, eventHandler EventHandler) error {
	const errMessage = "failed to setup event handler: %w"

	if err := b.AddHandlerWithOptions(
		ctx,
		eventHandler.Events(),
		eventHandler,
		WithHandlerTopic(eventHandler.Topic()),
	); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

// SetupEventHandlers sets up the given event handlers.
func (b *EventBus) SetupEventHandlers(ctx context.Context, handlers ...EventHandler) error {
	const errMessage = "failed to add event handlers: %w"

	for i := range handlers {
		handler := handlers[i]

		if err := b.SetupEventHandler(ctx, handler); err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	return nil
}
