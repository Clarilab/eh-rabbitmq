package rabbitmq_test

import (
	"context"
	"errors"
	"fmt"
	"sync"

	eh "github.com/Clarilab/eventhorizon"
)

const (
	handler1Topic = "handler-1-topic"
	handler2Topic = "handler-2-topic"
)

const (
	Handler1Event = eh.EventType("handler-1-event")
	Handler2Event = eh.EventType("handler-2-event")
)

func RegisterEvents() {
	eh.RegisterEventData(Handler1Event, func() eh.EventData { return new(HandlerEventData) })
	eh.RegisterEventData(Handler2Event, func() eh.EventData { return new(HandlerEventData) })
}

func UnregisterEvents() {
	eh.UnregisterEventData(Handler1Event)
	eh.UnregisterEventData(Handler2Event)
}

type handler1 struct {
	wg *sync.WaitGroup
}

func (*handler1) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType("test-handler-1")
}

func (h *handler1) HandleEvent(_ context.Context, _ eh.Event) error {
	h.wg.Done()

	return nil
}

func (*handler1) Events() eh.MatchEvents {
	return eh.MatchEvents{
		Handler1Event,
	}
}

func (*handler1) Topic() string {
	return handler1Topic
}

type HandlerEventData struct{}

type handler2 struct {
	wg *sync.WaitGroup
}

func (*handler2) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType("test-handler-2")
}

func (h *handler2) HandleEvent(_ context.Context, _ eh.Event) error {
	h.wg.Done()

	return nil
}

func (*handler2) Events() eh.MatchEvents {
	return eh.MatchEvents{
		Handler2Event,
	}
}

func (*handler2) Topic() string {
	return handler2Topic
}

type mockErrorEventHandler struct{}

func (*mockErrorEventHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType("mock-event-handler")
}

var errTestError = errors.New("error-from-mock-event-handler")

func (*mockErrorEventHandler) HandleEvent(_ context.Context, _ eh.Event) error {
	err := fmt.Errorf("failed to handle event: %w", errTestError)

	return err
}
