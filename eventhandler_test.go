package rabbitmq_test

import (
	"context"
	"sync"
	"testing"
	"time"

	rabbitmq "github.com/Clarilab/eh-rabbitmq/v2"
	eh "github.com/Clarilab/eventhorizon"
)

func Test_Integration_SetupEventHandlers(t *testing.T) { //nolint:paralleltest // must not run in parallel
	bus, _, err := newTestEventBus("")
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	t.Cleanup(func() { bus.Close() })

	wg1 := new(sync.WaitGroup)
	wg2 := new(sync.WaitGroup)

	handler1 := &handler1{wg1}
	handler2 := &handler2{wg2}

	ctx := context.Background()

	if err = bus.SetupEventHandlers(ctx, handler1, handler2); err != nil {
		t.Fatal("there should be no error:", err)
	}

	wg1.Add(1)
	wg2.Add(1)

	RegisterEvents()

	defer UnregisterEvents()

	if err := bus.StartHandling(); err != nil {
		t.Fatal("there should be no error:", err)
	}

	handlers := bus.RegisteredHandlers()
	if len(handlers) != 2 {
		t.Fatal("there should be 2 registered handlers")
	}

	if err := bus.PublishEventWithOptions(
		ctx,
		eh.NewEvent(Handler1Event, new(HandlerEventData), time.Now()),
		rabbitmq.WithPublishingTopic(handler1Topic),
	); err != nil {
		t.Fatal("there should be no error:", err)
	}

	if err := bus.PublishEventWithOptions(
		ctx,
		eh.NewEvent(Handler2Event, new(HandlerEventData), time.Now()),
		rabbitmq.WithPublishingTopic(handler2Topic),
	); err != nil {
		t.Fatal("there should be no error:", err)
	}

	// waiting for handlers to handle events.
	wg1.Wait()
	wg2.Wait()
}

func Test_SetupEventHandlersWithMiddleware(t *testing.T) { //nolint:paralleltest // must not run in parallel
	bus, _, err := newTestEventBus("")
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	t.Cleanup(func() { bus.Close() })

	wg := new(sync.WaitGroup)
	handler := &handler1{wg}

	middlewares := []eh.EventHandlerMiddleware{newTestEventHandlerMiddleware(wg)}

	ctx := context.Background()

	if err = bus.SetupEventHandlersWithMiddlewares(ctx, middlewares, handler); err != nil {
		t.Fatal("there should be no error:", err)
	}

	if err := bus.StartHandling(); err != nil {
		t.Fatal("there should be no error:", err)
	}

	RegisterEvents()

	t.Cleanup(UnregisterEvents)

	wg.Add(2) // 'wg.Done' expected to be called by the middleware and the actual handler

	if err := bus.PublishEventWithOptions(
		ctx,
		eh.NewEvent(Handler1Event, new(HandlerEventData), time.Now()),
		rabbitmq.WithPublishingTopic(handler1Topic),
	); err != nil {
		t.Fatal("there should be no error:", err)
	}

	wg.Wait()
}
