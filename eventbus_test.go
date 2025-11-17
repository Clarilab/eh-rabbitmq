// Copyright (c) 2014 - The Event Horizon authors.
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

package rabbitmq_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Clarilab/clarimq/v2"
	rabbitmq "github.com/Clarilab/eh-rabbitmq/v2"
	eh "github.com/Clarilab/eventhorizon"
	"github.com/Clarilab/eventhorizon/eventbus"
	"github.com/Clarilab/eventhorizon/mocks"
	"github.com/Clarilab/eventhorizon/namespace"
	"github.com/Clarilab/eventhorizon/uuid"
	"github.com/orlangure/gnomock"
	gnormq "github.com/orlangure/gnomock/preset/rabbitmq"
)

const (
	rmqVersion = "3.13.3-management"
	rmqUser    = "guest"
	rmqPasswd  = "guest"
)

var (
	rmqPort           int //nolint:gochecknoglobals // test code
	rmqManagementPort int //nolint:gochecknoglobals // test code
)

func TestMain(m *testing.M) {
	preset := gnormq.Preset(gnormq.WithUser(rmqUser, rmqPasswd), gnormq.WithVersion(rmqVersion))

	container, err := gnomock.Start(preset, gnomock.WithUseLocalImagesFirst())
	if err != nil {
		panic(err)
	}

	rmqPort = container.DefaultPort()
	rmqManagementPort = container.Port(gnormq.ManagementPort)

	m.Run()
}

func Test_Integration_AddHandler(t *testing.T) { //nolint:paralleltest // must not run in parallel
	bus, _, err := newTestEventBus("")
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	t.Cleanup(func() { bus.Close() })

	eventbus.TestAddHandler(t, bus)
}

func Test_Integration_RemoveHandler(t *testing.T) { //nolint:paralleltest // must not run in parallel
	bus, _, err := newTestEventBus("app-id")
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	t.Cleanup(func() { bus.Close() })

	t.Run("happy path", func(t *testing.T) { //nolint:paralleltest // must not run in parallel
		handler := mocks.NewEventHandler("handler-1")
		queueName := fmt.Sprintf("%s_%s", "app-id", handler.HandlerType())

		if err := bus.AddHandler(context.Background(), eh.MatchAll{}, handler); err != nil {
			t.Fatal("there should be no error:", err)
		}

		if len(bus.RegisteredHandlers()) != 1 {
			t.Fatal("there should be 1 registered handler")
		}

		if expectQueueConsumerCount(t, queueName, 1); err != nil {
			t.Fatal("there should be 1 consumer on the queue")
		}

		if err := bus.RemoveHandler(eh.EventHandlerType("handler-1")); err != nil {
			t.Fatal("there should be no error:", err)
		}

		if len(bus.RegisteredHandlers()) != 0 {
			t.Fatal("there should be 0 registered handler")
		}

		if expectQueueConsumerCount(t, queueName, 0); err != nil {
			t.Fatal("there should be 0 consumer on the queue")
		}
	})

	t.Run("handler not registered", func(t *testing.T) { //nolint:paralleltest // must not run in parallel
		err := bus.RemoveHandler(eh.EventHandlerType("handler-1"))
		if !errors.Is(err, rabbitmq.ErrHandlerNotRegistered) {
			t.Fatal("error should be: 'handler not registered'", err)
		}
	})
}

func Test_Integration_EventBus(t *testing.T) { //nolint:paralleltest // must not run in parallel
	bus1, appID, err := newTestEventBus("", rabbitmq.WithHandlerConsumeAfterAdd(true))
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	t.Cleanup(func() { bus1.Close() })

	bus2, _, err := newTestEventBus(appID, rabbitmq.WithHandlerConsumeAfterAdd(true))
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	t.Cleanup(func() { bus2.Close() })

	t.Logf("using stream: %s_events", appID)

	eventbus.AcceptanceTest(t, bus1, bus2, time.Second)
}

func Test_Integration_EventBusLoadTest(t *testing.T) { //nolint:paralleltest // must not run in parallel
	bus, appID, err := newTestEventBus("", rabbitmq.WithHandlerConsumeAfterAdd(true))
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	t.Cleanup(func() { bus.Close() })

	t.Logf("using stream: %s_events", appID)

	eventbus.LoadTest(t, bus)
}

func Test_Integration_ExternalConnections(t *testing.T) { //nolint:paralleltest // must not run in parallel
	amqpURI := fmt.Sprintf("amqp://%s:%s@localhost:%d/", rmqUser, rmqPasswd, rmqPort)
	waitTime := 5 * time.Second

	t.Run("without external connections", func(t *testing.T) { //nolint:paralleltest // must not run in parallel
		for range 3 {
			bus, err := rabbitmq.NewEventBus(
				amqpURI,
				"test-app",
				uuid.New().String(),
				"eh-rabbitmq-test",
				"rabbit",
			)
			if err != nil {
				t.Fatal("there should be no error:", err)
			}

			t.Cleanup(func() { bus.Close() })
		}

		time.Sleep(waitTime) // wait for connections to be fully established

		if err := expectConnectionCount(t, 6); err != nil {
			t.Fatal("there should be 6 connections")
		}
	})

	t.Run("with external connections", func(t *testing.T) { //nolint:paralleltest // must not run in parallel
		publishConn, err := clarimq.NewConnection(amqpURI)
		if err != nil {
			t.Fatal("there should be no error:", err)
		}

		t.Cleanup(func() {
			if err := publishConn.Close(); err != nil {
				t.Fatal("there should be no error:", err)
			}
		})

		consumeConn, err := clarimq.NewConnection(amqpURI)
		if err != nil {
			t.Fatal("there should be no error:", err)
		}

		t.Cleanup(func() {
			if err := consumeConn.Close(); err != nil {
				t.Fatal("there should be no error:", err)
			}
		})

		for range 3 {
			bus, err := rabbitmq.NewEventBus(
				"it does not matter what the uri is",
				"test-app",
				uuid.New().String(),
				"eh-rabbitmq-test",
				"rabbit",
				rabbitmq.WithClariMQConnections(publishConn, consumeConn),
			)
			if err != nil {
				t.Fatal("there should be no error:", err)
			}

			t.Cleanup(func() { bus.Close() })
		}

		time.Sleep(waitTime) // wait for connections to be fully established

		if err := expectConnectionCount(t, 2); err != nil {
			t.Fatal("there should be 2 connections")
		}
	})
}

func Benchmark_EventBus(b *testing.B) {
	bus, appID, err := newTestEventBus("")
	if err != nil {
		b.Fatal("there should be no error:", err)
	}

	b.Cleanup(func() { bus.Close() })

	b.Logf("using stream: %s_events", appID)

	eventbus.Benchmark(b, bus)
}

func Test_Integration_MaxRetriesExceededHandler(t *testing.T) { //nolint:paralleltest // must not run in parallel
	wg := new(sync.WaitGroup)
	wg.Add(1)

	// the max-retries-exceeded-handler
	handler := rabbitmq.MaxRetriesExceededHandler(func(ctx context.Context, event eh.Event, errorMessage string) error {
		ns := namespace.FromContext(ctx)

		if !strings.Contains(errorMessage, errTestError.Error()) {
			t.Fatal("error message should contain 'error-from-mock-event-handler'")
		}

		// assert namespace
		if ns != "test-namespace" {
			t.Fatal("namespace should be 'test-namespace'")
		}

		data, ok := event.Data().(*mocks.EventData)
		if !ok {
			t.Fatal("data should be of type mocks.EventData")
		}

		// assert event data
		if data.Content != "event-content" {
			t.Fatal("content should be 'event-content'")
		}

		// call done to finish test
		wg.Done()

		return nil
	})

	uri := fmt.Sprintf("amqp://%s:%s@localhost:%d/", rmqUser, rmqPasswd, rmqPort)

	// create event bus with retry options
	bus, err := rabbitmq.NewEventBus(uri, createAppID(), uuid.New().String(), "eh-rabbitmq-test", "rabbit",
		rabbitmq.WithRetry(2, []time.Duration{time.Second}, handler),
	)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	t.Cleanup(func() { bus.Close() })

	eventHandler := new(mockErrorEventHandler)

	// add mock event handler that always returns an error
	if err := bus.AddHandler(context.Background(), eh.MatchAll{}, eventHandler); err != nil {
		t.Fatal("there should be no error:", err)
	}

	if bus.StartHandling(); err != nil {
		t.Fatal("there should be no error:", err)
	}

	// publish test event
	if err := bus.PublishEvent(
		namespace.NewContext(context.Background(), "test-namespace"),
		eh.NewEvent(
			mocks.EventType,
			mocks.EventData{Content: "event-content"},
			time.Now(),
		)); err != nil {
		t.Fatal("there should be no error:", err)
	}

	// wait for max-retries-exceeded-handler to be called
	wg.Wait()
}

func Test_Integration_AddHandlerAfterHandlingAlreadyStarted(t *testing.T) { //nolint:paralleltest // must not run in parallel
	bus, _, err := newTestEventBus("")
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	t.Cleanup(func() { bus.Close() })

	// setup first handler

	wg1 := new(sync.WaitGroup)
	handler1 := &handler1{wg1}
	wg1.Add(1)

	ctx := context.Background()

	if err = bus.SetupEventHandlers(ctx, handler1); err != nil {
		t.Fatal("there should be no error:", err)
	}

	// start handling events

	if err := bus.StartHandling(); err != nil {
		t.Fatal("there should be no error:", err)
	}

	// setup second handler AFTER handling already started
	// the handler should automatically be started

	wg2 := new(sync.WaitGroup)
	handler2 := &handler2{wg2}
	wg2.Add(1)

	if err = bus.SetupEventHandlers(ctx, handler2); err != nil {
		t.Fatal("there should be no error:", err)
	}

	handlers := bus.RegisteredHandlers()
	if len(handlers) != 2 {
		t.Fatal("there should be 2 registered handlers")
	}

	RegisterEvents()

	t.Cleanup(UnregisterEvents)

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

func newTestEventBus(appID string, options ...rabbitmq.Option) (*rabbitmq.EventBus, string, error) {
	// Get a random app ID.
	if appID == "" {
		appID = createAppID()
	}

	uri := fmt.Sprintf("amqp://%s:%s@localhost:%d/", rmqUser, rmqPasswd, rmqPort)

	bus, err := rabbitmq.NewEventBus(uri, appID, uuid.New().String(), "eh-rabbitmq-test", "rabbit", options...)
	if err != nil {
		return nil, "", fmt.Errorf("could not create event bus: %w", err)
	}

	return bus, appID, nil
}

func newTestEventHandlerMiddleware(wg *sync.WaitGroup) eh.EventHandlerMiddleware {
	return func(h eh.EventHandler) eh.EventHandler {
		return &testEventMiddlewareHandler{
			inner: h,
			wg:    wg,
		}
	}
}

type testEventMiddlewareHandler struct {
	inner eh.EventHandler
	wg    *sync.WaitGroup
}

func (m *testEventMiddlewareHandler) HandlerType() eh.EventHandlerType {
	return m.inner.HandlerType()
}

func (m *testEventMiddlewareHandler) HandleEvent(ctx context.Context, event eh.Event) error {
	if err := m.inner.HandleEvent(ctx, event); err != nil {
		return err
	}

	m.wg.Done()

	return nil
}

func createAppID() string {
	bts := make([]byte, 8)
	_, _ = rand.Read(bts)

	return "app-" + hex.EncodeToString(bts)
}

func expectConnectionCount(t *testing.T, expected int) error {
	t.Helper()

	type rqmAPIResponse []struct{}

	url := fmt.Sprintf("http://localhost:%d/api/connections", rmqManagementPort)

	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	request.SetBasicAuth(rmqUser, rmqPasswd)

	var apiResp rqmAPIResponse

	do := func() bool {
		return len(apiResp) == expected
	}

	return compare(t, request, &apiResp, do)
}

func expectQueueConsumerCount(t *testing.T, queueName string, expected int) error {
	t.Helper()

	type queue struct {
		Name      string `json:"name"`
		Consumers int    `json:"consumers"`
	}

	type rqmAPIResponse []queue

	url := fmt.Sprintf("http://localhost:%d/api/queues", rmqManagementPort)

	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	request.SetBasicAuth(rmqUser, rmqPasswd)

	var apiResp rqmAPIResponse

	do := func() bool {
		for i := range apiResp {
			return apiResp[i].Name == queueName && apiResp[i].Consumers == expected
		}

		return false
	}

	return compare(t, request, &apiResp, do)
}

func compare(t *testing.T, request *http.Request, result any, compareFn func() bool) error {
	t.Helper()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	retries := 0

	restClient := new(http.Client)

	for range ticker.C {
		retries++

		pollRMQ(t, restClient, request, &result)

		if compareFn() {
			return nil
		}

		if retries >= 10 {
			return errors.New("failed to compare after retry limit exceeded") //nolint:goerr113 // test code
		}
	}

	return nil
}

func pollRMQ(t *testing.T, client *http.Client, request *http.Request, result any) {
	t.Helper()

	resp, err := client.Do(request)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatal("RabbitMQ management API should return status code 200")
	}

	buff := new(bytes.Buffer)

	_, err = buff.ReadFrom(resp.Body)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	err = json.Unmarshal(buff.Bytes(), &result)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}
}
