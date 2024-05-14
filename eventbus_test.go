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
	"testing"
	"time"

	"github.com/Clarilab/clarimq"
	rabbitmq "github.com/Clarilab/eh-rabbitmq"
	"github.com/Clarilab/eventhorizon"
	"github.com/Clarilab/eventhorizon/eventbus"
	"github.com/Clarilab/eventhorizon/mocks"
	"github.com/Clarilab/eventhorizon/uuid"
)

func Test_Integration_AddHandler(t *testing.T) { //nolint:paralleltest // must not run in parallel
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	bus, _, err := newTestEventBus("")
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	t.Cleanup(func() { bus.Close() })

	eventbus.TestAddHandler(t, bus)
}

func Test_Integration_RemoveHandler(t *testing.T) { //nolint:paralleltest // must not run in parallel
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	bus, _, err := newTestEventBus("app-id")
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	t.Cleanup(func() { bus.Close() })

	t.Run("happy path", func(t *testing.T) { //nolint:paralleltest // must not run in parallel
		handler := mocks.NewEventHandler("handler-1")
		queueName := fmt.Sprintf("%s_%s", "app-id", handler.HandlerType())

		if err := bus.AddHandler(context.Background(), eventhorizon.MatchAll{}, handler); err != nil {
			t.Fatal("there should be no error:", err)
		}

		if len(bus.RegisteredHandlers()) != 1 {
			t.Fatal("there should be 1 registered handler")
		}

		if expectQueueConsumerCount(t, queueName, 1); err != nil {
			t.Fatal("there should be 1 consumer on the queue")
		}

		if err := bus.RemoveHandler(eventhorizon.EventHandlerType("handler-1")); err != nil {
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
		err := bus.RemoveHandler(eventhorizon.EventHandlerType("handler-1"))
		if !errors.Is(err, rabbitmq.ErrHandlerNotRegistered) {
			t.Fatal("error should be: 'handler not registered'", err)
		}
	})
}

func Test_Integration_EventBus(t *testing.T) { //nolint:paralleltest // must not run in parallel
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	bus1, appID, err := newTestEventBus("")
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	t.Cleanup(func() { bus1.Close() })

	bus2, _, err := newTestEventBus(appID)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	t.Cleanup(func() { bus2.Close() })

	t.Logf("using stream: %s_events", appID)

	eventbus.AcceptanceTest(t, bus1, bus2, time.Second)
}

func Test_Integration_EventBusLoadTest(t *testing.T) { //nolint:paralleltest // must not run in parallel
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	bus, appID, err := newTestEventBus("")
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	t.Cleanup(func() { bus.Close() })

	t.Logf("using stream: %s_events", appID)

	eventbus.LoadTest(t, bus)
}

func Test_Integration_ExternalConnections(t *testing.T) { //nolint:paralleltest // must not run in parallel
	amqpURI := "amqp://guest:guest@localhost:5672/"
	waitTime := 5 * time.Second

	t.Run("without external connections", func(t *testing.T) { //nolint:paralleltest // must not run in parallel
		for i := 0; i < 3; i++ {
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

		consumeConn, err := clarimq.NewConnection(amqpURI)
		if err != nil {
			t.Fatal("there should be no error:", err)
		}

		for i := 0; i < 3; i++ {
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

func newTestEventBus(appID string) (*rabbitmq.EventBus, string, error) {
	// Get a random app ID.
	if appID == "" {
		bts := make([]byte, 8)
		if _, err := rand.Read(bts); err != nil {
			return nil, "", fmt.Errorf("could not randomize app ID: %w", err)
		}

		appID = "app-" + hex.EncodeToString(bts)
	}

	bus, err := rabbitmq.NewEventBus("amqp://guest:guest@localhost:5672/", appID, uuid.New().String(), "eh-rabbitmq-test", "rabbit")
	if err != nil {
		return nil, "", fmt.Errorf("could not create event bus: %w", err)
	}

	return bus, appID, nil
}

func expectConnectionCount(t *testing.T, expected int) error {
	t.Helper()

	type rqmAPIResponse []struct{}

	request, err := http.NewRequest(http.MethodGet, "http://localhost:15672/api/connections", nil)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	request.SetBasicAuth("guest", "guest")

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

	request, err := http.NewRequest(http.MethodGet, "http://localhost:15672/api/queues", nil)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	request.SetBasicAuth("guest", "guest")

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
