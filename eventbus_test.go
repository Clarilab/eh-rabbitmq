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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Clarilab/clarimq"
	rabbitmq "github.com/Clarilab/eh-rabbitmq"
	"github.com/looplab/eventhorizon/eventbus"
	"github.com/looplab/eventhorizon/uuid"
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

		connCount := getConnectionCount(t)

		if connCount != 6 { // expecting 6 connections: 1 publish and 1 consume connections per event bus
			t.Fatal("there should be 6 connections, got:", connCount)
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

		connCount := getConnectionCount(t)

		if connCount != 2 { // expecting 2 connections
			t.Fatal("there should be 2 connections, got:", connCount)
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

func getConnectionCount(t *testing.T) int {
	t.Helper()

	type rqmAPIResponse []struct{}

	request, err := http.NewRequest(http.MethodGet, "http://localhost:15672/api/connections", nil)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	request.SetBasicAuth("guest", "guest")

	restClient := http.Client{}

	resp, err := restClient.Do(request)
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

	var apiResp rqmAPIResponse

	err = json.Unmarshal(buff.Bytes(), &apiResp)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	return len(apiResp)
}
