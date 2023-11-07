package rabbitmq

import (
	"log/slog"
	"time"

	"github.com/Clarilab/clarimq"
	eh "github.com/looplab/eventhorizon"
)

// Option is an option setter used to configure creation.
type Option func(*EventBus)

// WithEventCodec uses the specified codec for encoding events.
func WithEventCodec(codec eh.EventCodec) Option {
	return func(b *EventBus) {
		b.eventCodec = codec
	}
}

// WithLogging enables logging to the given loggers.
func WithLogging(loggers []*slog.Logger) Option {
	return func(b *EventBus) {
		b.loggers = loggers
	}
}

// WithRetry enables event retries. If maxRetries is bigger than the number of delays provided,
// it will use the last value until maxRetries has been reached. Use InfiniteRetries to never drop the message.
func WithRetry(maxRetries int64, delays []time.Duration) Option {
	return func(bus *EventBus) {
		bus.useRetry = true
		bus.maxRetries = maxRetries
		bus.queueDelays = delays
	}
}

// WithMaxRecoveryRetry sets the max count for recovery retries.
//
// Default: Infinite.
func WithMaxRecoveryRetry(maxRetries int64) Option {
	return func(b *EventBus) {
		b.maxRecoveryRetries = maxRetries
	}
}

// WithClariMQPublishingCache enables caching events that failed to be published.
func WithClariMQPublishingCache(publishingCache clarimq.PublishingCache) Option {
	return func(b *EventBus) {
		b.publishingCache = publishingCache
	}
}

// WithClariMQConnections sets the connections used for publishing and consuming events.
func WithClariMQConnections(publishingConn *clarimq.Connection, consumeConn *clarimq.Connection) Option {
	return func(bus *EventBus) {
		if publishingConn != nil {
			bus.publishConn = publishingConn
		}

		if consumeConn != nil {
			bus.consumeConn = consumeConn
		}
	}
}
