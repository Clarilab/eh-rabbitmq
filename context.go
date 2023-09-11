package rabbitmq

import (
	"context"
)

// DefaultNumRetries is the retry value to use if not set in the context.
const DefaultNumRetries = 0

type contextKey int

const (
	retriesKey contextKey = iota
)

// NumRetriesFromContext returns the number of retries from the context, or zero.
func NumRetriesFromContext(ctx context.Context) int64 {
	if numRetries, ok := ctx.Value(retriesKey).(int64); ok {
		return numRetries
	}

	return DefaultNumRetries
}

// NewContextWithNumRetries sets the retries value to use in the context. The number of retries is used to
// determine how often an event has failed to be handled.
func NewContextWithNumRetries(ctx context.Context, numRetries int64) context.Context {
	return context.WithValue(ctx, retriesKey, numRetries)
}
