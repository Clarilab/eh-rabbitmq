package rabbitmq

import (
	"context"
	"fmt"

	"github.com/Clarilab/clarimq"
	ehtracygo "github.com/Clarilab/eh-tracygo"
	eh "github.com/looplab/eventhorizon"
)

const (
	retryCountHeader = "x-retry-count"
)

// Handles all events coming in on the channel.
func (b *EventBus) handle(
	consumer *clarimq.Consumer,
) {
	defer b.wg.Done()

	<-b.ctx.Done()

	b.consumerMu.Lock()
	consumer.Close()
	b.consumerMu.Unlock()
}

func (b *EventBus) handler(
	ctx context.Context,
	matcher eh.EventMatcher,
	handler eh.EventHandler,
) func(d *clarimq.Delivery) clarimq.Action {
	return func(msg *clarimq.Delivery) clarimq.Action {
		event, ctx, err := b.eventCodec.UnmarshalEvent(ctx, msg.Body)
		if err != nil {
			b.sendErrToErrChannel(ctx, err, handler, event)

			return clarimq.NackDiscard
		}

		// Ignore non-matching events.
		if !matcher.Match(event) {
			return clarimq.Ack
		}

		if b.useRetry {
			retryCount, ok := msg.Headers[retryCountHeader].(int32)
			if !ok {
				retryCount = 0
			}

			ctx = NewContextWithNumRetries(ctx, int64(retryCount))
		}

		// Handle the event if it did match.
		if err := handler.HandleEvent(
			ehtracygo.NewContext(ctx, msg.CorrelationId),
			event,
		); err != nil {
			b.sendErrToErrChannel(ctx, err, handler, event)

			return clarimq.NackDiscard
		}

		return clarimq.Ack
	}
}

func (b *EventBus) returnHandler(rtn clarimq.Return) {
	event, ctx, err := b.eventCodec.UnmarshalEvent(b.ctx, rtn.Body)
	if err != nil {
		b.logger.logDebug("return handler: failed to unmarshal event", "error", err)

		return
	}

	b.errCh <- &eh.EventBusError{Err: ErrCouldNotBeRouted, Ctx: ctx, Event: event}
}

func (b *EventBus) sendErrToErrChannel(ctx context.Context, err error, h eh.EventHandler, event eh.Event) {
	err = fmt.Errorf("could not handle event (%s): %w", h.HandlerType(), err)
	select {
	case b.errCh <- &EventBusError{eh.EventBusError{Err: err, Ctx: ctx, Event: event}, h.HandlerType()}:
	default:
		b.logger.logError("eventhorizon: missed error in RabbitMQ event bus", "error", err)
	}
}
