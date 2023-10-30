package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	"github.com/Clarilab/clarimq"
	eh "github.com/looplab/eventhorizon"
)

func createFilter(topic string, m eh.EventMatcher) []string {
	switch m := m.(type) {
	case eh.MatchEvents:
		s := make([]string, len(m))
		for i, et := range m {
			s[i] = fmt.Sprintf(`%s.%s`, topic, et) // Filter event types by key to save space.
		}

		return s
	default:
		return []string{fmt.Sprintf("%s.*", topic)}
	}
}

func (b *EventBus) setupConnections() error {
	const errMessage = "failed to setup eventbus connections: %w"

	var err error

	if b.publishConn, err = b.setupConnection(
		clarimq.WithConnectionOptionConnectionName(fmt.Sprintf("%s_publish_connection", b.appID)),
		clarimq.WithConnectionOptionReturnHandler(b.returnHandler),
		clarimq.WithConnectionOptionMultipleLoggers(b.loggers),
	); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if b.consumeConn, err = b.setupConnection(
		clarimq.WithConnectionOptionConnectionName(fmt.Sprintf("%s_consume_connection", b.appID)),
		clarimq.WithConnectionOptionMultipleLoggers(b.loggers),
	); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if b.publisher, err = clarimq.NewPublisher(b.publishConn,
		clarimq.WithPublisherOptionPublishingCache(b.publishingCache),
	); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (b *EventBus) setupConnection(options ...clarimq.ConnectionOption) (*clarimq.Connection, error) {
	const errMessage = "failed to setup connection: %w"

	opt := []clarimq.ConnectionOption{
		clarimq.WithConnectionOptionBackOffFactor(1),
		clarimq.WithConnectionOptionMaxRecoveryRetries(int(InfiniteRetries)),
	}

	opt = append(opt, options...)

	conn, err := clarimq.NewConnection(b.addr, opt...)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	b.watchConnectionErrors(conn)

	return conn, nil
}

func (b *EventBus) watchConnectionErrors(conn *clarimq.Connection) {
	go func() {
		for err := range conn.NotifyErrors() {
			if err == nil {
				return
			}

			var amqpErr *clarimq.AMQPError
			var recoveryFailed *clarimq.RecoveryFailedError

			switch {
			case errors.As(err, &amqpErr):
				err := AMQPError(*amqpErr)

				b.errCh <- &err

			case errors.As(err, &recoveryFailed):
				b.errCh <- &RecoveryFailedError{err}

			default:
				b.errCh <- err
			}
		}
	}()
}

func (b *EventBus) declareConsumer(ctx context.Context, matcher eh.EventMatcher, handler eh.EventHandler) (*clarimq.Consumer, error) {
	const errMessage = "failed to declare consumer: %w"

	queueName := fmt.Sprintf("%s_%s", b.appID, handler.HandlerType())

	optionFuncs := []clarimq.ConsumeOption{
		clarimq.WithConsumerOptionConsumerName(fmt.Sprintf("%s_%s", queueName, b.clientID)),
		clarimq.WithExchangeOptionName(b.exchangeName),
		clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
		clarimq.WithExchangeOptionDeclare(true),
		clarimq.WithExchangeOptionDurable(true),
		clarimq.WithQueueOptionDurable(true),
	}

	if b.useRetry {
		optionFuncs = append(optionFuncs, clarimq.WithConsumerOptionDeadLetterRetry(&clarimq.RetryOptions{
			RetryConn:  b.publishConn,
			Delays:     b.queueDelays,
			MaxRetries: b.maxRetries,
		}))
	}

	for _, routingKey := range createFilter(b.topic, matcher) {
		optionFuncs = append(optionFuncs, clarimq.WithConsumerOptionRoutingKey(routingKey))
	}

	consumer, err := clarimq.NewConsumer(
		b.consumeConn,
		queueName,
		b.handler(ctx, matcher, handler),
		optionFuncs...,
	)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	return consumer, nil
}