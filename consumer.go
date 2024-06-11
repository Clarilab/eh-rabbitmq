package rabbitmq

import (
	"github.com/Clarilab/clarimq"
)

type consumer struct {
	*clarimq.Consumer
	topic    string
	exchange string
}

func (b *EventBus) newConsumer() *consumer {
	return &consumer{
		topic:    b.topic,
		exchange: b.exchangeName,
	}
}

// HandlerOption is a handler option.
type HandlerOption func(*consumer)

// WithHandlerTopic is an option to set the handler topic.
func WithHandlerTopic(topic string) PublishOption {
	return func(p *publisher) {
		p.topic = topic
	}
}

// WithHandlerExchange is an option to set the handler exchange.
func WithHandlerExchange(name string) PublishOption {
	return func(p *publisher) {
		p.exchange = name
	}
}
