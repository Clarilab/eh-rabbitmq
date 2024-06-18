package rabbitmq

import (
	"github.com/Clarilab/clarimq"
)

type handler struct {
	*clarimq.Consumer
	topic    string
	exchange string
}

func (b *EventBus) newHandler() *handler {
	return &handler{
		topic:    b.topic,
		exchange: b.exchangeName,
	}
}

// HandlerOption is a handler option.
type HandlerOption func(*handler)

// WithHandlerTopic is an option to set the handler topic.
func WithHandlerTopic(topic string) HandlerOption {
	return func(h *handler) {
		h.topic = topic
	}
}

// WithHandlerExchange is an option to set the handler exchange.
func WithHandlerExchange(name string) HandlerOption {
	return func(h *handler) {
		h.exchange = name
	}
}
