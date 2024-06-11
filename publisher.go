package rabbitmq

type publisher struct {
	topic    string
	exchange string
}

func (b *EventBus) newPublisher() *publisher {
	return &publisher{
		topic:    b.topic,
		exchange: b.exchangeName,
	}
}

// PublishOption is a publish option.
type PublishOption func(*publisher)

// WithPublishingTopic is an option to set the publishing topic.
func WithPublishingTopic(topic string) PublishOption {
	return func(p *publisher) {
		p.topic = topic
	}
}

// WithPublishingExchange is an option to set the publishing exchange.
func WithPublishingExchange(name string) PublishOption {
	return func(p *publisher) {
		p.exchange = name
	}
}
