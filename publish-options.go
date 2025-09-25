package rabbitmq

type publishOptions struct {
	topic            string
	exchange         string
	publishMandatory bool
}

func (b *EventBus) newPublishOptions() *publishOptions {
	return &publishOptions{
		topic:            b.topic,
		exchange:         b.exchangeName,
		publishMandatory: b.publishMandatory,
	}
}

// PublishOption is a publish option.
type PublishOption func(*publishOptions)

// WithPublishingTopic is an option to set the publishing topic.
func WithPublishingTopic(topic string) PublishOption {
	return func(p *publishOptions) {
		p.topic = topic
	}
}

// WithPublishingExchange is an option to set the publishing exchange.
func WithPublishingExchange(name string) PublishOption {
	return func(p *publishOptions) {
		p.exchange = name
	}
}

// WithPublishingMandatory is an option to publish with or without mandatory flag.
// Enabled by default.
func WithPublishingMandatory(mandatory bool) PublishOption {
	return func(p *publishOptions) {
		p.publishMandatory = mandatory
	}
}
