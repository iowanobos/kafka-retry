package consumer

import "github.com/segmentio/kafka-go"

type Consumer interface {
	Consume(message kafka.Message) error
}

type Guard interface {
	IsStopRetry(message kafka.Message, attempt int) bool
}
