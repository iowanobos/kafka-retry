package consumer

import (
	"context"
	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	Consume(ctx context.Context, message kafka.Message) error
}

type Guard interface {
	IsStopRetry(message kafka.Message, err error, attempt int) bool
}
