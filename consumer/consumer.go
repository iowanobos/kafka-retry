package consumer

import (
	"context"
	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	Consume(ctx context.Context, message kafka.Message) error
}

type Continuer interface {
	Continue(message kafka.Message, err error, attempt int) bool
}
