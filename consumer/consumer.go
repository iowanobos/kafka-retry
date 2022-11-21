package consumer

import (
	"context"
	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	Consume(ctx context.Context, message kafka.Message) error
}

type ContinueParam struct {
	Message kafka.Message
	Error   error
	Attempt int
}

type Continuer interface {
	Continue(ctx context.Context, param ContinueParam) bool
}
