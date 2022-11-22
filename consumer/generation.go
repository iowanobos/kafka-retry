package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/iowanobos/kafka-retry/consumer/queue"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type generation struct {
	config
	gen      *kafka.Generation
	producer *kafka.Writer

	processedRecords     map[string]map[int]*processedRecords
	processedMessageChan chan kafka.Message

	workersWaitGroup  sync.WaitGroup
	commitLoopStopped chan struct{}
}

func (g *Group) newGeneration() (*generation, error) {
	gen, err := g.group.Next(g.gracefulShutdownCtx)
	if err != nil {
		return nil, err
	}

	records := make(map[string]map[int]*processedRecords, len(g.nextByTopic))
	for topic := range g.nextByTopic {
		records[topic] = make(map[int]*processedRecords)
	}

	producer := &kafka.Writer{
		Addr:         kafka.TCP(g.brokers...),
		Balancer:     &kafka.RoundRobin{},
		Async:        true,
		RequiredAcks: kafka.RequireAll,
	}

	return &generation{
		config:               g.config,
		gen:                  gen,
		producer:             producer,
		processedRecords:     records,
		processedMessageChan: make(chan kafka.Message),
		commitLoopStopped:    make(chan struct{}),
	}, nil
}

func (g *generation) Run(ctx context.Context, consumer Consumer) {
	ctx, cancel := context.WithCancel(ctx)

	for topic, partitions := range g.gen.Assignments {
		for _, partition := range partitions {
			messageChan := g.getMessageChan(topic, partition.ID, partition.Offset)

			g.workersWaitGroup.Add(g.workerCount)
			for i := 0; i < g.workerCount; i++ {
				logrus.WithFields(logrus.Fields{
					"topic":     topic,
					"partition": partition.ID,
					"worker":    i,
				}).Info("worker started")
				go func() {
					defer g.workersWaitGroup.Done()

					for message := range messageChan {
						m := headersToMap(message.Headers)

						if retryAt, hasRetryAt := m.GetRetryAt(); hasRetryAt {
							select {
							case <-ctx.Done(): // graceful shutdown
								return
							case <-time.After(time.Until(retryAt)): // wait
							}
						}

						if err := consumer.Consume(ctx, message); err != nil {
							g.retry(ctx, consumer, m, message, err)
						}

						select {
						case <-ctx.Done(): // graceful shutdown
							return
						case g.processedMessageChan <- message:
						}
					}
				}()
			}
		}
	}

	g.runCommitLoop(ctx)

	go func() {
		g.workersWaitGroup.Wait()
		logrus.Debug("shutdown. all the workers are stopped")
		cancel()
	}()

	<-g.commitLoopStopped
	logrus.Debug("shutdown. commit loop stopped")
}

func (g *generation) getMessageChan(topic string, partition int, offset int64) chan kafka.Message {
	messageChan := make(chan kafka.Message)

	g.gen.Start(func(ctx context.Context) {
		defer close(messageChan)

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:        g.brokers,
			Topic:          topic,
			Partition:      partition,
			MinBytes:       10e3,
			MaxBytes:       10e6,
			SessionTimeout: g.timeout,
		})
		defer reader.Close()

		if err := reader.SetOffset(offset); err != nil {
			return
		}

		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			return
		}

		g.processedRecords[topic][partition] = &processedRecords{ // init new partition processed records
			NextOffset:       msg.Offset,
			ProcessedOffsets: queue.New(),
		}

		select {
		case <-ctx.Done(): // graceful shutdown + retry-generation
			return
		case messageChan <- msg:
		}

		for {
			msg, err = reader.FetchMessage(ctx)
			if err != nil {
				return
			}

			select {
			case <-ctx.Done(): // graceful shutdown + retry-generation
				return
			case messageChan <- msg:
			}
		}
	})

	return messageChan
}

func (g *generation) runCommitLoop(ctx context.Context) {
	offsets := make(map[string]map[int]int64, len(g.processedRecords))
	for topic := range g.processedRecords {
		offsets[topic] = make(map[int]int64)
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done(): // graceful shutdown
				g.flushOffsets(offsets)
				close(g.commitLoopStopped)
				return
			case <-ticker.C:
				g.flushOffsets(offsets)
			case record := <-g.processedMessageChan:
				records, ok := g.processedRecords[record.Topic][record.Partition]
				if ok {
					records.Lock()
					if records.NextOffset == record.Offset {
						records.NextOffset++

						for records.ProcessedOffsets.Root() == records.NextOffset {
							records.ProcessedOffsets.Pop()
							records.NextOffset++
						}
					} else {
						records.ProcessedOffsets.Push(record.Offset)
					}
					records.Unlock()
				}
			}
		}
	}()
}

func (g *generation) flushOffsets(offsets map[string]map[int]int64) {
	for topic, partitions := range g.processedRecords {
		for partition, records := range partitions {
			offsets[topic][partition] = records.NextOffset
		}
	}

	if err := g.gen.CommitOffsets(offsets); err != nil {
		logrus.WithError(err).Warn("commit offsets failed")
	}
}

type processedRecords struct {
	sync.Mutex
	NextOffset       int64
	ProcessedOffsets *queue.Queue
}

func (g *generation) retry(ctx context.Context, consumer Consumer, m headerMap, message kafka.Message, err error) {
	attempt := m.GetAttempt()
	if continuer, hasContinuer := consumer.(Continuer); !hasContinuer || continuer.Continue(ctx,
		ContinueParam{
			Message: message,
			Error:   err,
			Attempt: attempt,
		}) {
		if next, ok := g.nextByTopic[message.Topic]; ok {
			m.SetAttempt(attempt + 1)
			nextRetryAt := time.Now().Add(next.Delay)
			m.SetRetryAt(nextRetryAt)

			message.Headers = m.ToHeaders()
			if err := g.producer.WriteMessages(ctx, kafka.Message{
				Topic:   next.Topic,
				Key:     message.Key,
				Value:   message.Value,
				Headers: m.ToHeaders(),
			}); err != nil {
				logrus.WithError(err).Warn("send retry failed")
			}
		}
	}
}
