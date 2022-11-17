package consumer

import (
	"container/list"
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type Group struct {
	brokers              []string
	nextByTopic          map[string]nextTopic
	workerCount          int
	group                *kafka.ConsumerGroup
	generation           *kafka.Generation
	processedRecords     map[string]map[int]*processedRecords
	processedMessageChan chan kafka.Message
	producer             *kafka.Writer
	timeout              time.Duration
}

type nextTopic struct {
	Topic string
	Delay time.Duration
}

func NewGroup(ctx context.Context, options Options) (*Group, error) {
	topics := make([]string, 0, 1+len(options.RetryDelays))
	nextByTopic := getNextByTopic(options)
	if len(nextByTopic) > 0 {
		for topic := range nextByTopic {
			topics = append(topics, topic)
		}
	} else {
		topics = append(topics, options.Topic)
	}

	if err := createTopics(ctx, options.Brokers, topics); err != nil {
		return nil, err
	}

	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:             options.Group,
		Brokers:        options.Brokers,
		Topics:         topics,
		StartOffset:    kafka.FirstOffset,
		SessionTimeout: options.SessionTimeout,
	})
	if err != nil {
		return nil, err
	}

	generation, err := group.Next(ctx)
	if err != nil {
		return nil, err
	}

	records := make(map[string]map[int]*processedRecords, len(topics))
	for _, topic := range topics {
		records[topic] = make(map[int]*processedRecords)
	}

	producer := &kafka.Writer{
		Addr:         kafka.TCP(options.Brokers...),
		Balancer:     &kafka.RoundRobin{},
		Async:        true,
		RequiredAcks: kafka.RequireNone,
	}

	return &Group{
		brokers:              options.Brokers,
		nextByTopic:          nextByTopic,
		workerCount:          options.PartitionWorkerCount,
		group:                group,
		generation:           generation,
		processedRecords:     records,
		processedMessageChan: make(chan kafka.Message),
		producer:             producer,
		timeout:              options.SessionTimeout,
	}, nil
}

func createTopics(ctx context.Context, brokers, topics []string) error {
	topicsConfigs := make([]kafka.TopicConfig, len(topics))
	for i, topic := range topics {
		topicsConfigs[i] = kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
	}

	for _, broker := range brokers {
		conn, err := kafka.DialContext(ctx, "tcp", broker)
		if err != nil {
			return err
		}

		if err = conn.CreateTopics(topicsConfigs...); err != nil {
			log.Println("create topic failed. error: ", err.Error())
		} else {
			log.Println("create topic succeeded")
			break
		}
	}

	return nil
}

func getNextByTopic(options Options) map[string]nextTopic {
	delayTopicsNumber := len(options.RetryDelays)
	if delayTopicsNumber == 0 {
		return nil
	}

	nextByTopic := make(map[string]nextTopic, 1+delayTopicsNumber)

	currentTopic := options.Topic
	for _, delay := range options.RetryDelays {
		topic := options.Topic + "_retry_" + delay.String()
		nextByTopic[currentTopic] = nextTopic{
			Topic: topic,
			Delay: delay,
		}
		currentTopic = topic
	}

	// последняя нода зациклена сама на себя
	nextByTopic[currentTopic] = nextTopic{
		Topic: currentTopic,
		Delay: options.RetryDelays[delayTopicsNumber-1],
	}

	return nextByTopic
}

func (g *Group) Run(ctx context.Context, consumer Consumer) {
	var initCommitLoopWg, gracefulShutdownWg sync.WaitGroup

	guard, hasGuard := consumer.(Guard)

	for topic, partitions := range g.generation.Assignments {
		initCommitLoopWg.Add(len(partitions))
		for _, partition := range partitions {
			messageChan := g.getMessageChan(topic, partition.ID, partition.Offset, &initCommitLoopWg)

			gracefulShutdownWg.Add(g.workerCount)
			for i := 0; i < g.workerCount; i++ {
				go func(topic string, partition, workerID int) {
					log.Printf("Start. Topic: %s. Partition: %d. Worker: %d\n", topic, partition, workerID)
					for message := range messageChan {
						log.Printf("Got Message. LocalTopic: %s. LocalPartition: %d. MessageTopic: %s. MessagePartition: %d. MessageOffset: %d. Worker: %d\n", topic, partition, message.Topic, message.Partition, message.Offset, workerID)
						m := headersToMap(message.Headers)

						retryAt, hasRetryAt := m.GetRetryAt()
						if hasRetryAt {
							log.Printf("RetryAt: %s\n", retryAt.String())
							time.Sleep(time.Until(retryAt))
						}

						if err := consumer.Consume(message); err != nil {

							attempt := m.GetAttempt()
							if !hasGuard || !guard.IsStopRetry(message, attempt) {
								if next, ok := g.nextByTopic[message.Topic]; ok {
									m.SetAttempt(attempt + 1)
									nextRetryAt := time.Now().Add(next.Delay)
									m.SetRetryAt(nextRetryAt)
									log.Printf("Now: %s. NextRetryAt: %s. Delay: %s\n", time.Now().String(), nextRetryAt.String(), next.Delay.String())

									message.Headers = m.ToHeaders()
									if err = g.producer.WriteMessages(ctx, kafka.Message{
										Topic:   next.Topic,
										Key:     message.Key,
										Value:   message.Value,
										Headers: m.ToHeaders(),
									}); err != nil {
										log.Println("producer failed. error: ", err.Error())
									}
								}
							}
						}

						g.processedMessageChan <- message
					}

					gracefulShutdownWg.Done()
				}(topic, partition.ID, i+1)
			}
		}
	}

	initCommitLoopWg.Wait()
	g.runCommitLoop(ctx) // TODO: Может на запуститься если в какую-то партицию ничего не придет

	gracefulShutdownWg.Wait()
}

func (g *Group) Close() error {
	return g.group.Close()
}

func (g *Group) getMessageChan(topic string, partition int, offset int64, wg *sync.WaitGroup) chan kafka.Message {
	messageChan := make(chan kafka.Message)

	g.generation.Start(func(ctx context.Context) {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:        g.brokers,
			Topic:          topic,
			Partition:      partition,
			MinBytes:       10e3, // 10KB
			MaxBytes:       10e6, // 10MB
			SessionTimeout: g.timeout,
		})
		defer reader.Close()

		if err := reader.SetOffset(offset); err != nil {
			log.Println("set offset failed. error: ", err.Error())
			wg.Done()
			return
		}

		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Println("fetch message failed. error: ", err.Error())
			wg.Done()
			return
		}

		g.processedRecords[topic][partition] = &processedRecords{
			NextOffset:       msg.Offset,
			ProcessedOffsets: list.New(),
		}
		wg.Done()
		messageChan <- msg

		for {
			msg, err = reader.FetchMessage(ctx)
			if err != nil {
				log.Println("fetch message failed. error: ", err.Error())
				return
			}
			log.Printf("Fetch. Value: %s. Partition: %d. Offset: %d\n", string(msg.Value), msg.Partition, msg.Offset)

			select {
			case <-ctx.Done():
				log.Println("fetch shutdown")
				close(messageChan)
				return
			case messageChan <- msg:
			}
		}
	})

	return messageChan
}

func (g *Group) runCommitLoop(ctx context.Context) {
	offsets := make(map[string]map[int]int64, len(g.nextByTopic))
	for topic := range g.nextByTopic {
		offsets[topic] = make(map[int]int64)
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				log.Println("runCommitLoop shutdown")
				g.flushOffsets(ctx, offsets)
				return
			case <-ticker.C:
				g.flushOffsets(ctx, offsets)
			case record := <-g.processedMessageChan:
				records, ok := g.processedRecords[record.Topic][record.Partition]
				if ok {

					{
						// TODO: Удалить
						log.Printf("Processed. Partition: %d. Offset: %d. NextOffset: %d\n", record.Partition, record.Offset, records.NextOffset)
					}

					records.Lock()
					if records.NextOffset == record.Offset {
						records.NextOffset++

						for hasNextOffset(records.ProcessedOffsets, records.NextOffset) {
							records.NextOffset++
						}

					} else {
						insertionPush(records.ProcessedOffsets, record.Offset)
					}
					records.Unlock()
				}
			}
		}
	}()
}

func (g *Group) flushOffsets(ctx context.Context, offsets map[string]map[int]int64) {
	for topic, partitions := range g.processedRecords {
		for partition, records := range partitions {
			offsets[topic][partition] = records.NextOffset
		}
	}

	{
		// TODO: Удалить
		data, _ := json.Marshal(offsets)
		log.Println("flush offsets: ", string(data))
	}

	if err := g.generation.CommitOffsets(offsets); err != nil {
		log.Printf("commit offsets failed. error: %s\n", err.Error())

		generation, err := g.group.Next(ctx)
		if err != nil {
			log.Printf("get next failed. error: %s\n", err.Error())
		} else {
			g.generation = generation
		}
	}
}

func insertionPush(processedOffsets *list.List, offset int64) {
	{
		// TODO: Удалить
		log.Printf("cache size: %d\n", processedOffsets.Len())
	}

	elem := processedOffsets.Back()
	for {
		if elem == nil {
			processedOffsets.PushFront(offset)
			return
		}

		if value, ok := elem.Value.(int64); ok {
			if value > offset {
				elem = elem.Prev()
			} else {
				processedOffsets.InsertAfter(offset, elem)
				return
			}
		}
	}
}

func hasNextOffset(processedOffsets *list.List, offset int64) bool {
	elem := processedOffsets.Front()
	if elem != nil && elem.Value == offset {
		processedOffsets.Remove(elem)
		return true
	}
	return false
}

type processedRecords struct {
	sync.Mutex
	NextOffset       int64
	ProcessedOffsets *list.List
}
