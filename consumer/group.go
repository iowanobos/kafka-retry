package consumer

import (
	"context"
	"errors"
	"github.com/sirupsen/logrus"
	"time"

	"github.com/segmentio/kafka-go"
)

type Group struct {
	config

	group *kafka.ConsumerGroup

	gracefulShutdownCtx context.Context
	gracefulShutdown    func()
}

type config struct {
	brokers     []string
	workerCount int
	timeout     time.Duration
	nextByTopic map[string]nextTopic
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

	ctx, cancel := context.WithCancel(context.Background())

	return &Group{
		config: config{
			brokers:     options.Brokers,
			workerCount: options.PartitionWorkerCount,
			timeout:     options.SessionTimeout,
			nextByTopic: nextByTopic,
		},
		group: group,

		gracefulShutdownCtx: ctx,
		gracefulShutdown:    cancel,
	}, nil
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

func createTopics(ctx context.Context, brokers, topics []string) error {
	topicsConfigs := make([]kafka.TopicConfig, len(topics))
	for i, topic := range topics {
		topicsConfigs[i] = kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
	}

	var err error
	for _, broker := range brokers {
		var conn *kafka.Conn
		conn, err = kafka.DialContext(ctx, "tcp", broker)
		if err != nil {
			return err
		}
		defer conn.Close() // TODO: вынести в метод

		if err = conn.CreateTopics(topicsConfigs...); err != nil {
			logrus.WithError(err).Warn("create topic failed")
		} else {
			logrus.Info("create topic succeeded")
			break
		}
	}

	return err
}

func (g *Group) Run(consumer Consumer) error {
	for {
		logrus.Info("try to connect")

		gen, err := g.newGeneration()
		if err != nil {
			if errors.Is(err, context.Canceled) { // graceful shutdown
				return nil
			}
			logrus.WithError(err).Warn("connect failed")
		} else {
			gen.Run(g.gracefulShutdownCtx, consumer)
		}

		select {
		case <-g.gracefulShutdownCtx.Done(): // graceful shutdown
			logrus.Debug("generation shutdown succeeded")
			return nil
		case <-time.After(3 * time.Second): // wait before reconnect
		}
	}
}

func (g *Group) Close() error {
	g.gracefulShutdown()
	return g.group.Close()
}
