package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/iowanobos/kafka-retry/consumer"
	"github.com/kelseyhightower/envconfig"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

const (
	configPrefix = "TEST"
)

type config struct {
	GroupID string `envconfig:"GROUP_ID" default:"groupID"`
	Brokers string `envconfig:"BROKERS" default:"localhost:9092"`
	Topic   string `envconfig:"TOPIC" default:"test"`
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	cfg := new(config)
	envconfig.MustProcess(configPrefix, cfg)

	group, err := consumer.NewGroup(ctx, consumer.Options{
		Brokers:              strings.Split(cfg.Brokers, ","),
		Group:                cfg.GroupID,
		Topic:                cfg.Topic,
		RetryDelays:          []time.Duration{time.Second * 15, time.Second * 30, time.Second * 60},
		PartitionWorkerCount: 10,
		SessionTimeout:       time.Second * 120,
	})
	if err != nil {
		log.Fatalln("create consumer group failed. error: ", err.Error())
	}

	var eg errgroup.Group
	eg.Go(func() error {
		if err := group.Run(new(errorConsumer)); err != nil {
			log.Fatalln("run consumer group failed. err: ", err.Error())
		}
		return nil
	})
	eg.Go(func() error {
		return RunProducer(ctx, cfg)
	})

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	select {
	case <-sigc:
		log.Println("Start shutdowning")
		cancel()
		if err := group.Close(); err != nil {
			log.Println("close consumer group failed. error: ", err.Error())
		}
		if err := eg.Wait(); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Println("Shutdown error: ", err.Error())
			}
		}
	}
	log.Println("Application shut downing...")
}

type errorConsumer struct{}

func (c *errorConsumer) Consume(_ context.Context, _ kafka.Message) error {
	if rand.Intn(10) > 3 {
		return errors.New("error")
	}
	return nil
}

func RunProducer(ctx context.Context, cfg *config) error {
	w := &kafka.Writer{
		Addr:     kafka.TCP(strings.Split(cfg.Brokers, ",")...),
		Topic:    cfg.Topic,
		Balancer: new(kafka.LeastBytes),
	}

	ticker := time.NewTicker(time.Millisecond * 100)
	for range ticker.C {

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			go func() {
				id := uuid.New().String()

				if err := w.WriteMessages(ctx, kafka.Message{Value: []byte(id)}); err != nil {
					return
				}
				//log.Printf("Write. Value: %s\n", id)
			}()
		}
	}

	return nil
}
