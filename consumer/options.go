package consumer

import "time"

type Options struct {
	Brokers              []string
	Group                string
	Topic                string
	RetryDelays          []time.Duration
	PartitionWorkerCount int
	SessionTimeout       time.Duration
}
