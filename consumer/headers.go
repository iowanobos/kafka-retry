package consumer

import (
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type RetryHeader = string

const (
	RetryHeaderAttempt RetryHeader = "X-Retry-Attempt"
	RetryHeaderRetryAt RetryHeader = "X-Retry-RetryAt"
)

type headerMap map[string][]byte

func headersToMap(headers []kafka.Header) headerMap {
	m := make(headerMap, len(headers))
	for _, header := range headers {
		m[header.Key] = header.Value
	}
	return m
}

func (m headerMap) ToHeaders() []kafka.Header {
	headers := make([]kafka.Header, 0, len(m))
	for key, value := range m {
		headers = append(headers, kafka.Header{
			Key:   key,
			Value: value,
		})
	}
	return headers
}

func (m headerMap) GetAttempt() int {
	attempt, _ := strconv.Atoi(string(m[RetryHeaderAttempt]))
	return attempt
}

func (m headerMap) SetAttempt(attempt int) {
	m[RetryHeaderAttempt] = []byte(strconv.Itoa(attempt))
}

func (m headerMap) GetRetryAt() (time.Time, bool) {
	value, ok := m[RetryHeaderRetryAt]
	if !ok {
		return time.Time{}, false
	}
	retryAt, _ := time.Parse(time.RFC3339, string(value))
	return retryAt, true
}

func (m headerMap) SetRetryAt(retryAt time.Time) {
	m[RetryHeaderRetryAt] = []byte(retryAt.Format(time.RFC3339))
}
