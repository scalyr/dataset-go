package client

import (
	"time"
)

type QueueStats struct {
	Processed   uint64  `mapstructure:"processed"`
	Enqueued    uint64  `mapstructure:"enqueued"`
	Dropped     uint64  `mapstructure:"dropped"`
	Broken      uint64  `mapstructure:"broken"`
	Waiting     uint64  `mapstructure:"waiting"`
	ProcessingS float64 `mapstructure:"processingS"`
}

type TransferStats struct {
	BytesSentMB     float64       `mapstructure:"bytesSentMB"`
	BytesAcceptedMB float64       `mapstructure:"bytesAcceptedMB"`
	ThroughputMBpS  float64       `mapstructure:"throughputMBpS"`
	PerBufferMB     float64       `mapstructure:"perBufferMB"`
	SuccessRate     float64       `mapstructure:"successRate"`
	ProcessingS     float64       `mapstructure:"processingS"`
	Processing      time.Duration `mapstructure:"processing"`
}

type Statistics struct {
	Buffers  QueueStats    `mapstructure:"buffers"`
	Events   QueueStats    `mapstructure:"events"`
	Transfer TransferStats `mapstructure:"transfer"`
}
