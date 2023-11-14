/*
 * Copyright 2023 SentinelOne, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package statistics

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/scalyr/dataset-go/pkg/buffer_config"

	"go.opentelemetry.io/otel/metric"
)

func key(key string) string {
	return "dataset." + key
}

type Statistics struct {
	buffersEnqueued  atomic.Uint64
	buffersProcessed atomic.Uint64
	buffersDropped   atomic.Uint64
	buffersBroken    atomic.Uint64

	eventsEnqueued  atomic.Uint64
	eventsProcessed atomic.Uint64
	eventsDropped   atomic.Uint64
	eventsBroken    atomic.Uint64

	bytesAPISent     atomic.Uint64
	bytesAPIAccepted atomic.Uint64

	meter *metric.Meter

	cBuffersEnqueued  metric.Int64UpDownCounter
	cBuffersProcessed metric.Int64UpDownCounter
	cBuffersDropped   metric.Int64UpDownCounter
	cBuffersBroken    metric.Int64UpDownCounter

	cEventsEnqueued  metric.Int64UpDownCounter
	cEventsProcessed metric.Int64UpDownCounter
	cEventsDropped   metric.Int64UpDownCounter
	cEventsBroken    metric.Int64UpDownCounter

	cBytesAPISent     metric.Int64UpDownCounter
	cBytesAPIAccepted metric.Int64UpDownCounter

	hPayloadSize  metric.Int64Histogram
	hResponseTime metric.Int64Histogram
}

func NewStatistics(meter *metric.Meter) (*Statistics, error) {
	statistics := &Statistics{
		buffersEnqueued:  atomic.Uint64{},
		buffersProcessed: atomic.Uint64{},
		buffersDropped:   atomic.Uint64{},
		buffersBroken:    atomic.Uint64{},

		eventsEnqueued:  atomic.Uint64{},
		eventsProcessed: atomic.Uint64{},
		eventsDropped:   atomic.Uint64{},
		eventsBroken:    atomic.Uint64{},

		bytesAPIAccepted: atomic.Uint64{},
		bytesAPISent:     atomic.Uint64{},

		meter: meter,
	}

	err := statistics.initMetrics(meter)

	return statistics, err
}

func (stats *Statistics) initMetrics(meter *metric.Meter) error {
	// if there is no meter, there is no need to initialise counters
	if meter == nil {
		return nil
	}

	stats.meter = meter

	err := error(nil)
	stats.cBuffersEnqueued, err = (*meter).Int64UpDownCounter(key("buffersEnqueued"))
	if err != nil {
		return err
	}
	stats.cBuffersProcessed, err = (*meter).Int64UpDownCounter(key("buffersProcessed"))
	if err != nil {
		return err
	}
	stats.cBuffersDropped, err = (*meter).Int64UpDownCounter(key("buffersDropped"))
	if err != nil {
		return err
	}
	stats.cBuffersBroken, err = (*meter).Int64UpDownCounter(key("buffersBroken"))
	if err != nil {
		return err
	}

	stats.cEventsEnqueued, err = (*meter).Int64UpDownCounter(key("eventsEnqueued"))
	if err != nil {
		return err
	}
	stats.cEventsProcessed, err = (*meter).Int64UpDownCounter(key("eventsProcessed"))
	if err != nil {
		return err
	}
	stats.cEventsDropped, err = (*meter).Int64UpDownCounter(key("eventsDropped"))
	if err != nil {
		return err
	}
	stats.cEventsBroken, err = (*meter).Int64UpDownCounter(key("eventsBroken"))
	if err != nil {
		return err
	}

	stats.cBytesAPISent, err = (*meter).Int64UpDownCounter(key("bytesAPISent"))
	if err != nil {
		return err
	}
	stats.cBytesAPIAccepted, err = (*meter).Int64UpDownCounter(key("bytesAPIAccepted"))
	if err != nil {
		return err
	}

	var payloadBuckets []float64
	for _, r := range [11]float64{0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 0.9, 0.95, 1.0, 1.1, 2} {
		payloadBuckets = append(payloadBuckets, r*buffer_config.LimitBufferSize)
	}
	stats.hPayloadSize, err = (*meter).Int64Histogram(key(
		"payloadSize"),
		metric.WithExplicitBucketBoundaries(payloadBuckets...),
		metric.WithUnit("b"),
	)
	if err != nil {
		return err
	}

	var responseBuckets []float64
	for i := 0; i < 12; i++ {
		responseBuckets = append(responseBuckets, float64(4*2^i))
	}
	stats.hResponseTime, err = (*meter).Int64Histogram(key(
		"responseTime"),
		metric.WithExplicitBucketBoundaries(responseBuckets...),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return err
	}

	return err
}

func (stats *Statistics) BuffersEnqueued() uint64 {
	return stats.buffersEnqueued.Load()
}

func (stats *Statistics) BuffersProcessed() uint64 {
	return stats.buffersProcessed.Load()
}

func (stats *Statistics) BuffersDropped() uint64 {
	return stats.buffersDropped.Load()
}

func (stats *Statistics) BuffersBroken() uint64 {
	return stats.buffersBroken.Load()
}

func (stats *Statistics) EventsEnqueued() uint64 {
	return stats.eventsEnqueued.Load()
}

func (stats *Statistics) EventsProcessed() uint64 {
	return stats.eventsProcessed.Load()
}

func (stats *Statistics) EventsDropped() uint64 {
	return stats.eventsDropped.Load()
}

func (stats *Statistics) EventsBroken() uint64 {
	return stats.eventsBroken.Load()
}

func (stats *Statistics) BuffersEnqueuedAdd(i uint64) {
	stats.buffersEnqueued.Add(i)
	stats.add(stats.cBuffersEnqueued, i)
}

func (stats *Statistics) BuffersProcessedAdd(i uint64) {
	stats.buffersProcessed.Add(i)
	stats.add(stats.cBuffersProcessed, i)
}

func (stats *Statistics) BuffersDroppedAdd(i uint64) {
	stats.buffersDropped.Add(i)
	stats.add(stats.cBuffersDropped, i)
}

func (stats *Statistics) BuffersBrokenAdd(i uint64) {
	stats.buffersBroken.Add(i)
	stats.add(stats.cBuffersBroken, i)
}

func (stats *Statistics) EventsEnqueuedAdd(i uint64) {
	stats.eventsEnqueued.Add(i)
	stats.add(stats.cEventsEnqueued, i)
}

func (stats *Statistics) EventsProcessedAdd(i uint64) {
	stats.eventsProcessed.Add(i)
	stats.add(stats.cEventsProcessed, i)
}

func (stats *Statistics) EventsDroppedAdd(i uint64) {
	stats.eventsDropped.Add(i)
	stats.add(stats.cEventsDropped, i)
}

func (stats *Statistics) EventsBrokenAdd(i uint64) {
	stats.eventsBroken.Add(i)
	stats.add(stats.cEventsBroken, i)
}

func (stats *Statistics) BytesAPISentAdd(i uint64) {
	stats.bytesAPISent.Add(i)
	stats.add(stats.cBytesAPISent, i)
}

func (stats *Statistics) BytesAPIAcceptedAdd(i uint64) {
	stats.bytesAPIAccepted.Add(i)
	stats.add(stats.cBytesAPIAccepted, i)
}

func (stats *Statistics) PayloadSizeRecord(payloadSizeInBytes int64) {
	if stats.hPayloadSize != nil {
		stats.hPayloadSize.Record(context.Background(), payloadSizeInBytes)
	}
}

func (stats *Statistics) ResponseTimeRecord(duration time.Duration) {
	if stats.hResponseTime != nil {
		stats.hResponseTime.Record(context.Background(), duration.Milliseconds())
	}
}

func (stats *Statistics) add(counter metric.Int64UpDownCounter, i uint64) {
	if counter != nil {
		counter.Add(context.Background(), int64(i))
	}
}

func (stats *Statistics) Export(processingDur time.Duration) *ExportedStatistics {
	// log buffer stats
	bProcessed := stats.buffersProcessed.Load()
	bEnqueued := stats.buffersEnqueued.Load()
	bDropped := stats.buffersDropped.Load()
	bBroken := stats.buffersBroken.Load()

	buffersStats := QueueStats{
		bEnqueued,
		bProcessed,
		bDropped,
		bBroken,
		processingDur,
	}

	// log events stats
	eProcessed := stats.eventsProcessed.Load()
	eEnqueued := stats.eventsEnqueued.Load()
	eDropped := stats.eventsDropped.Load()
	eBroken := stats.eventsBroken.Load()

	eventsStats := QueueStats{
		eEnqueued,
		eProcessed,
		eDropped,
		eBroken,
		processingDur,
	}

	// log transferred stats
	bAPISent := stats.bytesAPISent.Load()
	bAPIAccepted := stats.bytesAPIAccepted.Load()
	transferStats := TransferStats{
		bAPISent,
		bAPIAccepted,
		bProcessed,
		processingDur,
	}

	return &ExportedStatistics{
		Buffers:  buffersStats,
		Events:   eventsStats,
		Transfer: transferStats,
	}
}

// QueueStats stores statistics related to the queue processing
type QueueStats struct {
	// enqueued is number of items that has been accepted for processing
	enqueued uint64 `mapstructure:"enqueued"`
	// Processed is number of items that has been successfully processed
	processed uint64 `mapstructure:"processed"`
	// dropped is number of items that has been dropped since they couldn't be processed
	dropped uint64 `mapstructure:"dropped"`
	// broken is number of items that has been damaged by queue, should be zero
	broken uint64 `mapstructure:"broken"`
	// processingTime is duration of the processing
	processingTime time.Duration `mapstructure:"processingTime"`
}

// Enqueued is number of items that has been accepted for processing
func (stats QueueStats) Enqueued() uint64 {
	return stats.enqueued
}

// Processed is number of items that has been successfully processed
func (stats QueueStats) Processed() uint64 {
	return stats.processed
}

// Dropped is number of items that has been dropped since they couldn't be processed
func (stats QueueStats) Dropped() uint64 {
	return stats.dropped
}

// Broken is number of items that has been damaged by queue, should be zero
func (stats QueueStats) Broken() uint64 {
	return stats.broken
}

// Waiting is number of items that are waiting for being processed
// Enqueued - Processed - Dropped - Broken
func (stats QueueStats) Waiting() uint64 {
	return stats.enqueued - stats.processed - stats.dropped - stats.broken
}

// SuccessRate of items processing
// (Processed - Dropped - Broken) / Processed
func (stats QueueStats) SuccessRate() float64 {
	if stats.processed > 0 {
		return float64(stats.processed-stats.dropped-stats.broken) / float64(stats.processed)
	} else {
		return 0.0
	}
}

// ProcessingTime is duration of the processing
func (stats QueueStats) ProcessingTime() time.Duration {
	return stats.processingTime
}

// TransferStats stores statistics related to the data transfers
type TransferStats struct {
	// bytesSent is the amount of bytes that were sent to the server
	// each retry is counted
	bytesSent uint64 `mapstructure:"bytesSentMB"`
	// bytesAccepted is the amount of MB that were accepted by the server
	// retries are not counted
	bytesAccepted uint64 `mapstructure:"bytesAcceptedMB"`
	// buffersProcessed is number of processed buffers
	buffersProcessed uint64
	// processingTime is duration of the processing
	processingTime time.Duration `mapstructure:"processingTime"`
}

// BytesSent is the amount of bytes that were sent to the server
// each retry is counted
func (stats TransferStats) BytesSent() uint64 {
	return stats.bytesSent
}

// BytesAccepted is the amount of MB that were accepted by the server
// retries are not counted
func (stats TransferStats) BytesAccepted() uint64 {
	return stats.bytesAccepted
}

// BuffersProcessed is number of processed buffers
func (stats TransferStats) BuffersProcessed() uint64 {
	return stats.buffersProcessed
}

// ThroughputBpS is the throughput based on BytesAccepted
func (stats TransferStats) ThroughputBpS() float64 {
	return float64(stats.bytesAccepted) / stats.processingTime.Seconds()
}

// SuccessRate of the transfer - BytesAccepted / BytesSent
func (stats TransferStats) SuccessRate() float64 {
	if stats.bytesSent > 0 {
		return float64(stats.bytesAccepted) / float64(stats.bytesSent)
	} else {
		return 0.0
	}
}

// AvgBufferBytes is average buffer size in bytes - BytesAccepted / BuffersProcessed
func (stats TransferStats) AvgBufferBytes() float64 {
	if stats.buffersProcessed > 0 {
		return float64(stats.bytesAccepted) / float64(stats.buffersProcessed)
	} else {
		return 0
	}
}

// ProcessingTime is duration of the processing
func (stats TransferStats) ProcessingTime() time.Duration {
	return stats.processingTime
}

// Statistics store statistics about queues and transferred data
// These are statistics from the beginning of the processing
type ExportedStatistics struct {
	// Events stores statistics about processing events
	Events QueueStats `mapstructure:"events"`
	// Buffers stores statistics about processing buffers
	Buffers QueueStats `mapstructure:"buffers"`
	// Transfer stores statistics about data transfers
	Transfer TransferStats `mapstructure:"transfer"`
}
