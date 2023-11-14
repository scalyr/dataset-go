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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/stretchr/testify/assert"
)

var (
	meter = otel.Meter("test")
	tests = []struct {
		name  string
		meter *metric.Meter
	}{
		{
			name:  "no meter",
			meter: nil,
		},
		{
			name:  "with meter",
			meter: &meter,
		},
	}
)

func TestBuffersEnqueued(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			stats, err := NewStatistics(tt.meter)
			require.Nil(t, err)
			v := uint64(rand.Int())
			stats.BuffersEnqueuedAdd(v)
			assert.Equal(t, v, stats.BuffersEnqueued())
		})
	}
}

func TestBuffersProcessed(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			stats, err := NewStatistics(tt.meter)

			require.Nil(t, err)
			v := uint64(rand.Int())
			stats.BuffersProcessedAdd(v)
			assert.Equal(t, v, stats.BuffersProcessed())
		})
	}
}

func TestBuffersDropped(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			stats, err := NewStatistics(tt.meter)

			require.Nil(t, err)
			v := uint64(rand.Int())
			stats.BuffersDroppedAdd(v)
			assert.Equal(t, v, stats.BuffersDropped())
		})
	}
}

func TestBuffersBroken(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			stats, err := NewStatistics(tt.meter)
			require.Nil(t, err)
			v := uint64(rand.Int())
			stats.BuffersBrokenAdd(v)
			assert.Equal(t, v, stats.BuffersBroken())
		})
	}
}

func TestEventsEnqueued(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			stats, err := NewStatistics(tt.meter)
			require.Nil(t, err)
			v := uint64(rand.Int())
			stats.EventsEnqueuedAdd(v)
			assert.Equal(t, v, stats.EventsEnqueued())
		})
	}
}

func TestEventsProcessed(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			stats, err := NewStatistics(tt.meter)

			require.Nil(t, err)
			v := uint64(rand.Int())
			stats.EventsProcessedAdd(v)
			assert.Equal(t, v, stats.EventsProcessed())
		})
	}
}

func TestEventsDropped(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			stats, err := NewStatistics(tt.meter)

			require.Nil(t, err)
			v := uint64(rand.Int())
			stats.EventsDroppedAdd(v)
			assert.Equal(t, v, stats.EventsDropped())
		})
	}
}

func TestEventsBroken(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			stats, err := NewStatistics(tt.meter)
			require.Nil(t, err)
			v := uint64(rand.Int())
			stats.EventsBrokenAdd(v)
			assert.Equal(t, v, stats.EventsBroken())
		})
	}
}

func TestBytesAPISent(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			stats, err := NewStatistics(tt.meter)
			require.Nil(t, err)
			v := uint64(rand.Int())
			stats.BytesAPISentAdd(v)
			assert.Equal(t, v, stats.BytesAPISent())
		})
	}
}

func TestBytesAPIAccepted(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			stats, err := NewStatistics(tt.meter)
			require.Nil(t, err)
			v := uint64(rand.Int())
			stats.BytesAPIAcceptedAdd(v)
			assert.Equal(t, v, stats.BytesAPIAccepted())
		})
	}
}

func TestExport(t *testing.T) {
	stats, err := NewStatistics(nil)
	require.Nil(t, err)

	stats.BuffersEnqueuedAdd(1000)
	stats.BuffersProcessedAdd(100)
	stats.BuffersDroppedAdd(10)
	stats.BuffersBrokenAdd(1)

	stats.EventsEnqueuedAdd(2000)
	stats.EventsProcessedAdd(200)
	stats.EventsDroppedAdd(20)
	stats.EventsBrokenAdd(2)

	stats.BytesAPISentAdd(3000)
	stats.BytesAPIAcceptedAdd(300)

	exp := stats.Export(time.Second)
	assert.Equal(t, &ExportedStatistics{
		Buffers: QueueStats{
			enqueued:       1000,
			processed:      100,
			dropped:        10,
			broken:         1,
			processingTime: time.Second,
		},
		Events: QueueStats{
			enqueued:       2000,
			processed:      200,
			dropped:        20,
			broken:         2,
			processingTime: time.Second,
		},
		Transfer: TransferStats{
			bytesSent:        3000,
			bytesAccepted:    300,
			buffersProcessed: 100,
			processingTime:   time.Second,
		},
	}, exp)
}
