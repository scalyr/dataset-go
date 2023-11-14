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
