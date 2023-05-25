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
package metadata_config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfigWithOptions(t *testing.T) {
	// functions cannot be compared - https://github.com/stretchr/testify/issues/159
	// use nil instead
	metaCfg, errB := New(
		WithInterval(time.Second),
		WithUserAgent(map[string]string{"aa": "bb", "cc": "dd"}),
		WithGetMetadata(nil),
	)

	assert.Nil(t, errB)

	assert.Equal(t, DataSetMetadataSettings{
		Interval:    time.Second,
		UserAgent:   map[string]string{"aa": "bb", "cc": "dd"},
		GetMetadata: nil,
	}, *metaCfg)
}

func TestDataConfigUpdate(t *testing.T) {
	// functions cannot be compared - https://github.com/stretchr/testify/issues/159
	// use nil instead

	metaCfg, errB := New(
		WithInterval(time.Second),
		WithUserAgent(map[string]string{"aa": "bb", "cc": "dd"}),
		WithGetMetadata(nil),
	)
	assert.Nil(t, errB)

	assert.Equal(t, DataSetMetadataSettings{
		Interval:    time.Second,
		UserAgent:   map[string]string{"aa": "bb", "cc": "dd"},
		GetMetadata: nil,
	}, *metaCfg)

	metaCfg2, err := metaCfg.Update(
		WithInterval(time.Hour),
		WithUserAgent(map[string]string{"zz": "ZZ"}),
		WithGetMetadata(nil),
	)
	assert.Nil(t, err)

	// original config is unchanged
	assert.Equal(t, DataSetMetadataSettings{
		Interval:    time.Second,
		UserAgent:   map[string]string{"aa": "bb", "cc": "dd"},
		GetMetadata: nil,
	}, *metaCfg)

	// new config is changed
	assert.Equal(t, DataSetMetadataSettings{
		Interval:    time.Hour,
		UserAgent:   map[string]string{"zz": "ZZ"},
		GetMetadata: nil,
	}, *metaCfg2)
}

func TestDataConfigNewDefaultToString(t *testing.T) {
	cfg := NewDefaultDataSetMetadataSettings()
	assert.Equal(t, "UserAgent: map[], Interval: 1m0s", cfg.String())
}

func TestDataConfigNewDefaultIsValid(t *testing.T) {
	cfg := NewDefaultDataSetMetadataSettings()
	assert.Nil(t, cfg.Validate())
}
