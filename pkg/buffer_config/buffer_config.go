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

package buffer_config

import (
	"time"

	"github.com/cenkalti/backoff/v4"
)

const (
	ShouldSentBufferSize = 5 * 1024 * 1024
	LimitBufferSize      = 5*1024*1024 + 960*1024
)

type DataSetBufferSettings struct {
	MaxLifetime              time.Duration
	MaxSize                  int
	GroupBy                  []string
	RetryRandomizationFactor float64
	RetryMultiplier          float64
	RetryInitialInterval     time.Duration
	RetryMaxInterval         time.Duration
	RetryMaxElapsedTime      time.Duration
}

func NewDefaultDataSetBufferSettings() DataSetBufferSettings {
	return DataSetBufferSettings{
		MaxLifetime:              5 * time.Second,
		MaxSize:                  LimitBufferSize,
		GroupBy:                  []string{},
		RetryInitialInterval:     5 * time.Second,
		RetryMaxInterval:         30 * time.Second,
		RetryMaxElapsedTime:      300 * time.Second,
		RetryRandomizationFactor: backoff.DefaultRandomizationFactor,
		RetryMultiplier:          backoff.DefaultMultiplier,
	}
}

type DataSetBufferSettingsOption func(*DataSetBufferSettings) error

func WithMaxLifetime(maxLifetime time.Duration) DataSetBufferSettingsOption {
	return func(c *DataSetBufferSettings) error {
		c.MaxLifetime = maxLifetime
		return nil
	}
}

func WithMaxSize(maxSize int) DataSetBufferSettingsOption {
	return func(c *DataSetBufferSettings) error {
		c.MaxSize = maxSize
		return nil
	}
}

func WithGroupBy(groupBy []string) DataSetBufferSettingsOption {
	return func(c *DataSetBufferSettings) error {
		c.GroupBy = groupBy
		return nil
	}
}

func WithRetryInitialInterval(retryInitialInterval time.Duration) DataSetBufferSettingsOption {
	return func(c *DataSetBufferSettings) error {
		c.RetryInitialInterval = retryInitialInterval
		return nil
	}
}

func WithRetryMultiplier(retryMultiplier float64) DataSetBufferSettingsOption {
	return func(c *DataSetBufferSettings) error {
		c.RetryMultiplier = retryMultiplier
		return nil
	}
}

func WithRetryRandomizationFactor(retryRandomizationFactor float64) DataSetBufferSettingsOption {
	return func(c *DataSetBufferSettings) error {
		c.RetryRandomizationFactor = retryRandomizationFactor
		return nil
	}
}

func WithRetryMaxInterval(retryMaxInterval time.Duration) DataSetBufferSettingsOption {
	return func(c *DataSetBufferSettings) error {
		c.RetryMaxInterval = retryMaxInterval
		return nil
	}
}

func WithRetryMaxElapsedTime(retryMaxElapsedTime time.Duration) DataSetBufferSettingsOption {
	return func(c *DataSetBufferSettings) error {
		c.RetryMaxElapsedTime = retryMaxElapsedTime
		return nil
	}
}

func New(opts ...DataSetBufferSettingsOption) (*DataSetBufferSettings, error) {
	cfg := &DataSetBufferSettings{}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

func (cfg *DataSetBufferSettings) Update(opts ...DataSetBufferSettingsOption) (*DataSetBufferSettings, error) {
	newCfg := *cfg
	for _, opt := range opts {
		if err := opt(&newCfg); err != nil {
			return &newCfg, err
		}
	}
	return &newCfg, nil
}
