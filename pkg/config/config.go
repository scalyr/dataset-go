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

package config

import (
	"time"

	"github.com/scalyr/dataset-go/internal/pkg/util"
)

type DataSetTokens struct {
	WriteLog    string
	ReadLog     string
	WriteConfig string
	ReadConfig  string
}

type DataSetConfig struct {
	Endpoint       string
	Tokens         DataSetTokens
	MaxBufferDelay time.Duration
	MaxPayloadB    int64
	GroupBy        []string
	RetryBase      time.Duration
}

type DataSetConfigOption func(*DataSetConfig) error

func WithEndpoint(endpoint string) DataSetConfigOption {
	return func(c *DataSetConfig) error {
		c.Endpoint = endpoint
		return nil
	}
}

func WithTokens(tokens DataSetTokens) DataSetConfigOption {
	return func(c *DataSetConfig) error {
		c.Tokens = tokens
		return nil
	}
}

func WithMaxBufferDelay(maxBufferDelay time.Duration) DataSetConfigOption {
	return func(c *DataSetConfig) error {
		c.MaxBufferDelay = maxBufferDelay
		return nil
	}
}

func WithMaxPayloadB(maxPayloadB int64) DataSetConfigOption {
	return func(c *DataSetConfig) error {
		c.MaxPayloadB = maxPayloadB
		return nil
	}
}

func WithRetryBase(retryBase time.Duration) DataSetConfigOption {
	return func(c *DataSetConfig) error {
		c.RetryBase = retryBase
		return nil
	}
}

func WithGroupBy(groupBy []string) DataSetConfigOption {
	return func(c *DataSetConfig) error {
		c.GroupBy = groupBy
		return nil
	}
}

func FromEnv() DataSetConfigOption {
	return func(c *DataSetConfig) error {
		if c.Tokens.WriteLog == "" {
			c.Tokens.WriteLog = util.GetEnvWithDefault("SCALYR_WRITELOG_TOKEN", "")
		}
		if c.Tokens.ReadLog == "" {
			c.Tokens.ReadLog = util.GetEnvWithDefault("SCALYR_READLOG_TOKEN", "")
		}
		if c.Tokens.ReadConfig == "" {
			c.Tokens.ReadConfig = util.GetEnvWithDefault("SCALYR_READCONFIG_TOKEN", "")
		}
		if c.Tokens.WriteConfig == "" {
			c.Tokens.WriteConfig = util.GetEnvWithDefault("SCALYR_WRITECONFIG_TOKEN", "")
		}
		if c.Endpoint == "" {
			c.Endpoint = util.GetEnvWithDefault("SCALYR_SERVER", "")
		}

		return nil
	}
}

func New(opts ...DataSetConfigOption) (*DataSetConfig, error) {
	cfg := &DataSetConfig{}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

func (cfg *DataSetConfig) Update(opts ...DataSetConfigOption) (*DataSetConfig, error) {
	newCfg := *cfg
	for _, opt := range opts {
		if err := opt(&newCfg); err != nil {
			return &newCfg, err
		}
	}
	return &newCfg, nil
}
