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
	"fmt"
	"time"
)

const (
	MetadataInterval = time.Minute
	MinimalInterval  = time.Second
)

// GetMetadataFunc is the equivalent of Factory.CreateTraces.
type GetMetadataFunc func() (map[string]string, error)

type DataSetMetadataSettings struct {
	UserAgent   map[string]string
	Interval    time.Duration
	GetMetadata GetMetadataFunc
}

func NewDefaultDataSetMetadataSettings() DataSetMetadataSettings {
	return DataSetMetadataSettings{
		UserAgent:   make(map[string]string),
		Interval:    MetadataInterval,
		GetMetadata: nil,
	}
}

type DataSetMetadataSettingsOption func(*DataSetMetadataSettings) error

func WithUserAgent(userAgent map[string]string) DataSetMetadataSettingsOption {
	return func(c *DataSetMetadataSettings) error {
		c.UserAgent = userAgent
		return nil
	}
}

func WithInterval(interval time.Duration) DataSetMetadataSettingsOption {
	return func(c *DataSetMetadataSettings) error {
		c.Interval = interval
		return nil
	}
}

func WithGetMetadata(getMetadata GetMetadataFunc) DataSetMetadataSettingsOption {
	return func(c *DataSetMetadataSettings) error {
		c.GetMetadata = getMetadata
		return nil
	}
}

func New(opts ...DataSetMetadataSettingsOption) (*DataSetMetadataSettings, error) {
	cfg := &DataSetMetadataSettings{}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

func (cfg *DataSetMetadataSettings) Update(opts ...DataSetMetadataSettingsOption) (*DataSetMetadataSettings, error) {
	newCfg := *cfg
	for _, opt := range opts {
		if err := opt(&newCfg); err != nil {
			return &newCfg, err
		}
	}
	return &newCfg, nil
}

func (cfg *DataSetMetadataSettings) String() string {
	return fmt.Sprintf(
		"UserAgent: %s, Interval: %s",
		cfg.UserAgent,
		cfg.Interval,
	)
}

func (cfg *DataSetMetadataSettings) Validate() error {
	if cfg.Interval < MinimalInterval {
		return fmt.Errorf(
			"interval has value %s which is less than %s",
			cfg.Interval,
			MinimalInterval,
		)
	}

	return nil
}
