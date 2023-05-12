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
	"os"
	"testing"
	"time"

	"github.com/scalyr/dataset-go/pkg/buffer_config"

	"github.com/maxatome/go-testdeep/helpers/tdsuite"
	"github.com/maxatome/go-testdeep/td"
)

type SuiteConfig struct{}

func (s *SuiteConfig) PreTest(t *td.T, testName string) error {
	os.Clearenv()
	return nil
}

func (s *SuiteConfig) PostTest(t *td.T, testName string) error {
	os.Clearenv()
	return nil
}

func (s *SuiteConfig) Destroy(t *td.T) error {
	os.Clearenv()
	return nil
}

func TestSuiteConfig(t *testing.T) {
	td.NewT(t)
	tdsuite.Run(t, &SuiteConfig{})
}

func (s *SuiteConfig) TestDataConfigEmptyEnv(assert, require *td.T) {
	cfg1, err := New(FromEnv())
	assert.CmpNoError(err)
	assert.Cmp(cfg1.Endpoint, "")
}

func (s *SuiteConfig) TestDataConfigFromEnvEndpoint(assert, require *td.T) {
	assert.Setenv("SCALYR_SERVER", "http://test")
	cfg2, err := New(FromEnv())
	assert.CmpNoError(err)
	assert.Cmp(cfg2.Endpoint, "http://test")
}

func (s *SuiteConfig) TestDataConfigFromEnvTokens(assert, require *td.T) {
	assert.Setenv("SCALYR_WRITELOG_TOKEN", "writelog")
	assert.Setenv("SCALYR_READLOG_TOKEN", "readlog")
	assert.Setenv("SCALYR_WRITECONFIG_TOKEN", "writeconfig")
	assert.Setenv("SCALYR_READCONFIG_TOKEN", "readconfig")
	cfg4, err := New(FromEnv())
	assert.CmpNoError(err)
	assert.Cmp(cfg4.Tokens.ReadLog, "readlog")
	assert.Cmp(cfg4.Tokens.WriteLog, "writelog")
	assert.Cmp(cfg4.Tokens.ReadConfig, "readconfig")
	assert.Cmp(cfg4.Tokens.WriteConfig, "writeconfig")
}

func (s *SuiteConfig) TestDataConfigWithOptions(assert, require *td.T) {
	bufCfg, errB := buffer_config.New(
		buffer_config.WithMaxLifetime(3*time.Second),
		buffer_config.WithMaxSize(12345),
		buffer_config.WithGroupBy([]string{"aaa", "bbb"}),
		buffer_config.WithRetryInitialInterval(8*time.Second),
		buffer_config.WithRetryMaxInterval(30*time.Second),
		buffer_config.WithRetryMaxElapsedTime(10*time.Minute),
	)
	assert.CmpNoError(errB)
	cfg5, err := New(
		WithEndpoint("https://fooOpt"),
		WithTokens(DataSetTokens{WriteLog: "writeLogOpt"}),
		WithBufferSettings(*bufCfg),
	)
	assert.CmpNoError(err)
	assert.Cmp(cfg5.Endpoint, "https://fooOpt")
	assert.Cmp(cfg5.Tokens, DataSetTokens{WriteLog: "writeLogOpt"})
	assert.Cmp(cfg5.BufferSettings, buffer_config.DataSetBufferSettings{
		MaxLifetime:          3 * time.Second,
		MaxSize:              12345,
		GroupBy:              []string{"aaa", "bbb"},
		RetryInitialInterval: 8 * time.Second,
		RetryMaxInterval:     30 * time.Second,
		RetryMaxElapsedTime:  10 * time.Minute,
	})
}

func (s *SuiteConfig) TestDataConfigUpdate(assert, require *td.T) {
	bufCfg, errB := buffer_config.New(
		buffer_config.WithMaxLifetime(3*time.Second),
		buffer_config.WithMaxSize(12345),
		buffer_config.WithGroupBy([]string{"aaa", "bbb"}),
		buffer_config.WithRetryInitialInterval(8*time.Second),
		buffer_config.WithRetryMaxInterval(30*time.Second),
		buffer_config.WithRetryMaxElapsedTime(10*time.Minute),
	)
	assert.CmpNoError(errB)

	cfg5, err := New(
		WithEndpoint("https://fooOpt1"),
		WithTokens(DataSetTokens{WriteLog: "writeLogOpt1"}),
		WithBufferSettings(*bufCfg),
	)
	assert.CmpNoError(err)
	assert.Cmp(cfg5.Endpoint, "https://fooOpt1")
	assert.Cmp(cfg5.Tokens, DataSetTokens{WriteLog: "writeLogOpt1"})
	assert.Cmp(cfg5.BufferSettings, buffer_config.DataSetBufferSettings{
		MaxLifetime:          3 * time.Second,
		MaxSize:              12345,
		GroupBy:              []string{"aaa", "bbb"},
		RetryInitialInterval: 8 * time.Second,
		RetryMaxInterval:     30 * time.Second,
		RetryMaxElapsedTime:  10 * time.Minute,
	})

	bufCfg2, errB := buffer_config.New(
		buffer_config.WithMaxLifetime(23*time.Second),
		buffer_config.WithMaxSize(212345),
		buffer_config.WithGroupBy([]string{"2aaa", "2bbb"}),
		buffer_config.WithRetryInitialInterval(28*time.Second),
		buffer_config.WithRetryMaxInterval(230*time.Second),
		buffer_config.WithRetryMaxElapsedTime(210*time.Minute),
	)
	assert.CmpNoError(errB)

	cfg6, err := cfg5.Update(
		WithEndpoint("https://fooOpt2"),
		WithTokens(DataSetTokens{WriteLog: "writeLogOpt2"}),
		WithBufferSettings(*bufCfg2),
	)
	assert.CmpNoError(err)

	// original config is unchanged
	assert.Cmp(cfg5.Endpoint, "https://fooOpt1")
	assert.Cmp(cfg5.Tokens, DataSetTokens{WriteLog: "writeLogOpt1"})
	assert.Cmp(cfg5.BufferSettings, buffer_config.DataSetBufferSettings{
		MaxLifetime:          3 * time.Second,
		MaxSize:              12345,
		GroupBy:              []string{"aaa", "bbb"},
		RetryInitialInterval: 8 * time.Second,
		RetryMaxInterval:     30 * time.Second,
		RetryMaxElapsedTime:  10 * time.Minute,
	})

	// new config is changed
	assert.Cmp(cfg6.Endpoint, "https://fooOpt2")
	assert.Cmp(cfg6.Tokens, DataSetTokens{WriteLog: "writeLogOpt2"})
	assert.Cmp(cfg6.BufferSettings, buffer_config.DataSetBufferSettings{
		MaxLifetime:          23 * time.Second,
		MaxSize:              212345,
		GroupBy:              []string{"2aaa", "2bbb"},
		RetryInitialInterval: 28 * time.Second,
		RetryMaxInterval:     230 * time.Second,
		RetryMaxElapsedTime:  210 * time.Minute,
	})
}
