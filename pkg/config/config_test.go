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
	cfg5, err := New(
		WithEndpoint("https://fooOpt"),
		WithTokens(DataSetTokens{WriteLog: "writeLogOpt"}),
		WithMaxBufferDelay(3*time.Second),
		WithMaxPayloadB(int64(12345)),
		WithRetryBase(2*time.Minute),
		WithGroupBy([]string{"fooOpt", "barOpt"}),
		WithMaxRetries(int64(456)),
	)
	assert.CmpNoError(err)
	assert.Cmp(cfg5.Endpoint, "https://fooOpt")
	assert.Cmp(cfg5.Tokens, DataSetTokens{WriteLog: "writeLogOpt"})
	assert.Cmp(cfg5.MaxBufferDelay, 3*time.Second)
	assert.Cmp(cfg5.MaxPayloadB, int64(12345))
	assert.Cmp(cfg5.RetryBase, 2*time.Minute)
	assert.Cmp(cfg5.GroupBy, []string{"fooOpt", "barOpt"})
	assert.Cmp(cfg5.MaxRetries, int64(456))
}

func (s *SuiteConfig) TestDataConfigUpdate(assert, require *td.T) {
	cfg5, err := New(
		WithEndpoint("https://fooOpt1"),
		WithTokens(DataSetTokens{WriteLog: "writeLogOpt1"}),
		WithMaxBufferDelay(3*time.Second),
		WithMaxPayloadB(int64(123451)),
		WithRetryBase(2*time.Minute),
		WithGroupBy([]string{"fooOpt1", "barOpt1"}),
		WithMaxRetries(int64(456)),
	)
	assert.CmpNoError(err)
	assert.Cmp(cfg5.Endpoint, "https://fooOpt1")
	assert.Cmp(cfg5.Tokens, DataSetTokens{WriteLog: "writeLogOpt1"})
	assert.Cmp(cfg5.MaxBufferDelay, 3*time.Second)
	assert.Cmp(cfg5.MaxPayloadB, int64(123451))
	assert.Cmp(cfg5.RetryBase, 2*time.Minute)
	assert.Cmp(cfg5.GroupBy, []string{"fooOpt1", "barOpt1"})
	assert.Cmp(cfg5.MaxRetries, int64(456))

	cfg6, err := cfg5.Update(
		WithEndpoint("https://fooOpt2"),
		WithTokens(DataSetTokens{WriteLog: "writeLogOpt2"}),
		WithMaxBufferDelay(5*time.Second),
		WithMaxPayloadB(int64(54321)),
		WithRetryBase(4*time.Minute),
		WithGroupBy([]string{"fooOpt2", "barOpt2"}),
		WithMaxRetries(int64(678)),
	)
	assert.CmpNoError(err)

	// original config is unchanged
	assert.Cmp(cfg5.Endpoint, "https://fooOpt1")
	assert.Cmp(cfg5.Tokens, DataSetTokens{WriteLog: "writeLogOpt1"})
	assert.Cmp(cfg5.MaxBufferDelay, 3*time.Second)
	assert.Cmp(cfg5.MaxPayloadB, int64(123451))
	assert.Cmp(cfg5.RetryBase, 2*time.Minute)
	assert.Cmp(cfg5.GroupBy, []string{"fooOpt1", "barOpt1"})
	assert.Cmp(cfg5.MaxRetries, int64(456))

	// new config is changed
	assert.Cmp(cfg6.Endpoint, "https://fooOpt2")
	assert.Cmp(cfg6.Tokens, DataSetTokens{WriteLog: "writeLogOpt2"})
	assert.Cmp(cfg6.MaxBufferDelay, 5*time.Second)
	assert.Cmp(cfg6.MaxPayloadB, int64(54321))
	assert.Cmp(cfg6.RetryBase, 4*time.Minute)
	assert.Cmp(cfg6.GroupBy, []string{"fooOpt2", "barOpt2"})
	assert.Cmp(cfg6.MaxRetries, int64(678))
}
