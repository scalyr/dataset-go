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
	"os"
	"testing"
	"time"

	"github.com/maxatome/go-testdeep/helpers/tdsuite"
	"github.com/maxatome/go-testdeep/td"
)

type SuiteBufferSettings struct{}

func (s *SuiteBufferSettings) PreTest(t *td.T, testName string) error {
	os.Clearenv()
	return nil
}

func (s *SuiteBufferSettings) PostTest(t *td.T, testName string) error {
	os.Clearenv()
	return nil
}

func (s *SuiteBufferSettings) Destroy(t *td.T) error {
	os.Clearenv()
	return nil
}

func TestSuiteBufferSettings(t *testing.T) {
	td.NewT(t)
	tdsuite.Run(t, &SuiteBufferSettings{})
}

func (s *SuiteBufferSettings) TestConfigWithOptions(assert, require *td.T) {
	bufCfg, errB := New(
		WithMaxLifetime(3*time.Second),
		WithMaxSize(12345),
		WithGroupBy([]string{"aaa", "bbb"}),
		WithRetryInitialInterval(8*time.Second),
		WithRetryMaxInterval(30*time.Second),
		WithRetryMaxElapsedTime(10*time.Minute),
	)

	assert.CmpNoError(errB)

	assert.Cmp(*bufCfg, DataSetBufferSettings{
		MaxLifetime:          3 * time.Second,
		MaxSize:              12345,
		GroupBy:              []string{"aaa", "bbb"},
		RetryInitialInterval: 8 * time.Second,
		RetryMaxInterval:     30 * time.Second,
		RetryMaxElapsedTime:  10 * time.Minute,
	})
}

func (s *SuiteBufferSettings) TestDataConfigUpdate(assert, require *td.T) {
	bufCfg, errB := New(
		WithMaxLifetime(3*time.Second),
		WithMaxSize(12345),
		WithGroupBy([]string{"aaa", "bbb"}),
		WithRetryInitialInterval(8*time.Second),
		WithRetryMaxInterval(30*time.Second),
		WithRetryMaxElapsedTime(10*time.Minute),
	)
	assert.CmpNoError(errB)

	assert.Cmp(*bufCfg, DataSetBufferSettings{
		MaxLifetime:          3 * time.Second,
		MaxSize:              12345,
		GroupBy:              []string{"aaa", "bbb"},
		RetryInitialInterval: 8 * time.Second,
		RetryMaxInterval:     30 * time.Second,
		RetryMaxElapsedTime:  10 * time.Minute,
	})

	bufCfg2, err := bufCfg.Update(
		WithMaxLifetime(23*time.Second),
		WithMaxSize(212345),
		WithGroupBy([]string{"2aaa", "2bbb"}),
		WithRetryInitialInterval(28*time.Second),
		WithRetryMaxInterval(230*time.Second),
		WithRetryMaxElapsedTime(210*time.Minute),
	)
	assert.CmpNoError(err)

	// original config is unchanged
	assert.Cmp(*bufCfg, DataSetBufferSettings{
		MaxLifetime:          3 * time.Second,
		MaxSize:              12345,
		GroupBy:              []string{"aaa", "bbb"},
		RetryInitialInterval: 8 * time.Second,
		RetryMaxInterval:     30 * time.Second,
		RetryMaxElapsedTime:  10 * time.Minute,
	})

	// new config is changed
	assert.Cmp(*bufCfg2, DataSetBufferSettings{
		MaxLifetime:          23 * time.Second,
		MaxSize:              212345,
		GroupBy:              []string{"2aaa", "2bbb"},
		RetryInitialInterval: 28 * time.Second,
		RetryMaxInterval:     230 * time.Second,
		RetryMaxElapsedTime:  210 * time.Minute,
	})
}
