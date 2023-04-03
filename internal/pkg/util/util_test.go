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

package util

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/maxatome/go-testdeep/helpers/tdsuite"
	"github.com/maxatome/go-testdeep/td"
)

type SuiteUtil struct{}

func (s *SuiteUtil) PreTest(t *td.T, testName string) error {
	os.Clearenv()
	return nil
}

func (s *SuiteUtil) PostTest(t *td.T, testName string) error {
	os.Clearenv()
	return nil
}

func (s *SuiteUtil) Destroy(t *td.T) error {
	os.Clearenv()
	return nil
}

func TestSuiteUtil(t *testing.T) {
	td.NewT(t)
	tdsuite.Run(t, &SuiteUtil{})
}

func (s *SuiteUtil) TestGetEnvWithDefaultIsSet(assert, require *td.T) {
	key := fmt.Sprintf("ENV_VARIABLE_%d", time.Now().Nanosecond())
	assert.Setenv(key, "foo")
	assert.Cmp(GetEnvWithDefault(key, "bar"), "foo")
}

func (s *SuiteUtil) TestGetEnvWithDefaultUseDefault(assert, require *td.T) {
	key := fmt.Sprintf("ENV_VARIABLE_%d", time.Now().Nanosecond())
	assert.Cmp(GetEnvWithDefault(key, "bar"), "bar")
}
