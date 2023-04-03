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

package request

import (
	"testing"

	"github.com/maxatome/go-testdeep/helpers/tdsuite"
	"github.com/maxatome/go-testdeep/td"

	"github.com/scalyr/dataset-go/pkg/config"
)

type SuiteRequest struct{}

func TestSuiteRequest(t *testing.T) {
	td.NewT(t)
	tdsuite.Run(t, &SuiteRequest{})
}

func (s *SuiteRequest) TestMissingAuthJSONResponse(assert, require *td.T) {
	tokens := config.DataSetTokens{}
	r := NewRequest("GET", "/meh").WithWriteConfig(tokens).WithReadConfig(tokens).WithReadLog(tokens).WithWriteLog(tokens)
	_, err2 := r.HttpRequest()
	assert.NotNil(err2, "Should of gotten an error about missing authentication, got %s", r.supportedKeys)

	expectedAuthMethods := []string{"WriteConfig", "ReadConfig", "ReadLog", "WriteLog"}
	assert.Cmp(r.supportedKeys, expectedAuthMethods)
}

func (s *SuiteRequest) TestAuthOrderJSONResponse(assert, require *td.T) {
	tokens := config.DataSetTokens{WriteLog: "writeLog", ReadLog: "readLog", WriteConfig: "writeConfig", ReadConfig: "readConfig"}
	r := NewRequest("GET", "/meh").WithWriteConfig(tokens).WithReadConfig(tokens).WithReadLog(tokens).WithWriteLog(tokens)
	_, err := r.HttpRequest()
	assert.Nil(err, "Should not have gotten an error about missing authentication")
	assert.Cmp(r.apiKey, "writeConfig", "WriteConfig API Key should have been used")

	r = NewRequest("GET", "/meh").WithReadConfig(tokens).WithReadLog(tokens).WithWriteLog(tokens)
	_, err2 := r.HttpRequest()
	assert.Nil(err2)
	assert.Cmp(r.apiKey, "readConfig", "ReadConfig API Key should have been used")

	r = NewRequest("GET", "/meh").WithReadLog(tokens).WithWriteLog(tokens)
	_, err3 := r.HttpRequest()
	assert.Nil(err3)
	assert.Cmp(r.apiKey, "readLog", "ReadLog API Key should have been used")

	r = NewRequest("GET", "/meh").WithWriteLog(tokens)
	_, err4 := r.HttpRequest()
	assert.Nil(err4)
	assert.Cmp(r.apiKey, "writeLog", "WriteLog API Key should have been used")
}
