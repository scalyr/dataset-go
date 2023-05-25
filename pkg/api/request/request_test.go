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

	"github.com/scalyr/dataset-go/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestNewRequest(t *testing.T) {
	tokens := config.DataSetTokens{}
	r := NewRequest("GET", "/meh", "userAgent").WithWriteConfig(tokens).WithReadConfig(tokens).WithReadLog(tokens).WithWriteLog(tokens)
	assert.Equal(t, "GET", r.requestType)
	assert.Equal(t, "/meh", r.uri)
	assert.Equal(t, "userAgent", r.userAgent)
}

func TestMissingAuthJSONResponse(t *testing.T) {
	tokens := config.DataSetTokens{}
	r := NewRequest("GET", "/meh", "userAgent").WithWriteConfig(tokens).WithReadConfig(tokens).WithReadLog(tokens).WithWriteLog(tokens)
	_, err2 := r.HttpRequest()
	assert.NotNil(t, err2, "Should of gotten an error about missing authentication, got %s", r.supportedKeys)

	expectedAuthMethods := []string{"WriteConfig", "ReadConfig", "ReadLog", "WriteLog"}
	assert.Equal(t, expectedAuthMethods, r.supportedKeys)
}

func TestAuthOrderJSONResponse(t *testing.T) {
	tokens := config.DataSetTokens{WriteLog: "writeLog", ReadLog: "readLog", WriteConfig: "writeConfig", ReadConfig: "readConfig"}
	r := NewRequest("GET", "/meh", "userAgent").WithWriteConfig(tokens).WithReadConfig(tokens).WithReadLog(tokens).WithWriteLog(tokens)
	_, err := r.HttpRequest()
	assert.Nil(t, err, "Should not have gotten an error about missing authentication")
	assert.Equal(t, "writeConfig", r.apiKey, "WriteConfig API Key should have been used")

	r = NewRequest("GET", "/meh", "userAgent").WithReadConfig(tokens).WithReadLog(tokens).WithWriteLog(tokens)
	_, err2 := r.HttpRequest()
	assert.Nil(t, err2)
	assert.Equal(t, "readConfig", r.apiKey, "ReadConfig API Key should have been used")

	r = NewRequest("GET", "/meh", "userAgent").WithReadLog(tokens).WithWriteLog(tokens)
	_, err3 := r.HttpRequest()
	assert.Nil(t, err3)
	assert.Equal(t, "readLog", r.apiKey, "ReadLog API Key should have been used")

	r = NewRequest("GET", "/meh", "userAgent").WithWriteLog(tokens)
	_, err4 := r.HttpRequest()
	assert.Nil(t, err4)
	assert.Equal(t, "writeLog", r.apiKey, "WriteLog API Key should have been used")
}
