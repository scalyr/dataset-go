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

package buffer

import (
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/maxatome/go-testdeep/helpers/tdsuite"
	"github.com/maxatome/go-testdeep/td"

	"github.com/scalyr/dataset-go/pkg/api/add_events"
)

type SuiteBuffer struct{}

func TestSuiteBuffer(t *testing.T) {
	td.NewT(t)
	tdsuite.Run(t, &SuiteBuffer{})
}

func loadJson(name string) string {
	dat, err := os.ReadFile("../../test/testdata/" + name)
	if err != nil {
		panic(err)
	}

	return strings.Join(strings.Fields(string(dat)), "")
}

func (s *SuiteBuffer) TestEmptyPayloadShouldFail(assert, require *td.T) {

	buffer, err := NewBuffer("id", nil, "token")
	assert.Nil(err)
	_, err = buffer.Payload()
	assert.CmpError(err, "there is no event")
}
func (s *SuiteBuffer) TestEmptyTokenShouldFail(assert, require *td.T) {

	buffer, err := NewBuffer("id", nil, "")
	assert.Nil(err)
	_, err = buffer.Payload()
	assert.CmpError(err, "token is missing")
}
func (s *SuiteBuffer) TestEmptySessionShouldFail(assert, require *td.T) {

	buffer, err := NewBuffer("", nil, "token")
	assert.Nil(err)
	_, err = buffer.Payload()
	assert.CmpError(err, "session is missing")
}
func (s *SuiteBuffer) TestPayloadFull(assert, require *td.T) {

	sessionInfo := &add_events.SessionInfo{
		ServerId:   "serverId",
		ServerType: "serverType",
		Region:     "region",
	}
	session := "session"
	token := "token"
	buffer, err := NewBuffer(
		session,
		sessionInfo,
		token)

	assert.Nil(err)

	bundle := &add_events.EventBundle{
		Log: &add_events.Log{Id: "LId", Attrs: map[string]interface{}{
			"LAttr1": "LVal1",
			"LAttr2": "LVal2",
		}},
		Thread: &add_events.Thread{Id: "TId", Name: "TName"},
		Event: &add_events.Event{
			Thread: "TId",
			Sev:    3,
			Ts:     "0",
			Attrs: map[string]interface{}{
				"message": "test",
				"meh":     1,
			},
		},
	}

	added, err := buffer.AddBundle(bundle)
	assert.Nil(err)
	assert.Cmp(added, Added)
	assert.Cmp(buffer.countLogs, 1)
	assert.Cmp(buffer.lenLogs, 57)
	assert.Cmp(buffer.countThreads, 1)
	assert.Cmp(buffer.lenThreads, 28)

	payload, err := buffer.Payload()
	assert.Nil(err)

	params := add_events.AddEventsRequestParams{}
	err = json.Unmarshal(payload, &params)
	assert.Nil(err)
	assert.Cmp(params.Session, session)
	assert.Cmp(params.Token, token)
	assert.Cmp(params.SessionInfo, sessionInfo)

	expected := loadJson("buffer_test_payload_full.json")

	assert.Cmp(len(payload), len(expected), "Length differs")

	upperBound := int(math.Min(float64(len(expected)), float64(len(payload))))
	for i := 0; i < upperBound; i++ {
		if expected[i] != (payload)[i] {
			assert.Cmp(payload[0:i], expected[0:i], "Pos: %d", i)
		}
	}
	assert.Cmp(payload, []byte(expected))
}
func (s *SuiteBuffer) TestPayloadInjection(assert, require *td.T) {

	sessionInfo := &add_events.SessionInfo{
		ServerId:   "serverId\",\"sI\":\"I",
		ServerType: "serverType\",\"sT\":\"T",
		Region:     "region\",\"r\":\"R",
	}
	session := "session\",\"s\":\"S"
	token := "token\",\"events\":[{}],\"foo\":\"bar"
	buffer, err := NewBuffer(
		session,
		sessionInfo,
		token)

	assert.Nil(err)
	bundle := &add_events.EventBundle{
		Log: &add_events.Log{
			Id: "LId\",\"i\":\"i",
			Attrs: map[string]interface{}{
				"LAttr1\",\"i\":\"i": "LVal1\",\"i\":\"i",
				"LAttr2\",\"i\":\"i": "LVal2\",\"i\":\"i",
			}},
		Thread: &add_events.Thread{Id: "TId\",\"i\":\"i", Name: "TName\",\"i\":\"i"},
		Event: &add_events.Event{
			Thread: "TId\",\"i\":\"i",
			Sev:    3,
			Ts:     "0",
			Attrs: map[string]interface{}{
				"message\",\"i\":\"i": "test\",\"i\":\"i",
				"meh\",\"i\":\"i":     1.0,
			},
		},
	}
	added, err := buffer.AddBundle(bundle)
	assert.Nil(err)
	assert.Cmp(added, Added)

	assert.Cmp(buffer.countLogs, 1)
	assert.Cmp(buffer.lenLogs, 117)

	assert.Cmp(buffer.countThreads, 1)
	assert.Cmp(buffer.lenThreads, 52)

	payload, err := buffer.Payload()
	assert.Nil(err)

	params := add_events.AddEventsRequestParams{}
	err = json.Unmarshal(payload, &params)
	assert.Nil(err)
	assert.Cmp(params.Session, session)
	assert.Cmp(params.Token, token)
	assert.Cmp(len(params.Events), 1)

	expected := loadJson("buffer_test_payload_injection.json")

	assert.Cmp(len(payload), len(expected), "Length differs")

	upperBound := int(math.Min(float64(len(expected)), float64(len(payload))))
	for i := 0; i < upperBound; i++ {
		if expected[i] != (payload)[i] {
			assert.Cmp(payload[0:i], expected[0:i], "Pos: %d", i)
		}
	}
	assert.Cmp(payload, []byte(expected))
}
