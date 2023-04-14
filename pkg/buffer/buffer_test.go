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
	"sync/atomic"
	"testing"
	"time"

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
	buffer, err := NewBuffer("id", "token", nil)
	assert.Nil(err)
	_, err = buffer.Payload()
	assert.CmpError(err, "there is no event")
}

func (s *SuiteBuffer) TestEmptyTokenShouldFail(assert, require *td.T) {
	buffer, err := NewBuffer("id", "", nil)
	assert.Nil(err)
	_, err = buffer.Payload()
	assert.CmpError(err, "token is missing")
}

func (s *SuiteBuffer) TestEmptySessionShouldFail(assert, require *td.T) {
	buffer, err := NewBuffer("", "token", nil)
	assert.Nil(err)
	_, err = buffer.Payload()
	assert.CmpError(err, "session is missing")
}

func createTestBundle() add_events.EventBundle {
	return add_events.EventBundle{
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
}

func createEmptyBuffer() *Buffer {
	sessionInfo := &add_events.SessionInfo{
		ServerId:   "serverId",
		ServerType: "serverType",
		Region:     "region",
	}
	session := "session"
	token := "token"
	buffer, err := NewBuffer(
		session,
		token,
		sessionInfo)
	if err != nil {
		return nil
	}
	return buffer
}

func (s *SuiteBuffer) TestPayloadFull(assert, require *td.T) {
	buffer := createEmptyBuffer()
	assert.NotNil(buffer)

	bundle := createTestBundle()
	added, err := buffer.AddBundle(&bundle)
	assert.Nil(err)
	assert.Cmp(added, Added)
	assert.Cmp(buffer.countLogs.Load(), int32(1))
	assert.Cmp(buffer.lenLogs.Load(), int32(57))
	assert.Cmp(buffer.countThreads.Load(), int32(1))
	assert.Cmp(buffer.lenThreads.Load(), int32(28))

	payload, err := buffer.Payload()
	assert.Nil(err)

	params := add_events.AddEventsRequest{}
	err = json.Unmarshal(payload, &params)
	assert.Nil(err)
	assert.Cmp(params.Session, buffer.Session)
	assert.Cmp(params.Token, buffer.Token)
	assert.Cmp(params.SessionInfo, buffer.sessionInfo)

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
		token,
		sessionInfo)

	assert.Nil(err)
	bundle := &add_events.EventBundle{
		Log: &add_events.Log{
			Id: "LId\",\"i\":\"i",
			Attrs: map[string]interface{}{
				"LAttr1\",\"i\":\"i": "LVal1\",\"i\":\"i",
				"LAttr2\",\"i\":\"i": "LVal2\",\"i\":\"i",
			},
		},
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

	assert.Cmp(buffer.countLogs.Load(), int32(1))
	assert.Cmp(buffer.lenLogs.Load(), int32(117))

	assert.Cmp(buffer.countThreads.Load(), int32(1))
	assert.Cmp(buffer.lenThreads.Load(), int32(52))

	payload, err := buffer.Payload()
	assert.Nil(err)

	params := add_events.AddEventsRequest{}
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

func (s *SuiteBuffer) TestAddEventWithShouldSendAge(assert, require *td.T) {
	buffer := createEmptyBuffer()
	assert.NotNil(buffer)

	finished := atomic.Int32{}
	// add events into buffer
	go func() {
		for i := 10; i < 100; i += 10 {
			bundle := createTestBundle()
			added, err := buffer.AddBundle(&bundle)
			assert.Nil(err)
			assert.Cmp(added, Added)
			time.Sleep(time.Duration(i) * time.Millisecond)
		}
		finished.Add(1)
	}()

	go func() {
		waited := 0
		for !buffer.ShouldSendAge(60 * time.Millisecond) {
			waited += 1
			time.Sleep(10 * time.Millisecond)
			require.Cmp(finished.Load(), int32(0))
			if waited > 10 {
				break
			}
		}
		assert.Gt(waited, 5)
		finished.Add(1)
	}()

	for finished.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Cmp(finished.Load(), int32(2))
}

func (s *SuiteBuffer) TestAddEventWithShouldSendSize(assert, require *td.T) {
	buffer := createEmptyBuffer()
	assert.NotNil(buffer)

	finished := atomic.Int32{}
	// add events into buffer
	go func() {
		for {
			bundle := createTestBundle()
			added, err := buffer.AddBundle(&bundle)
			assert.Nil(err)
			if added != Added {
				break
			}
			assert.Cmp(added, Added)
			time.Sleep(time.Microsecond)
		}
		finished.Add(1)
	}()

	go func() {
		waited := 0
		for !buffer.ShouldSendSize() {
			waited += 1
			time.Sleep(10 * time.Microsecond)
			if buffer.BufferLengths() > 10000 {
				break
			}
		}
		assert.Gt(waited, 5)
		finished.Add(1)
	}()

	for finished.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Cmp(finished.Load(), int32(2))
	assert.Gt(buffer.BufferLengths(), int32(ShouldSentBufferSize-1000))
}
