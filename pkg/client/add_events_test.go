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

package client

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/scalyr/dataset-go/pkg/api/add_events"
	"github.com/scalyr/dataset-go/pkg/config"
	"go.uber.org/zap"

	"github.com/jarcoal/httpmock"
	"github.com/maxatome/go-testdeep/helpers/tdsuite"
	"github.com/maxatome/go-testdeep/td"
)

type SuiteAddEvents struct{}

const RetryBase = time.Second

func (s *SuiteAddEvents) Setup(t *td.T) error {
	// block all HTTP requests
	httpmock.Activate()
	return nil
}

func (s *SuiteAddEvents) PostTest(t *td.T, testName string) error {
	// remove any mocks after each test
	httpmock.Reset()
	return nil
}

func (s *SuiteAddEvents) Destroy(t *td.T) error {
	httpmock.DeactivateAndReset()
	return nil
}

func TestSuiteAddEvents(t *testing.T) {
	td.NewT(t)
	tdsuite.Run(t, &SuiteAddEvents{})
}

func (s *SuiteAddEvents) TestArticles(assert, require *td.T) {
	httpmock.RegisterResponder("GET", "https://api.mybiz.com/articles.json",
		httpmock.NewStringResponder(200, `[{"id": 1, "name": "My Great Article"}]`))

	// do stuff that makes a request to articles.json
}

func extract(req *http.Request) (add_events.AddEventsRequest, error) {
	data, _ := io.ReadAll(req.Body)
	b := bytes.NewBuffer(data)
	reader, _ := gzip.NewReader(b)

	var resB bytes.Buffer
	_, _ = resB.ReadFrom(reader)

	cer := &add_events.AddEventsRequest{}
	err := json.Unmarshal(resB.Bytes(), cer)
	return *cer, err
}

func (s *SuiteAddEvents) TestAddEventsRetry(assert, require *td.T) {
	attempt := 0
	wasSuccesful := false
	const succeedInAttempt = 3
	httpmock.RegisterResponder(
		"POST",
		"https://example.com/api/addEvents",
		func(req *http.Request) (*http.Response, error) {
			attempt += 1
			cer, err := extract(req)

			assert.CmpNoError(err, "Error reading request: %v", err)
			assert.Cmp("b", cer.SessionInfo.ServerType)
			assert.Cmp("a", cer.SessionInfo.ServerId)

			if attempt < succeedInAttempt {
				return httpmock.NewJsonResponse(530, map[string]interface{}{
					"status":       "error",
					"bytesCharged": 42,
				})
			} else {
				wasSuccesful = true
				return httpmock.NewJsonResponse(200, map[string]interface{}{
					"status":       "success",
					"bytesCharged": 42,
				})
			}
		})

	config := &config.DataSetConfig{
		Endpoint:       "https://example.com",
		Tokens:         config.DataSetTokens{WriteLog: "AAAA"},
		MaxPayloadB:    20,
		MaxBufferDelay: 0,
		RetryBase:      RetryBase,
	}
	sc, _ := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()))

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{event1, &add_events.Thread{Id: "5", Name: "fred"}, nil}
	err := sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	sc.SendAllAddEventsBuffers()

	for i := 0; i < succeedInAttempt*succeedInAttempt; i++ {
		if !wasSuccesful {
			time.Sleep(RetryBase)
		} else {
			break
		}
	}
	assert.CmpNoError(err)
	info := httpmock.GetCallCountInfo()
	assert.CmpDeeply(info, map[string]int{"POST https://example.com/api/addEvents": succeedInAttempt})
}

func (s *SuiteAddEvents) TestAddEventsRetryAfterSec(assert, require *td.T) {
	attempt := 0
	wasSuccessful := false
	var now, expectedTime = time.Now(), time.Now()
	retryAfter := RetryBase * 3
	httpmock.RegisterResponder(
		"POST",
		"https://example.com/api/addEvents",
		func(req *http.Request) (*http.Response, error) {
			if attempt == 0 {
				now = time.Now().Truncate(time.Second)
				expectedTime = now.Add(retryAfter)
			} else {
				assert.Gt(time.Now(), expectedTime, "start: %s, after: %s, expected: %s", now.Format(time.RFC1123), retryAfter, expectedTime.Format(time.RFC1123))
			}
			attempt += 1

			if attempt < 2 {
				resp, errr := httpmock.NewJsonResponse(429, map[string]interface{}{
					"status":       "error",
					"bytesCharged": 42,
				})
				resp.Header.Set("Retry-After", fmt.Sprintf("%d", int(retryAfter.Seconds())))
				return resp, errr
			} else {
				wasSuccessful = true
				return httpmock.NewJsonResponse(200, map[string]interface{}{
					"status":       "success",
					"bytesCharged": 42,
				})
			}
		})

	config := &config.DataSetConfig{
		Endpoint:       "https://example.com",
		Tokens:         config.DataSetTokens{WriteLog: "AAAA"},
		MaxPayloadB:    20,
		MaxBufferDelay: 0,
		RetryBase:      RetryBase,
	}
	sc, _ := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()))

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{event1, &add_events.Thread{Id: "5", Name: "fred"}, nil}
	err1 := sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	sc.SendAllAddEventsBuffers()

	for {
		if !wasSuccessful {
			time.Sleep(RetryBase)
		} else {
			break
		}
	}
	assert.CmpNoError(err1)
	info1 := httpmock.GetCallCountInfo()
	assert.CmpDeeply(info1, map[string]int{"POST https://example.com/api/addEvents": 2})

	// send second request to make sure that nothing is blocked
	event2 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle2 := &add_events.EventBundle{event2, &add_events.Thread{Id: "5", Name: "fred"}, nil}
	err2 := sc.AddEvents([]*add_events.EventBundle{eventBundle2})
	sc.SendAllAddEventsBuffers()

	wasSuccessful = false
	for {
		if !wasSuccessful {
			time.Sleep(RetryBase)
		} else {
			break
		}
	}
	assert.CmpNoError(err2)
	info2 := httpmock.GetCallCountInfo()
	assert.CmpDeeply(info2, map[string]int{"POST https://example.com/api/addEvents": 3})

}

func (s *SuiteAddEvents) TestAddEventsRetryAfterTime(assert, require *td.T) {
	attempt := 0
	wasSuccessful := false
	var now, expectedTime = time.Now(), time.Now()
	retryAfter := RetryBase * 3
	httpmock.RegisterResponder(
		"POST",
		"https://example.com/api/addEvents",
		func(req *http.Request) (*http.Response, error) {
			if attempt == 0 {
				now = time.Now().Truncate(time.Second)
				expectedTime = now.Add(retryAfter)
			} else {
				assert.Gt(time.Now(), expectedTime, "start: %s, after: %s, expected: %s", now.Format(time.RFC1123), retryAfter, expectedTime.Format(time.RFC1123))
			}
			attempt += 1

			if attempt < 2 {
				resp, errr := httpmock.NewJsonResponse(429, map[string]interface{}{
					"status":       "error",
					"bytesCharged": 42,
				})
				resp.Header.Set("Retry-After", expectedTime.Format(time.RFC1123))
				return resp, errr
			} else {
				wasSuccessful = true
				return httpmock.NewJsonResponse(200, map[string]interface{}{
					"status":       "success",
					"bytesCharged": 42,
				})
			}
		})

	config := &config.DataSetConfig{
		Endpoint:       "https://example.com",
		Tokens:         config.DataSetTokens{WriteLog: "AAAA"},
		MaxPayloadB:    20,
		MaxBufferDelay: 0,
		RetryBase:      RetryBase,
	}
	sc, _ := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()))

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{event1, &add_events.Thread{Id: "5", Name: "fred"}, nil}
	err := sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	sc.SendAllAddEventsBuffers()

	for {
		if !wasSuccessful {
			time.Sleep(RetryBase)
		} else {
			break
		}
	}
	assert.CmpNoError(err)
	info := httpmock.GetCallCountInfo()
	assert.CmpDeeply(info, map[string]int{"POST https://example.com/api/addEvents": 2})
}

func (s *SuiteAddEvents) TestAddEventsLargeEvent(assert, require *td.T) {
	originalAttrs := make(map[string]interface{})
	for i, v := range []int{-10000, 5000, -1000, 100, 0, -100, 1000, -5000, 10000} {
		originalAttrs[fmt.Sprintf("%d", i)] = strings.Repeat(fmt.Sprintf("%d", i), 1000000+v)
	}

	attempt := 0
	wasSuccessful := false
	const succeedInAttempt = 3
	httpmock.RegisterResponder(
		"POST",
		"https://example.com/api/addEvents",
		func(req *http.Request) (*http.Response, error) {
			attempt += 1
			cer, err := extract(req)
			assert.CmpNoError(err, "Error reading request: %v", err)
			assert.Cmp("b", cer.SessionInfo.ServerType)
			assert.Cmp("a", cer.SessionInfo.ServerId)

			assert.Cmp(len(cer.Events), 1)
			wasAttrs := (cer.Events)[0].Attrs
			// if attributes were not modified, then we
			// should update test, so they are modified
			assert.Not(wasAttrs, originalAttrs)

			wasLengths := make(map[string]int)
			for k, v := range wasAttrs {
				if str, ok := v.(string); ok {
					wasLengths[k] = len(str)
				}
			}
			expectedLengths := map[string]int{
				"bundle_key": 32,
				"0":          990000,
				"7":          995000,
				"2":          999000,
				"5":          999900,
				"4":          1000000,
				"3":          1000100,
				"6":          241689,
			}

			expectedAttrs := map[string]interface{}{
				"bundle_key": "d41d8cd98f00b204e9800998ecf8427e",
				"0":          strings.Repeat("0", expectedLengths["0"]),
				"7":          strings.Repeat("7", expectedLengths["7"]),
				"2":          strings.Repeat("2", expectedLengths["2"]),
				"5":          strings.Repeat("5", expectedLengths["5"]),
				"4":          strings.Repeat("4", expectedLengths["4"]),
				"3":          strings.Repeat("3", expectedLengths["3"]),
				"6":          strings.Repeat("6", expectedLengths["6"]),
			}
			assert.Cmp(wasAttrs, expectedAttrs)
			assert.Cmp(wasLengths, expectedLengths)

			wasSuccessful = true
			return httpmock.NewJsonResponse(200, map[string]interface{}{
				"status":       "success",
				"bytesCharged": 42,
			})
		})

	config := &config.DataSetConfig{
		Endpoint:       "https://example.com",
		Tokens:         config.DataSetTokens{WriteLog: "AAAA"},
		MaxPayloadB:    20,
		MaxBufferDelay: 0,
		RetryBase:      RetryBase,
	}
	sc, _ := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()))

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: originalAttrs}
	eventBundle1 := &add_events.EventBundle{event1, &add_events.Thread{Id: "5", Name: "fred"}, nil}
	err := sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	sc.SendAllAddEventsBuffers()

	for i := 0; i < succeedInAttempt*succeedInAttempt; i++ {
		if !wasSuccessful {
			time.Sleep(RetryBase)
		} else {
			break
		}
	}
	assert.CmpNoError(err)
	info := httpmock.GetCallCountInfo()
	assert.CmpDeeply(info, map[string]int{"POST https://example.com/api/addEvents": 1})
}

func (s *SuiteAddEvents) TestAddEventsLargeEventThatNeedEscaping(assert, require *td.T) {
	originalAttrs := make(map[string]interface{})
	for i, v := range []int{-10000, 5000, -1000, 100, 0, -100, 1000, -5000, 10000} {
		originalAttrs[fmt.Sprintf("%d", i)] = strings.Repeat("\"", 1000000+v)
	}

	attempt := 0
	wasSuccessful := false
	const succeedInAttempt = 3
	httpmock.RegisterResponder(
		"POST",
		"https://example.com/api/addEvents",
		func(req *http.Request) (*http.Response, error) {
			attempt += 1
			cer, err := extract(req)
			assert.CmpNoError(err, "Error reading request: %v", err)
			assert.Cmp("b", cer.SessionInfo.ServerType)
			assert.Cmp("a", cer.SessionInfo.ServerId)

			assert.Cmp(len(cer.Events), 1)
			wasAttrs := (cer.Events)[0].Attrs
			// if attributes were not modified, then we
			// should update test, so they are modified
			assert.Not(wasAttrs, originalAttrs)

			wasLengths := make(map[string]int)
			for k, v := range wasAttrs {
				if str, ok := v.(string); ok {
					wasLengths[k] = len(str)
				}
			}
			expectedLengths := map[string]int{
				"bundle_key": 32,
				"0":          990000,
				"7":          995000,
				"2":          999000,
				"5":          6,
			}

			expectedAttrs := map[string]interface{}{
				"bundle_key": "d41d8cd98f00b204e9800998ecf8427e",
				"0":          strings.Repeat("\"", expectedLengths["0"]),
				"7":          strings.Repeat("\"", expectedLengths["7"]),
				"2":          strings.Repeat("\"", expectedLengths["2"]),
				"5":          strings.Repeat("\"", expectedLengths["5"]),
			}
			assert.Cmp(wasAttrs, expectedAttrs)
			assert.Cmp(wasLengths, expectedLengths)

			wasSuccessful = true
			return httpmock.NewJsonResponse(200, map[string]interface{}{
				"status":       "success",
				"bytesCharged": 42,
			})
		})

	config := &config.DataSetConfig{
		Endpoint:       "https://example.com",
		Tokens:         config.DataSetTokens{WriteLog: "AAAA"},
		MaxPayloadB:    20,
		MaxBufferDelay: 0,
		RetryBase:      RetryBase,
	}
	sc, _ := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()))

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: originalAttrs}
	eventBundle1 := &add_events.EventBundle{event1, &add_events.Thread{Id: "5", Name: "fred"}, nil}
	err := sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	sc.SendAllAddEventsBuffers()

	for i := 0; i < succeedInAttempt*succeedInAttempt; i++ {
		if !wasSuccessful {
			time.Sleep(RetryBase)
		} else {
			break
		}
	}
	assert.CmpNoError(err)
	info := httpmock.GetCallCountInfo()
	assert.CmpDeeply(info, map[string]int{"POST https://example.com/api/addEvents": 1})
}
