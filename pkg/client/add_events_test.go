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
	"sync/atomic"
	"testing"
	"time"

	"github.com/scalyr/dataset-go/pkg/buffer_config"

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
	attempt := atomic.Int32{}
	attempt.Store(0)
	wasSuccessful := atomic.Bool{}
	wasSuccessful.Store(false)
	const succeedInAttempt = 3
	httpmock.RegisterResponder(
		"POST",
		"https://example.com/api/addEvents",
		func(req *http.Request) (*http.Response, error) {
			attempt.Add(1)
			cer, err := extract(req)

			assert.CmpNoError(err, "Error reading request: %v", err)
			assert.Cmp("b", cer.SessionInfo.ServerType)
			assert.Cmp("a", cer.SessionInfo.ServerId)

			if attempt.Load() < succeedInAttempt {
				return httpmock.NewJsonResponse(530, map[string]interface{}{
					"status":       "error",
					"bytesCharged": 42,
				})
			} else {
				wasSuccessful.Store(true)
				return httpmock.NewJsonResponse(200, map[string]interface{}{
					"status":       "success",
					"bytesCharged": 42,
				})
			}
		})

	config := &config.DataSetConfig{
		Endpoint: "https://example.com",
		Tokens:   config.DataSetTokens{WriteLog: "AAAA"},
		BufferSettings: buffer_config.DataSetBufferSettings{
			MaxSize:              20,
			MaxLifetime:          0,
			RetryInitialInterval: RetryBase,
		},
	}
	sc, _ := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()))

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err := sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	assert.CmpNoError(err)
	err = sc.Finish()
	assert.CmpNoError(err)

	assert.True(wasSuccessful.Load())
	assert.CmpNoError(err)
	info := httpmock.GetCallCountInfo()
	assert.CmpDeeply(info, map[string]int{"POST https://example.com/api/addEvents": succeedInAttempt})
}

func (s *SuiteAddEvents) TestAddEventsRetryAfterSec(assert, require *td.T) {
	attempt := atomic.Int32{}
	attempt.Store(0)
	wasSuccessful := atomic.Bool{}
	wasSuccessful.Store(false)
	now := atomic.Int64{}
	now.Store(time.Now().UnixNano())
	expectedTime := atomic.Int64{}
	expectedTime.Store(time.Now().UnixNano())

	retryAfter := (RetryBase * 3).Nanoseconds()
	httpmock.RegisterResponder(
		"POST",
		"https://example.com/api/addEvents",
		func(req *http.Request) (*http.Response, error) {
			if attempt.Load() == 0 {
				now.Store(time.Now().Truncate(time.Second).UnixNano())
				expectedTime.Store(now.Load() + retryAfter)
			} else {
				assert.Gt(time.Now().UnixNano(), expectedTime.Load(), "start: %s, after: %s, expected: %s", time.Unix(0, now.Load()).Format(time.RFC1123), retryAfter, time.Unix(0, expectedTime.Load()).Format(time.RFC1123))
			}
			attempt.Add(1)
			cer, err := extract(req)
			assert.CmpNoError(err, "Error reading request: %v", err)
			msg := cer.Events[0].Attrs["message"].(string)
			if attempt.Load() < 2 {
				assert.Cmp(msg, "test - 1")
				resp, errr := httpmock.NewJsonResponse(429, map[string]interface{}{
					"status":       "error",
					"bytesCharged": 42,
				})
				resp.Header.Set("Retry-After", fmt.Sprintf("%d", int(time.Duration(retryAfter).Seconds())))
				return resp, errr
			} else {
				if attempt.Load() == 2 {
					assert.Cmp(msg, "test - 1")
				} else if attempt.Load() == 3 {
					assert.Cmp(msg, "test - 22")
				} else {
					// this function should be called 3
					assert.Nil(msg, "Attempt: %d", attempt.Load())
				}
				wasSuccessful.Store(true)
				return httpmock.NewJsonResponse(200, map[string]interface{}{
					"status":       "success",
					"bytesCharged": 42,
				})
			}
		})

	config := &config.DataSetConfig{
		Endpoint: "https://example.com",
		Tokens:   config.DataSetTokens{WriteLog: "AAAA"},
		BufferSettings: buffer_config.DataSetBufferSettings{
			MaxSize:              20,
			MaxLifetime:          0,
			RetryInitialInterval: RetryBase,
		},
	}
	sc, _ := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()))

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err1 := sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	time.Sleep(RetryBase)
	sc.SendAllAddEventsBuffers()

	// wait for processing
	for i := 0; i < 10; i++ {
		if wasSuccessful.Load() {
			break
		}
		time.Sleep(RetryBase)
	}

	assert.True(wasSuccessful.Load())
	assert.Cmp(attempt.Load(), int32(2))
	assert.CmpNoError(err1)
	assert.CmpNoError(sc.LastError())
	info1 := httpmock.GetCallCountInfo()
	assert.CmpDeeply(info1, map[string]int{"POST https://example.com/api/addEvents": 2})

	// send second request to make sure that nothing is blocked
	event2 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 22"}}
	eventBundle2 := &add_events.EventBundle{Event: event2, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err2 := sc.AddEvents([]*add_events.EventBundle{eventBundle2})
	assert.CmpNoError(err2)
	err3 := sc.Finish()
	assert.CmpNoError(err3)

	assert.True(wasSuccessful.Load())
	assert.Cmp(attempt.Load(), int32(3))
	wasSuccessful.Store(false)
	assert.CmpNoError(err2)
	assert.CmpNoError(sc.LastError())
	info2 := httpmock.GetCallCountInfo()
	assert.CmpDeeply(info2, map[string]int{"POST https://example.com/api/addEvents": 3})
}

func (s *SuiteAddEvents) TestAddEventsRetryAfterTime(assert, require *td.T) {
	attempt := atomic.Int32{}
	attempt.Store(0)
	wasSuccessful := atomic.Bool{}
	wasSuccessful.Store(false)
	now := atomic.Int64{}
	now.Store(time.Now().UnixNano())
	expectedTime := atomic.Int64{}
	expectedTime.Store(time.Now().UnixNano())

	retryAfter := (RetryBase * 3).Nanoseconds()
	httpmock.RegisterResponder(
		"POST",
		"https://example.com/api/addEvents",
		func(req *http.Request) (*http.Response, error) {
			if attempt.Load() == 0 {
				now.Store(time.Now().Truncate(time.Second).UnixNano())
				expectedTime.Store(now.Load() + retryAfter)
			} else {
				assert.Gt(time.Now().UnixNano(), expectedTime.Load(), "start: %s, after: %s, expected: %s", time.Unix(0, now.Load()).Format(time.RFC1123), retryAfter, time.Unix(0, expectedTime.Load()).Format(time.RFC1123))
			}
			attempt.Add(1)

			if attempt.Load() < 2 {
				resp, errr := httpmock.NewJsonResponse(429, map[string]interface{}{
					"status":       "error",
					"bytesCharged": 42,
				})
				resp.Header.Set("Retry-After", time.Unix(0, expectedTime.Load()).Format(time.RFC1123))
				return resp, errr
			} else {
				wasSuccessful.Store(true)
				return httpmock.NewJsonResponse(200, map[string]interface{}{
					"status":       "success",
					"bytesCharged": 42,
				})
			}
		})

	config := &config.DataSetConfig{
		Endpoint: "https://example.com",
		Tokens:   config.DataSetTokens{WriteLog: "AAAA"},
		BufferSettings: buffer_config.DataSetBufferSettings{
			MaxSize:              20,
			MaxLifetime:          0,
			RetryInitialInterval: RetryBase,
		},
	}
	sc, _ := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()))

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err := sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	assert.CmpNoError(err)
	err = sc.Finish()
	assert.CmpNoError(err)

	assert.True(wasSuccessful.Load())
	assert.CmpNoError(err)
	assert.CmpNoError(sc.LastError())
	info := httpmock.GetCallCountInfo()
	assert.CmpDeeply(info, map[string]int{"POST https://example.com/api/addEvents": 2})
}

func (s *SuiteAddEvents) TestAddEventsLargeEvent(assert, require *td.T) {
	originalAttrs := make(map[string]interface{})
	for i, v := range []int{-10000, 5000, -1000, 100, 0, -100, 1000, -5000, 10000} {
		originalAttrs[fmt.Sprintf("%d", i)] = strings.Repeat(fmt.Sprintf("%d", i), 1000000+v)
	}

	attempt := atomic.Int32{}
	attempt.Store(0)
	wasSuccessful := atomic.Bool{}
	wasSuccessful.Store(false)

	httpmock.RegisterResponder(
		"POST",
		"https://example.com/api/addEvents",
		func(req *http.Request) (*http.Response, error) {
			attempt.Add(1)
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

			wasSuccessful.Store(true)
			return httpmock.NewJsonResponse(200, map[string]interface{}{
				"status":       "success",
				"bytesCharged": 42,
			})
		})

	config := &config.DataSetConfig{
		Endpoint: "https://example.com",
		Tokens:   config.DataSetTokens{WriteLog: "AAAA"},
		BufferSettings: buffer_config.DataSetBufferSettings{
			MaxSize:              20,
			MaxLifetime:          0,
			RetryInitialInterval: RetryBase,
		},
	}
	sc, _ := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()))

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: originalAttrs}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err := sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	assert.CmpNoError(err)
	err = sc.Finish()
	assert.CmpNoError(err)

	assert.True(wasSuccessful.Load())
	assert.CmpNoError(err)
	assert.CmpNoError(sc.LastError())
	info := httpmock.GetCallCountInfo()
	assert.CmpDeeply(info, map[string]int{"POST https://example.com/api/addEvents": 1})
}

func (s *SuiteAddEvents) TestAddEventsLargeEventThatNeedEscaping(assert, require *td.T) {
	originalAttrs := make(map[string]interface{})
	for i, v := range []int{-10000, 5000, -1000, 100, 0, -100, 1000, -5000, 10000} {
		originalAttrs[fmt.Sprintf("%d", i)] = strings.Repeat("\"", 1000000+v)
	}

	attempt := atomic.Int32{}
	attempt.Store(0)
	wasSuccessful := atomic.Bool{}
	wasSuccessful.Store(false)
	httpmock.RegisterResponder(
		"POST",
		"https://example.com/api/addEvents",
		func(req *http.Request) (*http.Response, error) {
			attempt.Add(1)
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

			wasSuccessful.Store(true)
			return httpmock.NewJsonResponse(200, map[string]interface{}{
				"status":       "success",
				"bytesCharged": 42,
			})
		})

	config := &config.DataSetConfig{
		Endpoint: "https://example.com",
		Tokens:   config.DataSetTokens{WriteLog: "AAAA"},
		BufferSettings: buffer_config.DataSetBufferSettings{
			MaxSize:              20,
			MaxLifetime:          0,
			RetryInitialInterval: RetryBase,
		},
	}
	sc, _ := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()))

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: originalAttrs}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err := sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	assert.CmpNoError(err)
	err = sc.Finish()
	assert.CmpNoError(err)

	assert.True(wasSuccessful.Load())
	assert.CmpNoError(sc.LastError())
	info := httpmock.GetCallCountInfo()
	assert.CmpDeeply(info, map[string]int{"POST https://example.com/api/addEvents": 1})
}

func (s *SuiteAddEvents) TestAddEventsRejectAfterFinish(assert, require *td.T) {
	config := &config.DataSetConfig{
		Endpoint: "https://example.com",
		Tokens:   config.DataSetTokens{WriteLog: "AAAA"},
		BufferSettings: buffer_config.DataSetBufferSettings{
			MaxSize:              20,
			MaxLifetime:          0,
			RetryInitialInterval: RetryBase,
		},
	}
	sc, _ := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()))
	err := sc.Finish()
	assert.CmpNoError(err)

	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err1 := sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	assert.NotNil(err1)
	assert.Cmp(err1.Error(), fmt.Errorf("client has finished - rejecting all new events").Error())
}

func (s *SuiteAddEvents) TestAddEventsWithBufferSweeper(assert, require *td.T) {
	attempt := atomic.Int32{}
	attempt.Store(0)
	httpmock.RegisterResponder(
		"POST",
		"https://example.com/api/addEvents",
		func(req *http.Request) (*http.Response, error) {
			attempt.Add(1)
			cer, err := extract(req)

			assert.CmpNoError(err, "Error reading request: %v", err)
			assert.NotNil(cer)

			return httpmock.NewJsonResponse(200, map[string]interface{}{
				"status":       "success",
				"bytesCharged": 42,
			})
		})

	sentDelay := 5 * time.Millisecond
	config := &config.DataSetConfig{
		Endpoint: "https://example.com",
		Tokens:   config.DataSetTokens{WriteLog: "AAAA"},
		BufferSettings: buffer_config.DataSetBufferSettings{
			MaxSize:              1000,
			MaxLifetime:          2 * sentDelay,
			RetryInitialInterval: RetryBase,
		},
	}
	sc, _ := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()))

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo

	const NumEvents = 10

	go func(n int) {
		for i := 0; i < n; i++ {
			event := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"value": fmt.Sprintf("val-%d", i)}}
			eventBundle := &add_events.EventBundle{Event: event, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
			err := sc.AddEvents([]*add_events.EventBundle{eventBundle})
			assert.Nil(err)
			time.Sleep(sentDelay)
		}
	}(NumEvents)

	// wait on all buffers to be sent
	time.Sleep(sentDelay * NumEvents * 2)

	assert.Gte(attempt.Load(), int32(4))
	info := httpmock.GetCallCountInfo()
	assert.CmpDeeply(info, map[string]int{"POST https://example.com/api/addEvents": int(attempt.Load())})
}

func (s *SuiteAddEvents) TestAddEventsDoNotRetryForever(assert, require *td.T) {
	attempt := atomic.Int32{}
	attempt.Store(0)
	httpmock.RegisterResponder(
		"POST",
		"https://example.com/api/addEvents",
		func(req *http.Request) (*http.Response, error) {
			attempt.Add(1)

			return httpmock.NewJsonResponse(503, map[string]interface{}{
				"status":       "error",
				"bytesCharged": 42,
			})
		})

	config := &config.DataSetConfig{
		Endpoint: "https://example.com",
		Tokens:   config.DataSetTokens{WriteLog: "AAAA"},
		BufferSettings: buffer_config.DataSetBufferSettings{
			MaxSize:                  20,
			MaxLifetime:              0,
			RetryInitialInterval:     time.Second,
			RetryMaxInterval:         time.Second,
			RetryMaxElapsedTime:      5 * time.Second,
			RetryMultiplier:          1.0,
			RetryRandomizationFactor: 1.0,
		},
	}
	sc, _ := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()))

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err := sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	sc.Finish()

	assert.CmpNoError(err)
	info := httpmock.GetCallCountInfo()
	assert.Gte(info["POST https://example.com/api/addEvents"], 3)
}
