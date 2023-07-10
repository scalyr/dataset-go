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
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scalyr/dataset-go/pkg/server_host_config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scalyr/dataset-go/pkg/buffer_config"

	"github.com/scalyr/dataset-go/pkg/api/add_events"
	"github.com/scalyr/dataset-go/pkg/config"
	"go.uber.org/zap"
)

const RetryBase = time.Second

var attempt = atomic.Int32{}

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

func TestAddEventsRetry(t *testing.T) {
	attempt.Store(0)
	wasSuccessful := atomic.Bool{}
	wasSuccessful.Store(false)
	const succeedInAttempt = int32(3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		attempt.Add(1)
		cer, err := extract(req)

		assert.Nil(t, err, "Error reading request: %v", err)
		assert.Equal(t, "b", cer.SessionInfo.ServerType)
		assert.Equal(t, "a", cer.SessionInfo.ServerId)

		status := "success"
		if attempt.Load() < succeedInAttempt {
			status = "error"
			w.WriteHeader(530)
		} else {
			wasSuccessful.Store(true)
		}

		payload, err := json.Marshal(map[string]interface{}{
			"status":       status,
			"bytesCharged": 42,
		})
		assert.NoError(t, err)
		l, err := w.Write(payload)
		assert.Greater(t, l, 1)
		assert.NoError(t, err)
	}))
	defer server.Close()

	config := newDataSetConfig(server.URL, *newBufferSettings(
		buffer_config.WithRetryMaxElapsedTime(10*RetryBase),
		buffer_config.WithRetryInitialInterval(RetryBase),
		buffer_config.WithRetryMaxInterval(RetryBase),
	), server_host_config.NewDefaultDataSetServerHostSettings())
	sc, err := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()), nil)
	require.Nil(t, err)

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err = sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	assert.Nil(t, err)
	err = sc.Shutdown()
	assert.Nil(t, err)

	assert.True(t, wasSuccessful.Load())
	assert.Nil(t, err)

	assert.Equal(t, attempt.Load(), succeedInAttempt)
}

func TestAddEventsRetryAfterSec(t *testing.T) {
	attempt.Store(0)
	wasSuccessful := atomic.Bool{}
	wasSuccessful.Store(false)
	now := atomic.Int64{}
	now.Store(time.Now().UnixNano())
	expectedTime := atomic.Int64{}
	expectedTime.Store(time.Now().UnixNano())

	retryAfter := (RetryBase * 3).Nanoseconds()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if attempt.Load() == 0 {
			now.Store(time.Now().Truncate(time.Second).UnixNano())
			expectedTime.Store(now.Load() + retryAfter)
		} else {
			assert.Greater(t, time.Now().UnixNano(), expectedTime.Load(), "start: %s, after: %s, expected: %s, now: %s", time.Unix(0, now.Load()).Format(time.RFC1123), retryAfter, time.Unix(0, expectedTime.Load()).Format(time.RFC1123), time.Now().Format(time.RFC1123))
		}
		attempt.Add(1)
		cer, err := extract(req)
		assert.Nil(t, err, "Error reading request: %v", err)
		msg := cer.Events[0].Attrs["message"].(string)
		status := "error"
		if attempt.Load() < 2 {
			assert.Equal(t, msg, "test - 1")
			w.Header().Set("Retry-After", fmt.Sprintf("%d", int(time.Duration(retryAfter).Seconds())))
			w.WriteHeader(429)
		} else {
			if attempt.Load() == 2 {
				assert.Equal(t, msg, "test - 1")
			} else if attempt.Load() == 3 {
				assert.Equal(t, msg, "test - 22")
			} else {
				// this function should be called 3
				assert.Nil(t, msg, "Attempt: %d", attempt.Load())
			}
			wasSuccessful.Store(true)
			status = "success"
		}

		payload, err := json.Marshal(map[string]interface{}{
			"status":       status,
			"bytesCharged": 42,
		})
		assert.NoError(t, err)
		l, err := w.Write(payload)
		assert.Greater(t, l, 1)
		assert.NoError(t, err)
	}))
	defer server.Close()

	config := newDataSetConfig(server.URL, *newBufferSettings(
		buffer_config.WithRetryMaxElapsedTime(10*RetryBase),
		buffer_config.WithRetryInitialInterval(RetryBase),
		buffer_config.WithRetryMaxInterval(RetryBase),
	), server_host_config.NewDefaultDataSetServerHostSettings())
	sc, err := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()), nil)
	require.Nil(t, err)

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

	assert.True(t, wasSuccessful.Load())
	assert.Equal(t, attempt.Load(), int32(2))
	assert.Nil(t, err1)
	assert.Nil(t, sc.LastError())
	// info1 := httpmock.GetCallCountInfo()
	// assert.CmpDeeply(info1, map[string]int{"POST https://example.com/api/addEvents": 2})

	// send second request to make sure that nothing is blocked
	event2 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 22"}}
	eventBundle2 := &add_events.EventBundle{Event: event2, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err2 := sc.AddEvents([]*add_events.EventBundle{eventBundle2})
	assert.Nil(t, err2)
	err3 := sc.Shutdown()
	assert.Nil(t, err3)

	assert.True(t, wasSuccessful.Load())
	assert.Equal(t, attempt.Load(), int32(3))
	wasSuccessful.Store(false)
	assert.Nil(t, err2)
	assert.Nil(t, sc.LastError())
	// info2 := httpmock.GetCallCountInfo()
	// assert.CmpDeeply(info2, map[string]int{"POST https://example.com/api/addEvents": 3})
}

func TestAddEventsRetryAfterTime(t *testing.T) {
	attempt.Store(0)
	wasSuccessful := atomic.Bool{}
	wasSuccessful.Store(false)
	now := atomic.Int64{}
	now.Store(time.Now().UnixNano())
	expectedTime := atomic.Int64{}
	expectedTime.Store(time.Now().UnixNano())

	retryAfter := (RetryBase * 3).Nanoseconds()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if attempt.Load() == 0 {
			now.Store(time.Now().Truncate(time.Second).UnixNano())
			expectedTime.Store(now.Load() + retryAfter)
		} else {
			assert.Greater(t, time.Now().UnixNano(), expectedTime.Load(), "start: %s, after: %s, expected: %s, now: %s", time.Unix(0, now.Load()).Format(time.RFC1123), retryAfter, time.Unix(0, expectedTime.Load()).Format(time.RFC1123), time.Now().Format(time.RFC1123))
		}
		attempt.Add(1)

		status := "error"
		if attempt.Load() < 2 {
			w.Header().Set("Retry-After", time.Unix(0, expectedTime.Load()).Format(time.RFC1123))
			w.WriteHeader(429)
		} else {
			wasSuccessful.Store(true)
			status = "success"
		}
		payload, err := json.Marshal(map[string]interface{}{
			"status":       status,
			"bytesCharged": 42,
		})
		assert.NoError(t, err)
		l, err := w.Write(payload)
		assert.Greater(t, l, 1)
		assert.NoError(t, err)
	}))
	defer server.Close()

	config := newDataSetConfig(server.URL, *newBufferSettings(
		buffer_config.WithRetryMaxElapsedTime(10*RetryBase),
		buffer_config.WithRetryInitialInterval(RetryBase),
		buffer_config.WithRetryMaxInterval(RetryBase),
	), server_host_config.NewDefaultDataSetServerHostSettings())
	sc, err := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()), nil)
	require.Nil(t, err)

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err = sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	assert.Nil(t, err)
	err = sc.Shutdown()
	assert.Nil(t, err)

	assert.True(t, wasSuccessful.Load())
	assert.Nil(t, err)
	assert.Nil(t, sc.LastError())
	// info := httpmock.GetCallCountInfo()
	// assert.CmpDeeply(info, map[string]int{"POST https://example.com/api/addEvents": 2})
}

func TestAddEventsLargeEvent(t *testing.T) {
	originalAttrs := make(map[string]interface{})
	for i, v := range []int{-10000, 5000, -1000, 100, 0, -100, 1000, -5000, 10000} {
		originalAttrs[fmt.Sprintf("%d", i)] = strings.Repeat(fmt.Sprintf("%d", i), 1000000+v)
	}

	attempt.Store(0)
	wasSuccessful := atomic.Bool{}
	wasSuccessful.Store(false)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		attempt.Add(1)
		cer, err := extract(req)
		assert.Nil(t, err, "Error reading request: %v", err)
		assert.Equal(t, "b", cer.SessionInfo.ServerType)
		assert.Equal(t, "a", cer.SessionInfo.ServerId)

		assert.Equal(t, len(cer.Events), 1)
		wasAttrs := (cer.Events)[0].Attrs
		// if attributes were not modified, then we
		// should update test, so they are modified
		assert.NotEqual(t, wasAttrs, originalAttrs)

		wasLengths := make(map[string]int)
		for k, v := range wasAttrs {
			if str, ok := v.(string); ok {
				wasLengths[k] = len(str)
			}
		}
		expectedLengths := map[string]int{
			add_events.AttrBundleKye: 32,
			"0":                      990000,
			"7":                      995000,
			"2":                      999000,
			"5":                      999900,
			"4":                      1000000,
			"3":                      1000100,
			"6":                      241670,
		}

		expectedAttrs := map[string]interface{}{
			add_events.AttrBundleKye: "3a8d26251579170a1a04bf5ba194d138",
			"0":                      strings.Repeat("0", expectedLengths["0"]),
			"7":                      strings.Repeat("7", expectedLengths["7"]),
			"2":                      strings.Repeat("2", expectedLengths["2"]),
			"5":                      strings.Repeat("5", expectedLengths["5"]),
			"4":                      strings.Repeat("4", expectedLengths["4"]),
			"3":                      strings.Repeat("3", expectedLengths["3"]),
			"6":                      strings.Repeat("6", expectedLengths["6"]),
		}
		assert.Equal(t, wasLengths, expectedLengths)
		assert.Equal(t, wasAttrs, expectedAttrs, wasAttrs)

		wasSuccessful.Store(true)
		payload, err := json.Marshal(map[string]interface{}{
			"status":       "success",
			"bytesCharged": 42,
		})
		assert.NoError(t, err)
		l, err := w.Write(payload)
		assert.Greater(t, l, 1)
		assert.NoError(t, err)
	}))
	defer server.Close()

	config := newDataSetConfig(server.URL, *newBufferSettings(
		buffer_config.WithRetryMaxElapsedTime(10*RetryBase),
		buffer_config.WithRetryInitialInterval(RetryBase),
		buffer_config.WithRetryMaxInterval(RetryBase),
	), *newDataSetServerHostSettings())
	sc, err := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()), nil)
	require.Nil(t, err)

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: originalAttrs}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err = sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	assert.Nil(t, err)
	err = sc.Shutdown()
	assert.Nil(t, err)

	assert.True(t, wasSuccessful.Load())
	assert.Nil(t, err)
	assert.Nil(t, sc.LastError())
	// info := httpmock.GetCallCountInfo()
	// assert.CmpDeeply(info, map[string]int{"POST https://example.com/api/addEvents": 1})
}

func TestAddEventsLargeEventThatNeedEscaping(t *testing.T) {
	originalAttrs := make(map[string]interface{})
	for i, v := range []int{-10000, 5000, -1000, 100, 0, -100, 1000, -5000, 10000} {
		originalAttrs[fmt.Sprintf("%d", i)] = strings.Repeat("\"", 1000000+v)
	}

	attempt.Store(0)
	wasSuccessful := atomic.Bool{}
	wasSuccessful.Store(false)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		attempt.Add(1)
		cer, err := extract(req)
		assert.Nil(t, err, "Error reading request: %v", err)
		assert.Equal(t, "b", cer.SessionInfo.ServerType)
		assert.Equal(t, "a", cer.SessionInfo.ServerId)

		assert.Equal(t, len(cer.Events), 1)
		wasAttrs := (cer.Events)[0].Attrs
		// if attributes were not modified, then we
		// should update test, so they are modified
		assert.NotEqual(t, wasAttrs, originalAttrs)

		wasLengths := make(map[string]int)
		for k, v := range wasAttrs {
			if str, ok := v.(string); ok {
				wasLengths[k] = len(str)
			}
		}
		expectedLengths := map[string]int{
			add_events.AttrBundleKye: 32,
			"0":                      990000,
			"7":                      995000,
			"2":                      999000,
			"5":                      6,
		}

		expectedAttrs := map[string]interface{}{
			add_events.AttrBundleKye: "3a8d26251579170a1a04bf5ba194d138",
			"0":                      strings.Repeat("\"", expectedLengths["0"]),
			"7":                      strings.Repeat("\"", expectedLengths["7"]),
			"2":                      strings.Repeat("\"", expectedLengths["2"]),
			"5":                      strings.Repeat("\"", expectedLengths["5"]),
		}
		assert.Equal(t, wasLengths, expectedLengths)
		assert.Equal(t, wasAttrs, expectedAttrs)

		wasSuccessful.Store(true)
		payload, err := json.Marshal(map[string]interface{}{
			"status":       "success",
			"bytesCharged": 42,
		})
		assert.NoError(t, err)
		l, err := w.Write(payload)
		assert.Greater(t, l, 1)
		assert.NoError(t, err)
	}))
	defer server.Close()

	config := newDataSetConfig(server.URL, *newBufferSettings(
		buffer_config.WithRetryMaxElapsedTime(10*RetryBase),
		buffer_config.WithRetryInitialInterval(RetryBase),
		buffer_config.WithRetryMaxInterval(RetryBase),
	), *newDataSetServerHostSettings())
	sc, err := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()), nil)
	require.Nil(t, err)

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: originalAttrs}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err = sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	assert.Nil(t, err)
	err = sc.Shutdown()
	assert.Nil(t, err)

	assert.True(t, wasSuccessful.Load())
	assert.Nil(t, sc.LastError())
	// info := httpmock.GetCallCountInfo()
	// assert.CmpDeeply(info, map[string]int{"POST https://example.com/api/addEvents": 1})
}

func TestAddEventsRejectAfterFinish(t *testing.T) {
	config := newDataSetConfig("https://example.com", *newBufferSettings(
		buffer_config.WithRetryMaxElapsedTime(10*RetryBase),
		buffer_config.WithRetryInitialInterval(RetryBase),
		buffer_config.WithRetryMaxInterval(RetryBase),
	), server_host_config.NewDefaultDataSetServerHostSettings())
	sc, err := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()), nil)
	require.Nil(t, err)
	err = sc.Shutdown()
	assert.Nil(t, err)

	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err1 := sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	assert.NotNil(t, err1)
	assert.Equal(t, err1.Error(), fmt.Errorf("client has finished - rejecting all new events").Error())
}

func TestAddEventsWithBufferSweeper(t *testing.T) {
	attempt.Store(0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		attempt.Add(1)
		cer, err := extract(req)

		assert.Nil(t, err, "Error reading request: %v", err)
		assert.NotNil(t, cer)

		payload, err := json.Marshal(map[string]interface{}{
			"status":       "success",
			"bytesCharged": 42,
		})
		assert.NoError(t, err)
		l, err := w.Write(payload)
		assert.Greater(t, l, 1)
		assert.NoError(t, err)
	}))
	defer server.Close()

	sentDelay := 50 * time.Millisecond
	config := &config.DataSetConfig{
		Endpoint: server.URL,
		Tokens:   config.DataSetTokens{WriteLog: "AAAA"},
		BufferSettings: buffer_config.DataSetBufferSettings{
			MaxSize:                  1000,
			MaxLifetime:              2 * sentDelay,
			RetryRandomizationFactor: 1.0,
			RetryMultiplier:          1.0,
			RetryInitialInterval:     RetryBase,
			RetryMaxInterval:         RetryBase,
			RetryMaxElapsedTime:      10 * RetryBase,
		},
		ServerHostSettings: server_host_config.NewDefaultDataSetServerHostSettings(),
	}
	sc, err := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()), nil)
	require.Nil(t, err)

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo

	const NumEvents = 10

	go func(n int) {
		for i := 0; i < n; i++ {
			event := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"value": fmt.Sprintf("val-%d", i)}}
			eventBundle := &add_events.EventBundle{Event: event, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
			err := sc.AddEvents([]*add_events.EventBundle{eventBundle})
			assert.Nil(t, err)
			time.Sleep(sentDelay)
		}
	}(NumEvents)

	// wait on all buffers to be sent
	time.Sleep(sentDelay * (NumEvents*2 + 1))

	assert.GreaterOrEqual(t, attempt.Load(), int32(4))
	// info := httpmock.GetCallCountInfo()
	// assert.CmpDeeply(info, map[string]int{"POST https://example.com/api/addEvents": int(attempt.Load())})
}

func TestAddEventsDoNotRetryForever(t *testing.T) {
	attempt.Store(0)
	server := mockServerDefaultPayload(t, 503)
	defer server.Close()

	config := newDataSetConfig(server.URL, *newBufferSettings(
		buffer_config.WithRetryMaxElapsedTime(time.Duration(5) * time.Second),
	), server_host_config.NewDefaultDataSetServerHostSettings())
	sc, err := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()), nil)
	require.Nil(t, err)

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err = sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	assert.Nil(t, err)
	err = sc.Shutdown()

	assert.NotNil(t, err)
	assert.Errorf(t, err, "some buffers were dropped during finishing - 1")
	assert.GreaterOrEqual(t, attempt.Load(), int32(2))
}

func TestAddEventsLogResponseBodyOnInvalidJson(t *testing.T) {
	attempt.Store(0)
	server := mockServer(t, 503, []byte("<html>not valid json</html>"))
	defer server.Close()
	config := newDataSetConfig(server.URL, *newBufferSettings(
		buffer_config.WithRetryMaxElapsedTime(time.Duration(3) * time.Second),
	), server_host_config.NewDefaultDataSetServerHostSettings())
	sc, err := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()), nil)
	require.Nil(t, err)

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	err = sc.AddEvents([]*add_events.EventBundle{eventBundle1})
	assert.Nil(t, err)
	err = sc.Shutdown()

	lastError := sc.LastError()

	assert.NotNil(t, lastError)
	assert.Equal(t, fmt.Errorf("unable to parse response body: invalid character '<' looking for beginning of value, url: %s, response: <html>not valid json</html>", sc.addEventsEndpointUrl).Error(), lastError.Error())

	assert.NotNil(t, err)
	assert.Errorf(t, err, "some buffers were dropped during finishing - 1")
	assert.GreaterOrEqual(t, attempt.Load(), int32(0))
}

func TestAddEventsAreNotRejectedOncePreviousReqRetriesMaxLifetimeExpired(t *testing.T) {
	// GIVEN
	maxElapsedTime := 10
	lastEventRetriesExpiration := maxElapsedTime + 1
	attempt.Store(0)
	server := mockServerDefaultPayload(t, http.StatusOK)
	defer server.Close()
	dataSetConfig := newDataSetConfig(server.URL, *newBufferSettings(
		buffer_config.WithMaxLifetime(time.Second),
		buffer_config.WithRetryMaxElapsedTime(time.Duration(maxElapsedTime)*time.Second),
		buffer_config.WithRetryRandomizationFactor(0.000000001),
	), server_host_config.NewDefaultDataSetServerHostSettings())
	client, err := NewClient(dataSetConfig, &http.Client{}, zap.Must(zap.NewDevelopment()), nil)
	require.Nil(t, err)

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	client.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}

	// GIVEN mock previous event request error
	client.setLastErrorTimestamp(time.Now().Add(-time.Duration(lastEventRetriesExpiration) * time.Second))
	client.setLastError(fmt.Errorf("failed to handle previous request"))
	client.LastHttpStatus.Store(http.StatusTooManyRequests)

	// WHEN
	err = client.AddEvents([]*add_events.EventBundle{eventBundle1})
	// THEN event is not rejected
	assert.Nil(t, err)
}

func TestAddEventsAreRejectedOncePreviousReqRetriesMaxLifetimeNotExpired(t *testing.T) {
	// GIVEN
	maxElapsedTime := 10
	lastEventRetriesExpiration := maxElapsedTime - 1
	attempt.Store(0)
	server := mockServerDefaultPayload(t, http.StatusOK)
	defer server.Close()
	dataSetConfig := newDataSetConfig(server.URL, *newBufferSettings(
		buffer_config.WithMaxLifetime(time.Second),
		buffer_config.WithRetryMaxElapsedTime(time.Duration(maxElapsedTime)*time.Second),
		buffer_config.WithRetryRandomizationFactor(0.000000001),
	), server_host_config.NewDefaultDataSetServerHostSettings())
	client, err := NewClient(dataSetConfig, &http.Client{}, zap.Must(zap.NewDevelopment()), nil)
	require.Nil(t, err)

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	client.SessionInfo = sessionInfo
	event1 := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test - 1"}}
	eventBundle1 := &add_events.EventBundle{Event: event1, Thread: &add_events.Thread{Id: "5", Name: "fred"}}

	// GIVEN mock previous event request error
	client.setLastErrorTimestamp(time.Now().Add(-time.Duration(lastEventRetriesExpiration) * time.Second))
	client.setLastError(fmt.Errorf("failed to handle previous request"))
	client.LastHttpStatus.Store(http.StatusTooManyRequests)

	// WHEN
	err = client.AddEvents([]*add_events.EventBundle{eventBundle1})
	// THEN event is rejected
	assert.NotNil(t, err)
	assert.Errorf(t, err, "AddEvents - reject batch: rejecting - Last HTTP request contains an error: failed to handle previous request")
}

func TestAddEventsServerHostLogic(t *testing.T) {
	configServerHost := "global-server-host"
	ev1ServerHost := "host-1"
	ev2ServerHost := "host-2"
	ev3ServerHost := "host-3"
	ev4ServerHost := "host-4"
	ev5ServerHost := "host-5"
	key := "key"
	ev1Value := "event-1-value"
	ev2Value := "event-2-value"
	ev3Value := "event-3-value"
	ev4Value := "event-4-value"
	ev5Value := "event-5-value"

	// define new types to make the code shorter
	type tAttr = add_events.EventAttrs
	type tEvent struct {
		attrs      tAttr
		serverHost string
	}

	tests := []struct {
		name     string
		events   []tEvent
		groupBy  []string
		expCalls [][]tAttr
	}{
		// when nothing is specified, there is just once call
		{
			name: "no server host is specified",
			events: []tEvent{
				{
					attrs: tAttr{key: ev1Value},
				},
				{
					attrs: tAttr{key: ev2Value},
				},
			},
			expCalls: [][]tAttr{
				{
					{key: ev1Value, add_events.AttrOrigServerHost: configServerHost},
					{key: ev2Value, add_events.AttrOrigServerHost: configServerHost},
				},
			},
		},

		// when serverHost is specified and is same as global one, there is just once call
		{
			name: "serverHost is same as global",
			events: []tEvent{
				{
					attrs:      tAttr{key: ev1Value},
					serverHost: configServerHost,
				},
				{
					attrs: tAttr{key: ev2Value},
				},
			},
			expCalls: [][]tAttr{
				{
					{key: ev1Value, add_events.AttrOrigServerHost: configServerHost},
					{key: ev2Value, add_events.AttrOrigServerHost: configServerHost},
				},
			},
		},

		// when serverHost is specified and is different from global one, there are two calls
		{
			name: "serverHost is different from global",
			events: []tEvent{
				{
					attrs:      tAttr{key: ev1Value},
					serverHost: ev1ServerHost,
				},
				{
					attrs: tAttr{key: ev2Value},
				},
			},
			expCalls: [][]tAttr{
				{
					{key: ev1Value, add_events.AttrOrigServerHost: ev1ServerHost},
					{key: ev2Value, add_events.AttrOrigServerHost: configServerHost},
				},
			},
		},

		// when serverHost is specified and is same as attribute serverHost
		{
			name: "serverHost and attribute serverHost are same",
			events: []tEvent{
				{
					attrs:      tAttr{key: ev1Value, add_events.AttrServerHost: ev1ServerHost},
					serverHost: ev1ServerHost,
				},
				{
					attrs: tAttr{key: ev2Value},
				},
			},
			expCalls: [][]tAttr{
				{
					{key: ev1Value, add_events.AttrOrigServerHost: ev1ServerHost},
					{key: ev2Value, add_events.AttrOrigServerHost: configServerHost},
				},
			},
		},

		// when serverHost is specified and is same as attribute serverHost then serverHost wins
		{
			name: "serverHost and attribute serverHost differs",
			events: []tEvent{
				{
					attrs:      tAttr{key: ev1Value, add_events.AttrServerHost: ev1ServerHost},
					serverHost: ev3ServerHost,
				},
				{
					attrs: tAttr{key: ev2Value},
				},
			},
			expCalls: [][]tAttr{
				{
					{key: ev1Value, add_events.AttrOrigServerHost: ev3ServerHost},
					{key: ev2Value, add_events.AttrOrigServerHost: configServerHost},
				},
			},
		},

		// when serverHosts are different, but they are not used for grouping
		{
			name: "serverHost is different and in both events from global",
			events: []tEvent{
				{
					attrs:      tAttr{key: ev1Value},
					serverHost: ev1ServerHost,
				},
				{
					attrs:      tAttr{key: ev2Value},
					serverHost: ev2ServerHost,
				},
				{
					attrs:      tAttr{key: ev3Value},
					serverHost: ev3ServerHost,
				},
				{
					attrs:      tAttr{key: ev4Value},
					serverHost: ev4ServerHost,
				},
				{
					attrs:      tAttr{key: ev5Value},
					serverHost: ev5ServerHost,
				},
			},

			expCalls: [][]tAttr{
				{
					{key: ev1Value, add_events.AttrOrigServerHost: ev1ServerHost},
					{key: ev2Value, add_events.AttrOrigServerHost: ev2ServerHost},
					{key: ev3Value, add_events.AttrOrigServerHost: ev3ServerHost},
					{key: ev4Value, add_events.AttrOrigServerHost: ev4ServerHost},
					{key: ev5Value, add_events.AttrOrigServerHost: ev5ServerHost},
				},
			},
		},

		// when serverHosts are different, but they are not used for grouping
		{
			name: "serverHost is different and in both events from global",
			events: []tEvent{
				{
					attrs:      tAttr{key: ev1Value},
					serverHost: ev1ServerHost,
				},
				{
					attrs:      tAttr{key: ev2Value},
					serverHost: ev2ServerHost,
				},
				{
					attrs:      tAttr{key: ev3Value},
					serverHost: ev3ServerHost,
				},
				{
					attrs:      tAttr{key: ev4Value},
					serverHost: ev4ServerHost,
				},
				{
					attrs:      tAttr{key: ev5Value},
					serverHost: ev5ServerHost,
				},
			},
			groupBy: []string{key},
			expCalls: [][]tAttr{
				{
					{key: ev1Value, add_events.AttrOrigServerHost: ev1ServerHost},
				},
				{
					{key: ev2Value, add_events.AttrOrigServerHost: ev2ServerHost},
				},
				{
					{key: ev3Value, add_events.AttrOrigServerHost: ev3ServerHost},
				},
				{
					{key: ev4Value, add_events.AttrOrigServerHost: ev4ServerHost},
				},
				{
					{key: ev5Value, add_events.AttrOrigServerHost: ev5ServerHost},
				},
			},
		},
	}

	extractAttrs := func(events []*add_events.Event) []map[string]interface{} {
		attrs := make([]map[string]interface{}, 0)
		for _, ev := range events {
			delete(ev.Attrs, add_events.AttrBundleKye)
			attrs = append(attrs, ev.Attrs)
		}
		return attrs
	}

	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			numCalls := atomic.Int32{}
			lock := sync.Mutex{}
			calls := make(map[string][]map[string]interface{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				numCalls.Add(1)
				cer, err := extract(req)

				assert.Nil(t, err, "Error reading request: %v", err)
				assert.Equal(t, "b", cer.SessionInfo.ServerType)
				assert.Equal(t, "a", cer.SessionInfo.ServerId)

				serverHost := cer.SessionInfo.ServerHost
				lock.Lock()
				val, ok := calls[serverHost]
				if ok {
					val = append(val, extractAttrs(cer.Events)...)
				} else {
					val = extractAttrs(cer.Events)
				}
				calls[serverHost] = val
				lock.Unlock()

				payload, err := json.Marshal(map[string]interface{}{
					"status":       "success",
					"bytesCharged": 42,
				})
				assert.NoError(t, err)
				l, err := w.Write(payload)
				assert.Greater(t, l, 1)
				assert.NoError(t, err)
			}))
			defer server.Close()

			bCfgD := buffer_config.NewDefaultDataSetBufferSettings()
			bCfg, err := (&bCfgD).WithOptions(
				buffer_config.WithGroupBy(tt.groupBy),
			)
			require.NoError(t, err)

			config := newDataSetConfig(
				server.URL,
				*bCfg,
				server_host_config.DataSetServerHostSettings{
					UseHostName: false,
					ServerHost:  configServerHost,
				})
			sc, err := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()), nil)
			require.Nil(t, err)
			sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
			sc.SessionInfo = sessionInfo

			bundles := make([]*add_events.EventBundle, 0)
			for _, event := range tt.events {
				bundles = append(
					bundles,
					&add_events.EventBundle{
						Event: &add_events.Event{
							Thread:     "5",
							Sev:        3,
							Ts:         "1",
							Attrs:      event.attrs,
							ServerHost: event.serverHost,
						},
					},
				)
			}

			err = sc.AddEvents(bundles)
			assert.Nil(t, err)
			err = sc.Shutdown()
			assert.Nil(t, err)

			// check that expected API calls were made with expected values
			assert.Equal(t, tt.expCalls, calls, tt.name)
		})
	}
}

func mockServerDefaultPayload(t *testing.T, statusCode int) *httptest.Server {
	payload, _ := json.Marshal(map[string]interface{}{
		"status":       "success",
		"bytesCharged": 42,
	})
	return mockServer(t, statusCode, payload)
}

func mockServer(t *testing.T, statusCode int, payload []byte) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		attempt.Add(1)
		w.WriteHeader(statusCode)
		l, err := w.Write(payload)
		assert.Greater(t, l, 1)
		assert.NoError(t, err)
	}))
	return server
}

func newDataSetConfig(url string, bufferSettings buffer_config.DataSetBufferSettings, serverHostSettings server_host_config.DataSetServerHostSettings) *config.DataSetConfig {
	return &config.DataSetConfig{
		Endpoint:           url,
		Tokens:             config.DataSetTokens{WriteLog: "AAAA"},
		BufferSettings:     bufferSettings,
		ServerHostSettings: serverHostSettings,
	}
}

func newBufferSettings(customOpts ...buffer_config.DataSetBufferSettingsOption) *buffer_config.DataSetBufferSettings {
	defaultOpts := []buffer_config.DataSetBufferSettingsOption{
		buffer_config.WithMaxSize(20),
		buffer_config.WithMaxLifetime(0),
		buffer_config.WithRetryInitialInterval(time.Second),
		buffer_config.WithRetryMaxInterval(time.Second),
		buffer_config.WithRetryMaxElapsedTime(time.Duration(1) * time.Second),
		buffer_config.WithRetryMultiplier(1.0),
		buffer_config.WithRetryRandomizationFactor(1.0),
	}
	bufferSetting, _ := buffer_config.New(append(defaultOpts, customOpts...)...)
	return bufferSetting
}

func newDataSetServerHostSettings() *server_host_config.DataSetServerHostSettings {
	return &server_host_config.DataSetServerHostSettings{
		UseHostName: false,
		ServerHost:  "foo",
	}
}
