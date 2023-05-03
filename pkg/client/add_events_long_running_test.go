//go:build long_running

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
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scalyr/dataset-go/pkg/api/add_events"
	"github.com/scalyr/dataset-go/pkg/config"
	"go.uber.org/zap"

	"github.com/jarcoal/httpmock"
	"github.com/maxatome/go-testdeep/td"
)

func (s *SuiteAddEvents) TestAddEventsManyLogsShouldSucceed(assert, require *td.T) {
	const MaxDelayMs = 200
	config := &config.DataSetConfig{
		Endpoint:       "https://example.com",
		Tokens:         config.DataSetTokens{WriteLog: "AAAA"},
		MaxPayloadB:    1000,
		MaxBufferDelay: time.Duration(MaxDelayMs) * time.Millisecond,
		RetryBase:      RetryBase,
	}
	sc, _ := NewClient(config, &http.Client{}, zap.Must(zap.NewDevelopment()))

	sessionInfo := &add_events.SessionInfo{ServerId: "a", ServerType: "b"}
	sc.SessionInfo = sessionInfo

	const MaxBatchCount = 20
	const LogsPerBatch = 10000
	const ExpectedLogs = uint64(MaxBatchCount * LogsPerBatch)

	attempt := atomic.Uint64{}
	wasSuccessful := atomic.Bool{}
	processedEvents := atomic.Uint64{}
	seen := make(map[string]int64)
	mutex := &sync.RWMutex{}

	httpmock.RegisterResponder(
		"POST",
		"https://example.com/api/addEvents",
		func(req *http.Request) (*http.Response, error) {
			attempt.Add(1)
			cer, err := extract(req)

			assert.CmpNoError(err, "Error reading request: %v", err)

			for _, ev := range cer.Events {
				processedEvents.Add(1)
				key, found := ev.Attrs["body.str"]
				assert.True(found)
				mutex.Lock()
				sKey := key.(string)
				_, f := seen[sKey]
				if !f {
					seen[sKey] = 0
				}
				seen[sKey] += 1
				mutex.Unlock()
			}

			wasSuccessful.Store(true)
			return httpmock.NewJsonResponse(200, map[string]interface{}{
				"status":       "success",
				"bytesCharged": 42,
			})
		})

	for bI := 0; bI < MaxBatchCount; bI++ {
		batch := make([]*add_events.EventBundle, 0)
		for lI := 0; lI < LogsPerBatch; lI++ {
			attrs := make(map[string]interface{})
			attrs["body.str"] = fmt.Sprintf("%04d-%06d", bI, lI)
			attrs["attributes.p1"] = strings.Repeat("A", rand.Intn(2000))

			event := &add_events.Event{
				Thread: "5",
				Sev:    3,
				Ts:     fmt.Sprintf("%d", time.Now().Nanosecond()),
				Attrs:  attrs,
			}
			eventBundle := &add_events.EventBundle{Event: event, Thread: &add_events.Thread{Id: "5", Name: "fred"}}

			batch = append(batch, eventBundle)
		}

		assert.Logf("Consuming batch: %d", bI)
		err := sc.AddEvents(batch)
		assert.Nil(err)

		time.Sleep(time.Duration(MaxDelayMs*0.7) * time.Millisecond)
	}

	time.Sleep(time.Second)
	sc.Finish()

	time.Sleep(2 * time.Second)

	assert.True(wasSuccessful.Load())

	assert.Cmp(processedEvents.Load(), ExpectedLogs, "processed items")
	assert.Cmp(uint64(len(seen)), ExpectedLogs, "unique items")
}
