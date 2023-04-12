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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/scalyr/dataset-go/pkg/api/response"

	"github.com/scalyr/dataset-go/pkg/api/add_events"
	"github.com/scalyr/dataset-go/pkg/api/request"
	"github.com/scalyr/dataset-go/pkg/buffer"

	"go.uber.org/zap"
)

/*
Wrapper around: https://app.scalyr.com/help/api#addEvents
*/

func (client *DataSetClient) AddEvents(bundles []*add_events.EventBundle) error {
	errR := client.shouldRejectNextBatch()
	if errR != nil {
		return fmt.Errorf("AddEventsOptim - reject batch: %w", errR)
	}

	grouped := make(map[string][]*add_events.EventBundle)

	// group batch
	for _, bundle := range bundles {
		if bundle == nil {
			continue
		}
		key := bundle.Key(client.Config.GroupBy)
		grouped[key] = append(grouped[key], bundle)
	}
	client.Logger.Debug("Batch was grouped",
		zap.Int("batchSize", len(bundles)),
		zap.Int("distinctStreams", len(grouped)),
	)

	for key, bundles := range grouped {

		buf, err := client.Buffer(key, client.SessionInfo)
		if err != nil {
			return fmt.Errorf("cannot get buffer: %w", err)
		}

		for _, bundle := range bundles {
			added, err := buf.AddBundle(bundle)
			if err != nil {
				client.Logger.Error("Cannot add bundle", zap.Error(err))
				// TODO: what to do? For now, lets skip it
				continue
			}

			if buf.ShouldSendSize() || added == buffer.TooMuch && buf.HasEvents() {
				buf = client.PublishBuffer(buf)
			}

			if added == buffer.TooMuch {
				added, err = buf.AddBundle(bundle)
				if err != nil {
					client.Logger.Error("Cannot add bundle", zap.Error(err))
					continue
				}
				if buf.ShouldSendSize() {
					buf = client.PublishBuffer(buf)
				}
				if added == buffer.TooMuch {
					client.Logger.Fatal("Bundle was not added for second time!", buf.ZapStats()...)
				}
			}
		}
	}

	return nil
}

func (client *DataSetClient) SendAddEventsBuffer(buf *buffer.Buffer) (*add_events.AddEventsResponse, error) {
	client.Logger.Debug("Sending buf", buf.ZapStats()...)

	payload, err := buf.Payload()
	if err != nil {
		client.Logger.Warn("Cannot create payload", buf.ZapStats(zap.Error(err))...)
		return nil, fmt.Errorf("cannot create payload: %w", err)
	}
	client.Logger.Debug("Created payload",
		buf.ZapStats(
			zap.Int("payload", len(payload)),
			zap.Float64("payloadRatio", float64(len(payload))/buffer.ShouldSentBufferSize),
		)...,
	)
	response := &add_events.AddEventsResponse{}

	httpRequest, err := request.NewRequest(
		"POST", client.Config.Endpoint+"/api/addEvents",
	).WithWriteLog(client.Config.Tokens).RawRequest(payload).HttpRequest()
	if err != nil {
		return nil, fmt.Errorf("cannot create request: %w", err)
	}

	err = client.apiCall(httpRequest, response)

	if strings.HasPrefix(response.Status, "error") {
		client.Logger.Error(
			"Problematic payload",
			zap.String("message", response.Message),
			zap.String("status", response.Status),
			zap.ByteString("Payload", payload),
		)
	}

	return response, err
}

func (client *DataSetClient) apiCall(req *http.Request, response response.SetResponseObj) error {
	resp, err := client.Client.Do(req)
	if err != nil {
		return fmt.Errorf("unable to send request: %w", err)
	}

	defer func() {
		if err = resp.Body.Close(); err != nil {
			client.Logger.Error("Error when closing:", zap.Error(err))
		}
	}()

	// foo
	client.Logger.Debug("Received response",
		zap.Int("statusCode", resp.StatusCode),
		zap.String("status", resp.Status),
		zap.Int64("contentLength", resp.ContentLength),
	)

	if resp.StatusCode != http.StatusOK {
		client.Logger.Warn(
			"!!!!! PAYLOAD WAS NOT ACCEPTED !!!!",
			zap.Int("statusCode", resp.StatusCode),
		)
	}

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("unable to read response: %w", err)
	}

	err = json.Unmarshal(responseBody, &response)
	if err != nil {
		return fmt.Errorf("unable to parse response body: %w", err)
	}

	response.SetResponseObj(resp)

	return nil
}

func (client *DataSetClient) SendAllAddEventsBuffers() {
	client.buffer.Range(func(k, v interface{}) bool {
		buf, ok := v.(*buffer.Buffer)
		if ok {
			client.PublishBuffer(buf)
		} else {
			client.Logger.Error("Unable to convert message to buffer")
		}
		return true
	})

	for client.buffersEnqueued.Load() > client.buffersProcessed.Load() {
		client.Logger.Info(
			"Not all buffers has been processed",
			zap.Int64("buffersEnqueued", client.buffersEnqueued.Load()),
			zap.Int64("buffersProcessed", client.buffersProcessed.Load()),
		)
		time.Sleep(client.Config.RetryBase)
		client.workers.Wait()
	}

	client.Logger.Info("All buffers have been processed")
}
