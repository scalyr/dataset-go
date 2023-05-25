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
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/scalyr/dataset-go/pkg/api/response"

	"github.com/scalyr/dataset-go/pkg/api/add_events"
	"github.com/scalyr/dataset-go/pkg/api/request"
	"github.com/scalyr/dataset-go/pkg/buffer"

	"go.uber.org/zap"
)

/*
Wrapper around: https://app.scalyr.com/help/api#addEvents
*/

// AddEvents enqueues all the events for processing.
// It returns an error if the batch was not accepted.
// When you want to finish processing, call Finish method.
func (client *DataSetClient) AddEvents(bundles []*add_events.EventBundle) error {
	if client.finished.Load() {
		return fmt.Errorf("client has finished - rejecting all new events")
	}
	errR := client.shouldRejectNextBatch()
	if errR != nil {
		return fmt.Errorf("AddEvents - reject batch: %w", errR)
	}

	// first, figure out which keys are part of the batch
	seenKeys := make(map[string]bool)
	for _, bundle := range bundles {
		key := bundle.Key(client.Config.BufferSettings.GroupBy)
		seenKeys[key] = true
	}

	// then create all subscribers
	// add subscriber for events by key
	// add subscriber for buffer by key
	client.addEventsMutex.Lock()
	defer client.addEventsMutex.Unlock()
	for key := range seenKeys {
		_, found := client.addEventsChannels[key]
		if !found {
			client.newBufferForEvents(key)

			client.newChannelForEvents(key)
		}
	}

	// and as last step - publish them
	for _, bundle := range bundles {
		key := bundle.Key(client.Config.BufferSettings.GroupBy)
		client.eventsEnqueued.Add(1)
		client.addEventsPubSub.Pub(bundle, key)
	}

	return nil
}

func (client *DataSetClient) newChannelForEvents(key string) {
	ch := client.addEventsPubSub.Sub(key)
	client.addEventsChannels[key] = ch
	go (func(session string, ch chan interface{}) {
		client.ListenAndSendBundlesForKey(key, ch)
	})(key, ch)
}

func (client *DataSetClient) newBufferForEvents(key string) {
	session := fmt.Sprintf("%s-%s", client.Id, key)
	buf := buffer.NewEmptyBuffer(session, client.Config.Tokens.WriteLog)
	client.initBuffer(buf, client.SessionInfo)

	client.buffersAllMutex.Lock()
	client.buffer[session] = buf
	defer client.buffersAllMutex.Unlock()

	// create subscriber, so all the upcoming buffers are processed as well
	client.addEventsSubscriber(session)
}

func (client *DataSetClient) ListenAndSendBundlesForKey(key string, ch chan interface{}) {
	client.Logger.Info("Listening to events with key",
		zap.String("key", key),
	)

	// this function has to be called from AddEvents - inner loop
	// it assumes that all bundles have the same key
	getBuffer := func(key string) *buffer.Buffer {
		buf := client.getBuffer(key)
		// change state to mark that bundles are being added
		buf.SetStatus(buffer.AddingBundles)
		return buf
	}

	publish := func(key string, buf *buffer.Buffer) *buffer.Buffer {
		client.publishBuffer(buf)
		return getBuffer(key)
	}

	for processedMsgCnt := 0; ; processedMsgCnt++ {
		if msg, ok := <-ch; ok {
			bundle, ok := msg.(*add_events.EventBundle)
			if ok {
				buf := getBuffer(key)
				added, err := buf.AddBundle(bundle)
				if err != nil {
					if errors.Is(err, &buffer.NotAcceptingError{}) {
						buf = getBuffer(key)
					} else {
						client.Logger.Error("Cannot add bundle", zap.Error(err))
						// TODO: what to do? For now, lets skip it
						continue
					}
				}

				if buf.ShouldSendSize() || added == buffer.TooMuch && buf.HasEvents() {
					buf = publish(key, buf)
				}

				if added == buffer.TooMuch {
					added, err = buf.AddBundle(bundle)
					if err != nil {
						if errors.Is(err, &buffer.NotAcceptingError{}) {
							buf = getBuffer(key)
						} else {
							client.Logger.Error("Cannot add bundle", zap.Error(err))
							continue
						}
					}
					if buf.ShouldSendSize() {
						buf = publish(key, buf)
					}
					if added == buffer.TooMuch {
						client.Logger.Fatal("Bundle was not added for second time!", buf.ZapStats()...)
					}
				}
				client.eventsProcessed.Add(1)

				buf.SetStatus(buffer.Ready)
				// it could happen that the buffer could have been published
				// by buffer sweeper, but it was skipped, because we have been
				// adding events, so lets check it and publish it if needed
				if buf.PublishAsap.Load() {
					client.publishBuffer(buf)
				}
			}
		}
	}
}

// IsProcessingBuffers returns True if there are still some unprocessed buffers.
// False otherwise.
func (client *DataSetClient) IsProcessingBuffers() bool {
	return client.buffersEnqueued.Load() > client.buffersProcessed.Load()
}

// IsProcessingEvents returns True if there are still some unprocessed events.
// False otherwise.
func (client *DataSetClient) IsProcessingEvents() bool {
	return client.eventsEnqueued.Load() > client.eventsProcessed.Load()
}

// Finish stops processing of new events and waits until all the data that are
// being processed are really processed.
func (client *DataSetClient) Finish() error {
	// mark as finished
	client.finished.Store(true)

	var lastError error = nil
	expBackoff := backoff.ExponentialBackOff{
		InitialInterval:     client.Config.BufferSettings.RetryInitialInterval,
		RandomizationFactor: client.Config.BufferSettings.RetryRandomizationFactor,
		Multiplier:          client.Config.BufferSettings.RetryMultiplier,
		MaxInterval:         client.Config.BufferSettings.RetryMaxInterval,
		MaxElapsedTime:      client.Config.BufferSettings.RetryMaxElapsedTime,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	expBackoff.Reset()

	// first we wait until all the events in buffers are added into buffers
	// then we are waiting until all the buffers are processed
	// if some progress is made we restart the waiting times

	// do wait for all events to be processed
	retryNum := 0
	lastProcessed := client.eventsProcessed.Load()
	for client.IsProcessingEvents() {
		if client.eventsProcessed.Load() != lastProcessed {
			expBackoff.Reset()
		}
		lastProcessed = client.eventsProcessed.Load()
		backoffDelay := expBackoff.NextBackOff()
		client.Logger.Info(
			"Not all events has been processed",
			zap.Int("retryNum", retryNum),
			zap.Duration("backoffDelay", backoffDelay),
			zap.Uint64("eventsEnqueued", client.eventsEnqueued.Load()),
			zap.Uint64("eventsProcessed", client.eventsProcessed.Load()),
		)
		if backoffDelay == expBackoff.Stop {
			lastError = fmt.Errorf("not all events has been processed")
			break
		}
		time.Sleep(backoffDelay)
		retryNum++
	}

	// send all buffers
	client.SendAllAddEventsBuffers()

	// do wait for all buffers to be processed
	retryNum = 0
	expBackoff.Reset()
	lastProcessed = client.buffersProcessed.Load()
	lastDropped := client.buffersDropped.Load()
	initialDropped := lastDropped
	for client.IsProcessingBuffers() {
		if client.buffersProcessed.Load()+lastDropped != lastProcessed+client.buffersDropped.Load() {
			expBackoff.Reset()
		}
		lastProcessed = client.buffersProcessed.Load()
		lastDropped = client.buffersDropped.Load()
		backoffDelay := expBackoff.NextBackOff()
		client.Logger.Info(
			"Not all buffers has been processed",
			zap.Int("retryNum", retryNum),
			zap.Duration("backoffDelay", backoffDelay),
			zap.Uint64("buffersEnqueued", client.buffersEnqueued.Load()),
			zap.Uint64("buffersProcessed", client.buffersProcessed.Load()),
			zap.Uint64("buffersDropped", client.buffersDropped.Load()),
		)
		if backoffDelay == expBackoff.Stop {
			lastError = fmt.Errorf("not all buffers has been processed")
			break
		}
		time.Sleep(backoffDelay)
		retryNum++
	}

	buffersDropped := client.buffersDropped.Load() - initialDropped
	if buffersDropped > 0 {
		lastError = fmt.Errorf(
			"some buffers were dropped during finishing - %d",
			buffersDropped,
		)
	}

	if lastError == nil {
		client.Logger.Info("Finishing with success")
	} else {
		client.Logger.Error("Finishing with error", zap.Error(lastError))
		if client.LastError() == nil {
			return lastError
		}
	}

	return client.LastError()
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
	resp := &add_events.AddEventsResponse{}

	httpRequest, err := request.NewRequest(
		"POST", client.Config.Endpoint+"/api/addEvents", "userAgent",
	).WithWriteLog(client.Config.Tokens).RawRequest(payload).HttpRequest()
	if err != nil {
		return nil, fmt.Errorf("cannot create request: %w", err)
	}

	err = client.apiCall(httpRequest, resp)

	if strings.HasPrefix(resp.Status, "error") {
		client.Logger.Error(
			"Problematic payload",
			zap.String("message", resp.Message),
			zap.String("status", resp.Status),
			zap.Int("payloadLength", len(payload)),
		)
	}

	return resp, err
}

//func (client *DataSetClient) groupBundles(bundles []*add_events.EventBundle) map[string][]*add_events.EventBundle {
//	grouped := make(map[string][]*add_events.EventBundle)
//
//	// group batch
//	for _, bundle := range bundles {
//		if bundle == nil {
//			continue
//		}
//		key := bundle.Key(client.Config.GroupBy)
//		grouped[key] = append(grouped[key], bundle)
//	}
//	client.Logger.Debug("Batch was grouped",
//		zap.Int("batchSize", len(bundles)),
//		zap.Int("distinctStreams", len(grouped)),
//	)
//	return grouped
//}

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
	buffers := client.getBuffers()
	client.Logger.Debug("Send all AddEvents buffers")
	for _, buf := range buffers {
		client.publishBuffer(buf)
	}
}

func (client *DataSetClient) getBuffers() []*buffer.Buffer {
	buffers := make([]*buffer.Buffer, 0)
	client.buffersAllMutex.Lock()
	defer client.buffersAllMutex.Unlock()
	for _, buf := range client.buffer {
		buffers = append(buffers, buf)
	}
	return buffers
}
