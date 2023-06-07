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
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scalyr/dataset-go/pkg/version"

	"github.com/cenkalti/backoff/v4"

	"github.com/scalyr/dataset-go/pkg/api/add_events"
	"github.com/scalyr/dataset-go/pkg/buffer"
	"github.com/scalyr/dataset-go/pkg/config"

	"github.com/cskr/pubsub"
	"github.com/google/uuid"
	"go.uber.org/zap"

	_ "net/http/pprof"
)

const (
	HttpErrorCannotConnect   = 600
	HttpErrorHasErrorMessage = 499
)

// isOkStatus returns true if status code is 200, false otherwise.
func isOkStatus(status uint32) bool {
	return status == http.StatusOK
}

// isRetryableStatus returns true if status code is 401, 403, 429, or any 5xx, false otherwise.
// Note from DataSet API request handling perspective 401 and 403 may be considered as retryable, see DSET-2201
func isRetryableStatus(status uint32) bool {
	return status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
}

// DataSetClient represent a DataSet REST API client
type DataSetClient struct {
	Id               uuid.UUID
	Config           *config.DataSetConfig
	Client           *http.Client
	SessionInfo      *add_events.SessionInfo
	buffer           map[string]*buffer.Buffer
	buffersAllMutex  sync.Mutex
	buffersEnqueued  atomic.Uint64
	buffersProcessed atomic.Uint64
	buffersDropped   atomic.Uint64
	BuffersPubSub    *pubsub.PubSub
	LastHttpStatus   atomic.Uint32
	lastError        error
	lastErrorMu      sync.RWMutex
	retryAfter       time.Time
	retryAfterMu     sync.RWMutex
	// indicates that client has been shut down and no further processing is possible
	finished          atomic.Bool
	Logger            *zap.Logger
	eventsEnqueued    atomic.Uint64
	eventsProcessed   atomic.Uint64
	bytesAPISent      atomic.Uint64
	bytesAPIAccepted  atomic.Uint64
	addEventsMutex    sync.Mutex
	addEventsPubSub   *pubsub.PubSub
	addEventsChannels map[string]chan interface{}
	// timestamp of first event processed by client
	firstReceivedAt atomic.Int64
	// timestamp of last event so far processed by client
	lastAcceptedAt atomic.Int64
	// Stores sanitized complete URL to the addEvents API endpoint, e.g.
	// https://app.scalyr.com/api/addEvents
	addEventsEndpointUrl string
}

func NewClient(cfg *config.DataSetConfig, client *http.Client, logger *zap.Logger) (*DataSetClient, error) {
	logger.Info(
		"Using config: ",
		zap.String("config", cfg.String()),
		zap.String("version", version.Version),
		zap.String("ReleaseDate", version.ReleasedDate),
	)

	validationErr := cfg.Validate()
	if validationErr != nil {
		return nil, validationErr
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("it was not possible to generate UUID: %w", err)
	}

	addEventsEndpointUrl := cfg.Endpoint
	if strings.HasSuffix(addEventsEndpointUrl, "/") {
		addEventsEndpointUrl += "api/addEvents"
	} else {
		addEventsEndpointUrl += "/api/addEvents"
	}

	dataClient := &DataSetClient{
		Id:                   id,
		Config:               cfg,
		Client:               client,
		buffer:               make(map[string]*buffer.Buffer),
		buffersEnqueued:      atomic.Uint64{},
		buffersProcessed:     atomic.Uint64{},
		buffersDropped:       atomic.Uint64{},
		buffersAllMutex:      sync.Mutex{},
		BuffersPubSub:        pubsub.New(0),
		LastHttpStatus:       atomic.Uint32{},
		retryAfter:           time.Now(),
		retryAfterMu:         sync.RWMutex{},
		lastErrorMu:          sync.RWMutex{},
		Logger:               logger,
		finished:             atomic.Bool{},
		eventsEnqueued:       atomic.Uint64{},
		eventsProcessed:      atomic.Uint64{},
		bytesAPIAccepted:     atomic.Uint64{},
		bytesAPISent:         atomic.Uint64{},
		addEventsMutex:       sync.Mutex{},
		addEventsPubSub:      pubsub.New(0),
		addEventsChannels:    make(map[string]chan interface{}),
		firstReceivedAt:      atomic.Int64{},
		lastAcceptedAt:       atomic.Int64{},
		addEventsEndpointUrl: addEventsEndpointUrl,
	}

	// run buffer sweeper if requested
	if cfg.BufferSettings.MaxLifetime > 0 {
		dataClient.Logger.Info("Buffer.MaxLifetime is positive => send buffers regularly",
			zap.Duration("Buffer.MaxLifetime", cfg.BufferSettings.MaxLifetime),
		)
		go dataClient.bufferSweeper(cfg.BufferSettings.MaxLifetime)
	} else {
		dataClient.Logger.Warn(
			"Buffer.MaxLifetime is not positive => do NOT send buffers regularly",
			zap.Duration("Buffer.MaxLifetime", cfg.BufferSettings.MaxLifetime),
		)
	}

	// run statistics sweeper
	go dataClient.statisticsSweeper()

	dataClient.Logger.Info("DataSetClient was created",
		zap.String("id", dataClient.Id.String()),
	)

	//// Server for pprof
	//go func() {
	//	fmt.Println(http.ListenAndServe("localhost:6060", nil))
	//}()

	return dataClient, nil
}

func (client *DataSetClient) getBuffer(key string) *buffer.Buffer {
	session := fmt.Sprintf("%s-%s", client.Id, key)
	client.buffersAllMutex.Lock()
	defer client.buffersAllMutex.Unlock()
	return client.buffer[session]
}

func (client *DataSetClient) initBuffer(buff *buffer.Buffer, info *add_events.SessionInfo) {
	client.Logger.Debug("Creating new buf",
		zap.String("uuid", buff.Id.String()),
		zap.String("session", buff.Session),
	)

	// Initialise
	err := buff.Initialise(info)
	// only reason why Initialise may fail is because SessionInfo cannot be
	// serialised into JSON. We do not care, so we can set it to nil and continue
	if err != nil {
		client.Logger.Error("Cannot initialize buffer: %w", zap.Error(err))
		err = buff.SetSessionInfo(nil)
		if err != nil {
			panic(fmt.Sprintf("Setting SessionInfo cannot cause an error: %s", err))
		}
	}
}

func (client *DataSetClient) addEventsSubscriber(session string) {
	ch := client.BuffersPubSub.Sub(session)
	go (func(session string, ch chan interface{}) {
		client.listenAndSendBufferForSession(session, ch)
	})(session, ch)
}

func (client *DataSetClient) listenAndSendBufferForSession(session string, ch chan interface{}) {
	client.Logger.Info("Listening to submit buffer",
		zap.String("session", session),
	)

	for processedMsgCnt := 0; ; processedMsgCnt++ {
		if msg, ok := <-ch; ok { // TODO lets use Guard clause if possible to improve readability
			client.Logger.Debug("Received buffer from channel",
				zap.String("session", session),
				zap.Int("processedMsgCnt", processedMsgCnt),
				zap.Uint64("buffersEnqueued", client.buffersEnqueued.Load()),
				zap.Uint64("buffersProcessed", client.buffersProcessed.Load()),
				zap.Uint64("buffersDropped", client.buffersDropped.Load()),
			)
			buf, ok := msg.(*buffer.Buffer) // TODO do not shadow ok which is previously used, rename isXySuccessful
			if ok {
				// TODO following block has ±100 lines, lets extract it to separate methods
				// sleep until retry time
				for client.RetryAfter().After(time.Now()) {
					client.sleep(client.RetryAfter(), buf)
				}
				if !buf.HasEvents() {
					client.Logger.Warn("Buffer is empty, skipping", buf.ZapStats()...)
					client.buffersProcessed.Add(1)
					continue
				}

				// Do not use NewExponentialBackOff since it calls Reset and the code here must
				// call Reset after changing the InitialInterval (this saves an unnecessary call to Now).
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
				retryNum := int64(0)
				for {
					response, payloadLen, err := client.sendAddEventsBuffer(buf)
					client.setLastError(err)
					lastHttpStatus := uint32(0)
					if err != nil {
						client.Logger.Error("unable to send addEvents buffers", zap.Error(err))
						if strings.Contains(err.Error(), "Unable to send request") {
							lastHttpStatus = HttpErrorCannotConnect
							client.LastHttpStatus.Store(lastHttpStatus)
						} else {
							lastHttpStatus = HttpErrorHasErrorMessage
							client.LastHttpStatus.Store(lastHttpStatus)
							client.onBufferDrop(buf, lastHttpStatus, err)
							break
						}
					}
					zaps := make([]zap.Field, 0)
					if response.ResponseObj != nil {
						zaps = append(
							zaps,
							zap.String("httpStatus", response.ResponseObj.Status),
							zap.Int("httpCode", response.ResponseObj.StatusCode),
						)
						lastHttpStatus = uint32(response.ResponseObj.StatusCode)
						client.LastHttpStatus.Store(lastHttpStatus)
					}

					zaps = append(
						zaps,
						zap.String("status", response.Status),
						zap.String("message", response.Message),
					)
					client.Logger.Debug("Events were sent to DataSet",
						zaps...,
					)

					if isOkStatus(lastHttpStatus) {
						// everything was fine, there is no need for retries
						client.bytesAPIAccepted.Add(uint64(payloadLen))
						break
					}

					backoffDelay := expBackoff.NextBackOff()
					client.Logger.Info("Backoff + Retries: ", zap.Int64("retryNum", retryNum), zap.Duration("backoffDelay", backoffDelay))
					if backoffDelay == backoff.Stop {
						// throw away the batch
						err = fmt.Errorf("max elapsed time expired %w", err)
						client.onBufferDrop(buf, lastHttpStatus, err)
						break
					}

					if isRetryableStatus(lastHttpStatus) {
						// check whether header is specified and get its value
						retryAfter, specified := client.getRetryAfter(
							response.ResponseObj,
							backoffDelay,
						)

						if specified {
							// retry after is specified, we should update
							// client state, so we do not send more requests

							client.setRetryAfter(retryAfter)
						}

						client.sleep(retryAfter, buf)
						buf.SetStatus(buffer.Retrying)
					} else {
						err = fmt.Errorf("non recoverable error %w", err)
						client.onBufferDrop(buf, lastHttpStatus, err)
						break
					}
					retryNum++
				}
			} else {
				client.Logger.Error("Cannot convert message", zap.Any("msg", msg))
			}
			client.buffersProcessed.Add(1)
			client.lastAcceptedAt.Store(time.Now().UnixNano())
		} else {
			client.buffersProcessed.Add(1)
			client.lastAcceptedAt.Store(time.Now().UnixNano())
			break
		}
	}
}

func (client *DataSetClient) statisticsSweeper() {
	for i := uint64(0); ; i++ {
		client.logStatistics()
		// wait for some time before new sweep
		time.Sleep(15 * time.Second)
	}
}

func (client *DataSetClient) logStatistics() {
	mb := float64(1024 * 1024)

	// for how long are events being processed
	firstAt := time.Unix(0, client.firstReceivedAt.Load())
	lastAt := time.Unix(0, client.lastAcceptedAt.Load())
	processingDur := lastAt.Sub(firstAt)
	processingInSec := processingDur.Seconds()

	// if nothing was processed, do not log statistics
	if processingInSec <= 0 {
		return
	}

	// log buffer stats
	bProcessed := client.buffersProcessed.Load()
	bEnqueued := client.buffersEnqueued.Load()
	bDropped := client.buffersDropped.Load()
	client.Logger.Info(
		"Buffers' Queue Stats:",
		zap.Uint64("processed", bProcessed),
		zap.Uint64("enqueued", bEnqueued),
		zap.Uint64("dropped", bDropped),
		zap.Uint64("waiting", bEnqueued-bProcessed),
		zap.Float64("processingS", processingInSec),
	)

	// log events stats
	eProcessed := client.eventsProcessed.Load()
	eEnqueued := client.eventsEnqueued.Load()
	client.Logger.Info(
		"Events' Queue Stats:",
		zap.Uint64("processed", eProcessed),
		zap.Uint64("enqueued", eEnqueued),
		zap.Uint64("waiting", eEnqueued-eProcessed),
		zap.Float64("processingS", processingInSec),
	)

	// log transferred stats
	bAPISent := float64(client.bytesAPISent.Load())
	bAPIAccepted := float64(client.bytesAPIAccepted.Load())
	throughput := bAPIAccepted / mb / processingInSec
	successRate := (bAPIAccepted + 1) / (bAPISent + 1)
	perBuffer := (bAPIAccepted) / float64(bProcessed)
	client.Logger.Info(
		"Transfer Stats:",
		zap.Float64("bytesSentMB", bAPISent/mb),
		zap.Float64("bytesAcceptedMB", bAPIAccepted/mb),
		zap.Float64("throughputMBpS", throughput),
		zap.Float64("perBufferMB", perBuffer/mb),
		zap.Float64("successRate", successRate),
		zap.Float64("processingS", processingInSec),
		zap.Duration("processing", processingDur),
	)
}

func (client *DataSetClient) bufferSweeper(lifetime time.Duration) {
	client.Logger.Info("Starting buffer sweeper with lifetime", zap.Duration("lifetime", lifetime))
	totalKept := atomic.Uint64{}
	totalSwept := atomic.Uint64{}
	for i := uint64(0); ; i++ {
		// if everything was finished, there is no need to run buffer sweeper
		//if client.finished.Load() {
		//	client.Logger.Info("Stopping buffer sweeper", zap.Uint64("sweepId", i))
		//	break
		//}
		kept := atomic.Uint64{}
		swept := atomic.Uint64{}
		client.Logger.Debug("Buffer sweeping started", zap.Uint64("sweepId", i))
		buffers := client.getBuffers()
		for _, buf := range buffers {
			// publish buffers that are ready only
			// if we are actively adding events into this buffer skip it for now
			if buf.ShouldSendAge(lifetime) {
				if buf.HasStatus(buffer.Ready) {
					client.publishBuffer(buf)
					swept.Add(1)
					totalSwept.Add(1)
				} else {
					buf.PublishAsap.Store(true)
					kept.Add(1)
					totalKept.Add(1)
				}
			} else {
				kept.Add(1)
				totalKept.Add(1)
			}
		}

		// log just every n-th sweep
		lvl := zap.DebugLevel
		if i%100 == 0 {
			lvl = zap.InfoLevel
		}
		client.Logger.Log(
			lvl,
			"Buffer sweeping finished",
			zap.Uint64("sweepId", i),
			zap.Uint64("nowKept", kept.Load()),
			zap.Uint64("nowSwept", swept.Load()),
			zap.Uint64("nowCombined", kept.Load()+swept.Load()),
			zap.Uint64("totalKept", totalKept.Load()),
			zap.Uint64("totalSwept", totalSwept.Load()),
			zap.Uint64("totalCombined", totalKept.Load()+totalSwept.Load()),
		)

		time.Sleep(lifetime)
	}
}

func (client *DataSetClient) publishBuffer(buf *buffer.Buffer) {
	if buf.HasStatus(buffer.Publishing) {
		// buffer is already publishing, this should not happen
		// so lets skip it
		client.Logger.Warn("Buffer is already beeing published", buf.ZapStats()...)
		return
	}

	// we are manipulating with client.buffer, so lets lock it
	client.buffersAllMutex.Lock()
	originalStatus := buf.Status()
	buf.SetStatus(buffer.Publishing)

	// if the buffer is being used, lets create new one as replacement
	if originalStatus.IsActive() {
		newBuf := buffer.NewEmptyBuffer(buf.Session, buf.Token)
		client.Logger.Debug(
			"Removing buffer for session",
			zap.String("session", buf.Session),
			zap.String("oldUuid", buf.Id.String()),
			zap.String("newUuid", newBuf.Id.String()),
		)

		client.initBuffer(newBuf, buf.SessionInfo())
		client.buffer[buf.Session] = newBuf
	}
	client.buffersAllMutex.Unlock()

	client.Logger.Debug("publishing buffer", buf.ZapStats()...)

	// publish buffer so it can be sent
	client.buffersEnqueued.Add(+1)
	client.BuffersPubSub.Pub(buf, buf.Session)
}

func (client *DataSetClient) shouldRejectNextBatch() error {
	if isRetryableStatus(client.LastHttpStatus.Load()) {
		err := client.LastError()
		if err != nil {
			return fmt.Errorf("rejecting - Last HTTP request contains an error: %w", err)
		} else {
			return fmt.Errorf("rejecting - Last HTTP request had retryable status")
		}
	}

	if client.RetryAfter().After(time.Now()) {
		return fmt.Errorf("rejecting - should retry after %s", client.RetryAfter().Format(time.RFC1123))
	}

	return nil
}

func (client *DataSetClient) getRetryAfter(response *http.Response, def time.Duration) (time.Time, bool) {
	if response == nil {
		return time.Now().Add(def), false
	}
	after := response.Header.Get("Retry-After")

	// if it's not specified, return default
	if len(after) == 0 {
		return time.Now().Add(def), false
	}

	// it is specified in seconds?
	afterS, errS := strconv.Atoi(after)
	if errS == nil {
		// it's integer => so lets convert it
		return time.Now().Add(time.Duration(afterS) * time.Second), true
	}

	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Date
	afterT, errT := time.Parse(time.RFC1123, after)
	if errT == nil {
		return afterT, true
	}

	client.Logger.Warn("Illegal value of Retry-After, using default",
		zap.String("retryAfter", after),
		zap.Duration("default", def),
		zap.Error(errS),
		zap.Error(errT),
	)

	return time.Now().Add(def), false
}

func (client *DataSetClient) sleep(retryAfter time.Time, buffer *buffer.Buffer) {
	// truncate current time to wait a little bit longer
	sleepFor := retryAfter.Sub(time.Now().Truncate(time.Second))
	client.Logger.Info(
		"RetryAfter is in the future, waiting...",
		zap.Duration("sleepFor", sleepFor),
		zap.String("bufferSession", buffer.Session),
		zap.String("bufferUuid", buffer.Id.String()),
	)
	time.Sleep(sleepFor)
}

func (client *DataSetClient) LastError() error {
	client.lastErrorMu.RLock()
	defer client.lastErrorMu.RUnlock()
	return client.lastError
}

func (client *DataSetClient) setLastError(err error) {
	client.lastErrorMu.Lock()
	defer client.lastErrorMu.Unlock()
	client.lastError = err
}

func (client *DataSetClient) RetryAfter() time.Time {
	client.retryAfterMu.RLock()
	defer client.retryAfterMu.RUnlock()
	return client.retryAfter
}

func (client *DataSetClient) setRetryAfter(t time.Time) {
	client.retryAfterMu.Lock()
	defer client.retryAfterMu.Unlock()
	client.retryAfter = t
}

func (client *DataSetClient) onBufferDrop(buf *buffer.Buffer, status uint32, err error) {
	client.buffersDropped.Add(1)
	client.Logger.Error("Dropping buffer",
		buf.ZapStats(
			zap.Uint32("httpStatus", status),
			zap.Error(err),
		)...,
	)
}
