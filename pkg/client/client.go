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

func IsRetryableStatus(status uint32) bool {
	return status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
}

type DataSetClient struct {
	Id               uuid.UUID
	Config           *config.DataSetConfig
	Client           *http.Client
	SessionInfo      *add_events.SessionInfo
	buffer           sync.Map
	buffersEnqueued  atomic.Int64
	buffersProcessed atomic.Int64
	PubSub           *pubsub.PubSub
	workers          sync.WaitGroup
	LastHttpStatus   atomic.Uint32
	lastError        error
	lastErrorMu      sync.RWMutex
	retryAfter       time.Time
	retryAfterMu     sync.RWMutex
	Logger           *zap.Logger
}

func (client *DataSetClient) Buffer(key string, info *add_events.SessionInfo) (*buffer.Buffer, error) {
	session := fmt.Sprintf("%s-%s", client.Id, key)
	// get buf so we can start using it
	buf, loaded := client.buffer.LoadOrStore(
		session,
		buffer.NewEmptyBuffer(session, client.Config.Tokens.WriteLog),
	)

	if !loaded {
		client.Logger.Debug("Creating new buf",
			zap.String("key", key),
			zap.String("session", session),
		)

		err := buf.(*buffer.Buffer).SetSessionInfo(info)
		if err != nil {
			return nil, fmt.Errorf("buffer - cannot set session: %w", err)
		}
		buf.(*buffer.Buffer).Reset()

		// it's brand-new buf, so let's create subscriber
		client.AddEventsSubscriber(session)
	}

	return buf.(*buffer.Buffer), nil
}

func (client *DataSetClient) AddEventsSubscriber(session string) {
	ch := client.PubSub.Sub(session)
	go (func(session string, ch chan interface{}) {
		client.ListenAndSendBufferForSession(session, ch)
	})(session, ch)
}

func (client *DataSetClient) ListenAndSendBufferForSession(session string, ch chan interface{}) {
	client.Logger.Info("Listening to submit buffer",
		zap.String("session", session),
	)

	for i := 0; ; i++ {
		if msg, ok := <-ch; ok {
			client.Logger.Debug("Received buffer from channel",
				zap.String("session", session),
				zap.Int("index", i),
				zap.Int64("buffersEnqueued", client.buffersEnqueued.Load()),
				zap.Int64("buffersProcessed", client.buffersProcessed.Load()),
			)
			buf, ok := msg.(*buffer.Buffer)
			client.workers.Add(1)
			if ok {
				for client.RetryAfter().After(time.Now()) {
					client.sleep(client.RetryAfter(), buf)
				}
				if !buf.HasEvents() {
					client.Logger.Warn("Buffer is empty, skipping", buf.ZapStats()...)
					client.workers.Done()
					client.buffersProcessed.Add(1)
					continue
				}
				response, err := client.SendAddEventsBuffer(buf)
				client.SetLastError(err)
				lastHttpStatus := uint32(0)
				if err != nil {
					client.Logger.Error("unable to send addEvents buffers", zap.Error(err))
					if strings.Contains(err.Error(), "Unable to send request") {
						lastHttpStatus = HttpErrorCannotConnect
						client.LastHttpStatus.Store(lastHttpStatus)
					} else {
						lastHttpStatus = HttpErrorHasErrorMessage
						client.LastHttpStatus.Store(lastHttpStatus)
						client.workers.Done()
						client.buffersProcessed.Add(1)
						continue
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

				if IsRetryableStatus(lastHttpStatus) {
					// check whether header is specified and get its value
					retryAfter, specified := client.getRetryAfter(
						response.ResponseObj,
						time.Duration(
							int64(buf.Attempt+1)*client.Config.RetryBase.Nanoseconds(),
						),
					)

					if specified {
						// retry after is specified, we should update
						// client state, so we do not send more requests

						client.SetRetryAfter(retryAfter)
					}

					client.sleep(retryAfter, buf)
					buf.Status = buffer.Retrying
					buf.Attempt++

					// and publish message back
					client.PublishBuffer(buf)
				}
			} else {
				client.Logger.Error("Cannot convert message", zap.Any("msg", msg))
			}
			client.workers.Done()
			client.buffersProcessed.Add(1)
		} else {
			client.buffersProcessed.Add(1)
			break
		}
	}
}

func (client *DataSetClient) bufferSweeper(delay time.Duration) {
	client.Logger.Info("Starting buffer sweeper with delay", zap.Duration("delay", delay))
	for i := 0; ; i++ {
		kept := 0
		swept := 0
		client.Logger.Debug("Buffer sweeping started", zap.Int("sweepId", i))
		client.buffer.Range(func(k, v interface{}) bool {
			buf, ok := v.(*buffer.Buffer)
			if ok {
				if buf.ShouldSendAge(delay) {
					client.PublishBuffer(buf)
					swept++
				} else {
					kept++
				}
			} else {
				client.Logger.Error("Unable to convert message to buffer")
			}
			return true
		})

		// log just every n-th sweep
		lvl := zap.DebugLevel
		if i%100 == 0 {
			lvl = zap.InfoLevel
		}
		client.Logger.Log(
			lvl,
			"Buffer sweeping finished",
			zap.Int("sweepId", i),
			zap.Int("kept", kept),
			zap.Int("swept", swept),
			zap.Int("total", kept+swept),
		)

		time.Sleep(delay)
	}
}

func (client *DataSetClient) PublishBuffer(buf *buffer.Buffer) *buffer.Buffer {
	session := buf.Session
	client.Logger.Debug("publishing buffer", buf.ZapStats()...)
	// lets remove it
	newBuffer := buf.NewEmpty()
	client.buffer.Store(session, newBuffer)

	swapped := atomic.CompareAndSwapUint32(&buf.Status, buffer.Ready, buffer.Publishing)
	if swapped || buf.Status == buffer.Retrying {
		// publish buffer so it can be sent
		client.buffersEnqueued.Add(+1)
		client.PubSub.Pub(buf, session)
	}
	return newBuffer
}

func (client *DataSetClient) shouldRejectNextBatch() error {
	if IsRetryableStatus(client.LastHttpStatus.Load()) {
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

func NewClient(cfg *config.DataSetConfig, client *http.Client, logger *zap.Logger) (*DataSetClient, error) {
	cfg, err := cfg.Update(config.FromEnv())
	if err != nil {
		return nil, fmt.Errorf("it was not possible to update config from env: %w", err)
	}
	logger.Info(
		"Using config",
		zap.String("endpoint", cfg.Endpoint),
		zap.Bool("hasTokenWriteLog", len(cfg.Tokens.WriteLog) > 0),
		zap.Bool("hasTokenReadLog", len(cfg.Tokens.ReadLog) > 0),
		zap.Bool("hasTokenWriteConfig", len(cfg.Tokens.WriteConfig) > 0),
		zap.Bool("hasTokenReadConfig", len(cfg.Tokens.ReadConfig) > 0),
		zap.Int64("maxBufferDelay", cfg.MaxBufferDelay.Milliseconds()),
		zap.Int64("maxPayloadB", cfg.MaxPayloadB),
		zap.Strings("groupBy", cfg.GroupBy),
		zap.Int64("retryBase", cfg.RetryBase.Milliseconds()),
	)

	if cfg.MaxPayloadB > buffer.LimitBufferSize {
		return nil, fmt.Errorf(
			"maxPayloadB has value %d which is more than %d",
			cfg.MaxPayloadB,
			buffer.LimitBufferSize,
		)
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("it was not possible to generate UUID: %w", err)
	}
	dataClient := &DataSetClient{
		Id:             id,
		Config:         cfg,
		Client:         client,
		PubSub:         pubsub.New(0),
		workers:        sync.WaitGroup{},
		LastHttpStatus: atomic.Uint32{},
		retryAfter:     time.Now(),
		retryAfterMu:   sync.RWMutex{},
		lastErrorMu:    sync.RWMutex{},
		Logger:         logger,
	}

	if cfg.MaxBufferDelay > 0 {
		dataClient.Logger.Info("MaxBufferDelay is positive => send buffers regularly",
			zap.Int64("maxDelayMs", cfg.MaxBufferDelay.Milliseconds()),
		)
		go dataClient.bufferSweeper(cfg.MaxBufferDelay)
	} else {
		dataClient.Logger.Warn(
			"MaxBufferDelay is not positive => do NOT send buffers regularly",
			zap.Int64("maxDelayMs", cfg.MaxBufferDelay.Milliseconds()),
		)
	}

	dataClient.Logger.Info("DataSetClient was created",
		zap.String("id", dataClient.Id.String()),
	)

	//// Server for pprof
	//go func() {
	//	fmt.Println(http.ListenAndServe("localhost:6060", nil))
	//}()

	return dataClient, nil
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
	)
	time.Sleep(sleepFor)
}

func (client *DataSetClient) LastError() error {
	client.lastErrorMu.RLock()
	defer client.lastErrorMu.RUnlock()
	return client.lastError
}

func (client *DataSetClient) SetLastError(err error) {
	client.lastErrorMu.Lock()
	defer client.lastErrorMu.Unlock()
	client.lastError = err
}

func (client *DataSetClient) RetryAfter() time.Time {
	client.retryAfterMu.RLock()
	defer client.retryAfterMu.RUnlock()
	return client.retryAfter
}

func (client *DataSetClient) SetRetryAfter(t time.Time) {
	client.retryAfterMu.Lock()
	defer client.retryAfterMu.Unlock()
	client.retryAfter = t
}
