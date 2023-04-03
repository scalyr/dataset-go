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
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/maxatome/go-testdeep/helpers/tdsuite"
	"github.com/maxatome/go-testdeep/td"

	"github.com/scalyr/dataset-go/pkg/api/add_events"
	"github.com/scalyr/dataset-go/pkg/buffer"
	"github.com/scalyr/dataset-go/pkg/config"
	"go.uber.org/zap"
)

type SuiteClient struct{}

func (s *SuiteClient) PreTest(t *td.T, testName string) error {
	os.Clearenv()
	return nil
}

func (s *SuiteClient) PostTest(t *td.T, testName string) error {
	os.Clearenv()
	return nil
}

func (s *SuiteClient) Destroy(t *td.T) error {
	os.Clearenv()
	return nil
}

func TestSuiteClient(t *testing.T) {
	td.NewT(t)
	tdsuite.Run(t, &SuiteClient{})
}

func (s *SuiteClient) TestNewClient(assert, require *td.T) {
	assert.Setenv("SCALYR_SERVER", "test")
	assert.Setenv("SCALYR_WRITELOG_TOKEN", "writelog")
	assert.Setenv("SCALYR_READLOG_TOKEN", "readlog")
	assert.Setenv("SCALYR_WRITECONFIG_TOKEN", "writeconfig")
	assert.Setenv("SCALYR_READCONFIG_TOKEN", "readconfig")
	sc4, err := NewClient(&config.DataSetConfig{}, nil, zap.Must(zap.NewDevelopment()))
	assert.Nil(err)
	assert.Cmp(sc4.Config.Tokens.ReadLog, "readlog")
	assert.Cmp(sc4.Config.Tokens.WriteLog, "writelog")
	assert.Cmp(sc4.Config.Tokens.ReadConfig, "readconfig")
	assert.Cmp(sc4.Config.Tokens.WriteConfig, "writeconfig")
}

func (s *SuiteClient) TestClientBuffer(assert, require *td.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := fmt.Fprintln(w, "{ \"hello\": \"yo\" }")
		assert.Nil(err)
	}))

	token := "token-test"
	sc, err := NewClient(&config.DataSetConfig{
		Endpoint: ts.URL,
		Tokens:   config.DataSetTokens{WriteLog: token},
	}, &http.Client{}, zap.Must(zap.NewDevelopment()))
	assert.Nil(err)

	sessionInfo := add_events.SessionInfo{
		ServerId:   "serverId",
		ServerType: "testing",
	}

	event1 := &add_events.Event{
		Thread: "TId",
		Sev:    3,
		Ts:     "1",
		Attrs: map[string]interface{}{
			"message": "test - 1",
			"meh":     1,
		},
	}

	buffer1, err := sc.Buffer("aaa", &sessionInfo)
	assert.Nil(err)
	added, err := buffer1.AddBundle(&add_events.EventBundle{Event: event1})
	assert.Nil(err)
	assert.Cmp(added, buffer.Added)

	payload1, err := buffer1.Payload()
	assert.Nil(err)
	params1 := add_events.AddEventsRequestParams{}
	err = json.Unmarshal(payload1, &params1)
	assert.Nil(err)

	event2 := &add_events.Event{
		Thread: "TId",
		Sev:    3,
		Ts:     "2",
		Attrs: map[string]interface{}{
			"message": "test - 2",
			"meh":     1,
		},
	}

	buffer2 := sc.PublishBuffer(buffer1)
	added2, err := buffer2.AddBundle(&add_events.EventBundle{Event: event2})
	assert.Nil(err)
	assert.Cmp(added2, buffer.Added)

	payload2, err := buffer2.Payload()
	assert.Nil(err)
	params2 := add_events.AddEventsRequestParams{}
	err = json.Unmarshal(payload2, &params2)
	assert.Nil(err)

	assert.Cmp(params1.Token, params2.Token)
	assert.Cmp(params1.Session, params2.Session)
	assert.Cmp(params1.SessionInfo, params2.SessionInfo)

	assert.Not((params1.Events)[0], (params2.Events)[0])
	assert.Cmp((params1.Events)[0].Ts, "1")
	assert.Cmp((params2.Events)[0].Ts, "2")

}
