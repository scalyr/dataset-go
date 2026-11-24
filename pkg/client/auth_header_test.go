/*
 * Copyright 2026 SentinelOne, Inc.
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
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/scalyr/dataset-go/pkg/api/add_events"
	"github.com/scalyr/dataset-go/pkg/config"
	"github.com/scalyr/dataset-go/pkg/server_host_config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// capturedRequest is what a test server saw, copied out of the handler so the
// test goroutine can assert on it without racing the server.
type capturedRequest struct {
	authorization []string
	contentType   string
	encoding      string
	body          []byte // decompressed
}

type capturingServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []capturedRequest
}

func newCapturingServer(t *testing.T) *capturingServer {
	t.Helper()
	cs := &capturingServer{}
	cs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		cs.record(t, req)
		writeSuccess(t, w)
	}))
	t.Cleanup(cs.Close)
	return cs
}

func (cs *capturingServer) record(t *testing.T, req *http.Request) {
	t.Helper()
	gz, err := gzip.NewReader(req.Body)
	require.NoError(t, err)
	body, err := io.ReadAll(gz)
	require.NoError(t, err)

	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.requests = append(cs.requests, capturedRequest{
		authorization: req.Header.Values("Authorization"),
		contentType:   req.Header.Get("Content-Type"),
		encoding:      req.Header.Get("Content-Encoding"),
		body:          body,
	})
}

func (cs *capturingServer) captured() []capturedRequest {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return append([]capturedRequest(nil), cs.requests...)
}

func writeSuccess(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	payload, err := json.Marshal(map[string]interface{}{"status": "success", "bytesCharged": 42})
	assert.NoError(t, err)
	_, err = w.Write(payload)
	assert.NoError(t, err)
}

func sendOneEventWithToken(t *testing.T, endpoint string, token string) *DataSetClient {
	t.Helper()
	cfg := newDataSetConfig(endpoint, *newBufferSettings(), server_host_config.NewDefaultDataSetServerHostSettings())
	cfg.Tokens = config.DataSetTokens{WriteLog: token}
	sc, err := NewClient(cfg, &http.Client{}, zap.Must(zap.NewDevelopment()), nil, nil)
	require.NoError(t, err)

	event := &add_events.Event{Thread: "5", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "test"}}
	bundle := &add_events.EventBundle{Event: event, Thread: &add_events.Thread{Id: "5", Name: "fred"}}
	require.NoError(t, sc.AddEvents([]*add_events.EventBundle{bundle}))
	// Shutdown waits until every buffer is sent or given up on.
	_ = sc.Shutdown()
	return sc
}

// assertNoTokenInBody checks the decompressed addEvents body carries no
// credential: no "token" key, and the token value nowhere in the bytes.
func assertNoTokenInBody(t *testing.T, body []byte, token string) {
	t.Helper()
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &raw))
	_, hasToken := raw["token"]
	assert.False(t, hasToken, "body must not have a \"token\" key")
	assert.NotContains(t, string(body), token)
}

func TestAddEventsSendsTokenInAuthorizationHeader(t *testing.T) {
	const token = "secret-write-log-token"
	server := newCapturingServer(t)

	sc := sendOneEventWithToken(t, server.URL, token)

	reqs := server.captured()
	require.Len(t, reqs, 1)
	// addEvents API contract: exactly one "Authorization: Bearer <token>".
	assert.Equal(t, []string{"Bearer " + token}, reqs[0].authorization)
	assert.Equal(t, "application/json", reqs[0].contentType)
	assert.Equal(t, "gzip", reqs[0].encoding)
	assertNoTokenInBody(t, reqs[0].body, token)

	var parsed add_events.AddEventsRequest
	require.NoError(t, json.Unmarshal(reqs[0].body, &parsed))
	assert.Empty(t, parsed.Token)
	assert.Len(t, parsed.Events, 1)

	assert.Equal(t, 1.0, sc.Statistics().Events.SuccessRate())
}

func TestAddEventsTokenEdgeCasesArriveVerbatimInHeader(t *testing.T) {
	tests := []struct {
		name  string
		token string
	}{
		{name: "json metacharacters", token: `tok"en\",\"events\":[{}]`},
		{name: "unicode", token: "tökén-✓"},
		{name: "long", token: strings.Repeat("A", 512)},
		{name: "single char", token: "§"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newCapturingServer(t)

			sendOneEventWithToken(t, server.URL, tt.token)

			reqs := server.captured()
			require.Len(t, reqs, 1)
			assert.Equal(t, []string{"Bearer " + tt.token}, reqs[0].authorization)
			assertNoTokenInBody(t, reqs[0].body, tt.token)
		})
	}
}

// A token with CR/LF must never be written onto the wire, where it would
// inject extra headers. net/http refuses to send it; the failure has to reach
// the caller instead of disappearing.
func TestAddEventsTokenWithNewlineIsRejected(t *testing.T) {
	hits := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		hits.Add(1)
		writeSuccess(t, w)
	}))
	defer server.Close()

	sc := sendOneEventWithToken(t, server.URL, "token\r\nX-Injected: yes")

	assert.Equal(t, int32(0), hits.Load(), "request with an invalid header must not be sent")
	require.Error(t, sc.LastError())
	assert.Contains(t, sc.LastError().Error(), "invalid header field value")
	assert.Equal(t, uint64(1), sc.Statistics().Buffers.Dropped())
}

// Regression test for DPDV-20057: the endpoint redirects (307 replays the
// body) to a different host. That host must receive neither the
// Authorization header nor the token in the body.
//
// The redirect target uses "localhost" while the origin is "127.0.0.1":
// net/http compares host names, not ports, when deciding whether to strip
// Authorization, so redirecting between two 127.0.0.1 ports would keep it.
func TestAddEventsRedirectToOtherHostDoesNotLeakToken(t *testing.T) {
	const token = "secret-write-log-token"
	exfil := newCapturingServer(t)
	exfilURL, err := url.Parse(exfil.URL)
	require.NoError(t, err)
	exfilURL.Host = "localhost:" + exfilURL.Port()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, exfilURL.String()+req.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	sendOneEventWithToken(t, origin.URL, token)

	reqs := exfil.captured()
	require.Len(t, reqs, 1, "redirect must have been followed for this test to prove anything")
	assert.Empty(t, reqs[0].authorization)
	assertNoTokenInBody(t, reqs[0].body, token)
}
