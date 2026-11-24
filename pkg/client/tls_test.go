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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scalyr/dataset-go/pkg/api/add_events"
	"github.com/scalyr/dataset-go/pkg/buffer_config"
	"github.com/scalyr/dataset-go/pkg/server_host_config"
	"github.com/scalyr/dataset-go/pkg/statistics"
)

// The certificates are generated for each run rather than checked in: checked-in
// certificates expire (and then the valid control fails on its own), and their
// private keys trip secret scanners.

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func (ca *testCA) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(ca.cert)
	return p
}

var serialNumber atomic.Int64

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	return key
}

func newTestCA(t *testing.T, name string) *testCA {
	t.Helper()
	key := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serialNumber.Add(1)),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return &testCA{cert: cert, key: key}
}

type leafOptions struct {
	dnsNames  []string
	ips       []net.IP
	notBefore time.Time
	notAfter  time.Time
}

func validLeafOptions() leafOptions {
	return leafOptions{
		dnsNames:  []string{"localhost"},
		ips:       []net.IP{net.IPv4(127, 0, 0, 1)},
		notBefore: time.Now().Add(-time.Hour),
		notAfter:  time.Now().Add(time.Hour),
	}
}

// newLeaf returns a server certificate signed by ca, or a self-signed one when ca is nil.
func newLeaf(t *testing.T, ca *testCA, opts leafOptions) tls.Certificate {
	t.Helper()
	key := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serialNumber.Add(1)),
		Subject:      pkix.Name{CommonName: "dataset-go test server"},
		DNSNames:     opts.dnsNames,
		IPAddresses:  opts.ips,
		NotBefore:    opts.notBefore,
		NotAfter:     opts.notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	parent, signerKey := tmpl, key
	if ca != nil {
		parent, signerKey = ca.cert, ca.key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, signerKey)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// startTLSServer serves handler on 127.0.0.1 with cert and counts the requests that reach it.
func startTLSServer(t *testing.T, cert tls.Certificate, handler http.HandlerFunc) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	hits := &atomic.Int32{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		handler(w, r)
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	// The handshake failures are expected; keep them out of the test output.
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, hits
}

func httpClientTrusting(pool *x509.CertPool) *http.Client {
	return &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	}}
}

func respondSuccess(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte(`{"status":"success","bytesCharged":42}`))
}

type sendResult struct {
	err            error // returned by Shutdown
	stats          *statistics.ExportedStatistics
	lastHttpStatus uint32
}

// sendOneEvent sends one event through a new client and shuts it down.
// Every failure is retried (600/601 are treated as 5xx), so the retry window is
// the smallest the config accepts: each failing case takes about a second.
func sendOneEvent(t *testing.T, url string, httpClient *http.Client) sendResult {
	t.Helper()
	cfg := newDataSetConfig(url, *newBufferSettings(
		buffer_config.WithRetryInitialInterval(buffer_config.MinimalInitialInterval),
		buffer_config.WithRetryMaxInterval(buffer_config.MinimalMaxInterval),
		buffer_config.WithRetryMaxElapsedTime(buffer_config.MinimalMaxElapsedTime),
		buffer_config.WithRetryShutdownTimeout(5*time.Second),
	), server_host_config.NewDefaultDataSetServerHostSettings())
	sc, err := NewClient(cfg, httpClient, zap.NewNop(), nil, nil)
	require.NoError(t, err)

	err = sc.AddEvents([]*add_events.EventBundle{{
		Event:  &add_events.Event{Thread: "T", Sev: 3, Ts: "0", Attrs: map[string]interface{}{"message": "tls test"}},
		Thread: &add_events.Thread{Id: "T", Name: "thread"},
	}})
	require.NoError(t, err)
	shutdownErr := sc.Shutdown()
	stats := sc.Statistics()
	require.NotNil(t, stats)
	return sendResult{err: shutdownErr, stats: stats, lastHttpStatus: sc.LastHttpStatus.Load()}
}

// TestTLSCertificateValidation checks that the client rejects bad server
// certificates with the specific x509 error, and never sends the payload.
func TestTLSCertificateValidation(t *testing.T) {
	ca := newTestCA(t, "dataset-go test CA")
	otherCA := newTestCA(t, "dataset-go other CA")

	expired := validLeafOptions()
	expired.notBefore = time.Now().Add(-2 * time.Hour)
	expired.notAfter = time.Now().Add(-time.Hour)

	notYetValid := validLeafOptions()
	notYetValid.notBefore = time.Now().Add(time.Hour)
	notYetValid.notAfter = time.Now().Add(2 * time.Hour)

	wrongHost := validLeafOptions()
	wrongHost.dnsNames = []string{"wrong.host.invalid"}
	wrongHost.ips = nil

	tests := []struct {
		name       string
		cert       tls.Certificate
		httpClient *http.Client
		// checkErr asserts the x509 error found in the error chain.
		checkErr func(t *testing.T, err error)
	}{
		{
			name:       "expired",
			cert:       newLeaf(t, ca, expired),
			httpClient: httpClientTrusting(ca.pool()),
			checkErr:   requireCertificateInvalid(x509.Expired),
		},
		{
			name:       "not yet valid",
			cert:       newLeaf(t, ca, notYetValid),
			httpClient: httpClientTrusting(ca.pool()),
			checkErr:   requireCertificateInvalid(x509.Expired),
		},
		{
			name:       "wrong host",
			cert:       newLeaf(t, ca, wrongHost),
			httpClient: httpClientTrusting(ca.pool()),
			checkErr: func(t *testing.T, err error) {
				var hostErr x509.HostnameError
				require.ErrorAs(t, err, &hostErr)
				assert.Equal(t, "127.0.0.1", hostErr.Host)
			},
		},
		{
			name:       "self-signed",
			cert:       newLeaf(t, nil, validLeafOptions()),
			httpClient: httpClientTrusting(ca.pool()),
			checkErr:   requireUnknownAuthority,
		},
		{
			name:       "signed by an untrusted CA",
			cert:       newLeaf(t, otherCA, validLeafOptions()),
			httpClient: httpClientTrusting(ca.pool()),
			checkErr:   requireUnknownAuthority,
		},
		{
			// What the exporter and examples/readme pass: a plain client using the
			// system roots. The library must not loosen verification. The platform
			// verifiers on macOS and Windows return their own error types, so only
			// the verification failure itself is asserted.
			name:       "default client rejects a private CA",
			cert:       newLeaf(t, ca, validLeafOptions()),
			httpClient: &http.Client{},
			checkErr: func(t *testing.T, err error) {
				var verErr *tls.CertificateVerificationError
				require.ErrorAs(t, err, &verErr)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			server, hits := startTLSServer(t, tt.cert, respondSuccess)

			res := sendOneEvent(t, server.URL, tt.httpClient)

			require.Error(t, res.err)
			tt.checkErr(t, res.err)
			assert.ErrorContains(t, res.err, errMsgUnableToSentRequest)
			assert.Equal(t, uint32(HttpErrorCannotConnect), res.lastHttpStatus)
			assert.Equal(t, int32(0), hits.Load(), "request must not get past the TLS handshake")
			assert.Equal(t, uint64(1), res.stats.Buffers.Dropped())
			assert.Equal(t, uint64(0), res.stats.Buffers.Processed())
		})
	}
}

func requireCertificateInvalid(reason x509.InvalidReason) func(t *testing.T, err error) {
	return func(t *testing.T, err error) {
		var invalidErr x509.CertificateInvalidError
		require.ErrorAs(t, err, &invalidErr)
		assert.Equal(t, reason, invalidErr.Reason)
	}
}

func requireUnknownAuthority(t *testing.T, err error) {
	var authErr x509.UnknownAuthorityError
	require.ErrorAs(t, err, &authErr)
}

// TestTLSValidCertificate is the control: a certificate the client trusts must
// pass the TLS layer and deliver a well-formed addEvents request.
func TestTLSValidCertificate(t *testing.T) {
	ca := newTestCA(t, "dataset-go test CA")

	for _, host := range []string{"127.0.0.1", "localhost"} {
		t.Run(host, func(t *testing.T) {
			t.Parallel()
			var received atomic.Value
			server, hits := startTLSServer(t, newLeaf(t, ca, validLeafOptions()), func(w http.ResponseWriter, r *http.Request) {
				req, err := extract(r)
				if assert.NoError(t, err) {
					received.Store(req)
				}
				respondSuccess(w, r)
			})
			_, port, err := net.SplitHostPort(server.Listener.Addr().String())
			require.NoError(t, err)

			res := sendOneEvent(t, "https://"+net.JoinHostPort(host, port), httpClientTrusting(ca.pool()))

			require.NoError(t, res.err)
			assert.Equal(t, int32(1), hits.Load())
			req, ok := received.Load().(add_events.AddEventsRequest)
			require.True(t, ok, "server did not receive a parseable addEvents request")
			require.Len(t, req.Events, 1)
			assert.Equal(t, "tls test", req.Events[0].Attrs["message"])
			assert.Equal(t, uint32(http.StatusOK), res.lastHttpStatus)
			assert.Equal(t, uint64(1), res.stats.Buffers.Processed())
			assert.Equal(t, uint64(0), res.stats.Buffers.Dropped())
		})
	}
}

// TestTLSServerResponses covers the two non-certificate cases the old
// badssl.com script checked (sentinelone.com and app.scalyr.com) over a
// trusted TLS connection.
func TestTLSServerResponses(t *testing.T) {
	ca := newTestCA(t, "dataset-go test CA")

	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr []string
	}{
		{
			name: "non-JSON response",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte("<!DOCTYPE html><html><body>Not Found</body></html>"))
			},
			wantErr: []string{errMsgUnableToParseResponse, "status: 404", "<!DOCTYPE html>"},
		},
		{
			name: "token rejected",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"status":"error/client/badParam","message":"Couldn't decode API token"}`))
			},
			wantErr: []string{"some buffers were dropped during finishing - 1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			server, hits := startTLSServer(t, newLeaf(t, ca, validLeafOptions()), tt.handler)

			res := sendOneEvent(t, server.URL, httpClientTrusting(ca.pool()))

			require.Error(t, res.err)
			for _, want := range tt.wantErr {
				assert.ErrorContains(t, res.err, want)
			}
			var verErr *tls.CertificateVerificationError
			assert.False(t, errors.As(res.err, &verErr), "TLS must succeed: %v", res.err)
			assert.NotContains(t, res.err.Error(), errMsgUnableToSentRequest)
			assert.Positive(t, hits.Load())
			assert.Equal(t, uint64(1), res.stats.Buffers.Dropped())
		})
	}
}
