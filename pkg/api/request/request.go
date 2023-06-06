package request

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/scalyr/dataset-go/pkg/config"

	"github.com/scalyr/dataset-go/pkg/version"
)

const UserAgent = "datasetexporter/" + version.Version + " (" + version.ReleasedDate + ")"

func (ap *AuthParams) setToken(token string) {
	ap.Token = token
}

type AuthParams struct {
	Token   string `json:"token,omitempty"`
	Message string `json:"message,omitempty"`
}

type TokenSetter interface {
	setToken(token string)
}

// TODO document this struct
type APIRequest struct {
	requestType   string
	payload       []byte
	request       interface{}
	uri           string
	apiKey        string
	supportedKeys []string
	err           error
}

func NewRequest(requestType string, uri string) *APIRequest {
	return &APIRequest{requestType: requestType, uri: uri}
}

func (r *APIRequest) WithWriteLog(tokens config.DataSetTokens) *APIRequest {
	if r.apiKey != "" {
		return r
	}

	if tokens.WriteLog != "" {
		r.apiKey = tokens.WriteLog
	} else {
		r.supportedKeys = append(r.supportedKeys, "WriteLog")
	}
	return r
}

func (r *APIRequest) WithReadLog(tokens config.DataSetTokens) *APIRequest {
	if r.apiKey != "" {
		return r
	}

	if tokens.ReadLog != "" {
		r.apiKey = tokens.ReadLog
	} else {
		r.supportedKeys = append(r.supportedKeys, "ReadLog")
	}
	return r
}

func (r *APIRequest) WithReadConfig(tokens config.DataSetTokens) *APIRequest {
	if r.apiKey != "" {
		return r
	}

	if tokens.ReadConfig != "" {
		r.apiKey = tokens.ReadConfig
	} else {
		r.supportedKeys = append(r.supportedKeys, "ReadConfig")
	}
	return r
}

func (r *APIRequest) WithWriteConfig(tokens config.DataSetTokens) *APIRequest {
	if r.apiKey != "" {
		return r
	}

	if tokens.WriteConfig != "" {
		r.apiKey = tokens.WriteConfig
	} else {
		r.supportedKeys = append(r.supportedKeys, "WriteConfig")
	}
	return r
}

func (r *APIRequest) JsonRequest(request TokenSetter) *APIRequest {
	payload, err := json.Marshal(request)
	r.request = request
	if err != nil {
		r.err = err
		return r
	}
	r.payload = payload
	return r
}

func (r *APIRequest) RawRequest(payload []byte) *APIRequest {
	r.payload = payload
	return r
}

func (r *APIRequest) emptyRequest() *APIRequest {
	return r.JsonRequest(TokenSetter(&AuthParams{}))
}

func (r *APIRequest) HttpRequest() (*http.Request, error) {
	if r.err != nil {
		return nil, r.err
	}

	if r.payload == nil || len(r.payload) == 0 {
		r.emptyRequest()
	}

	if r.apiKey == "" && len(r.supportedKeys) > 0 {
		return nil, fmt.Errorf("no API Key Found - Supported Tokens for %v are %v", r.uri, r.supportedKeys)
	} else if r.request != nil {
		r.request.(TokenSetter).setToken(r.apiKey)
	}

	var err error
	var buf bytes.Buffer
	g := gzip.NewWriter(&buf)
	if _, err = g.Write(r.payload); err != nil {
		r.err = fmt.Errorf("cannot compress payload: %w", err)
		return nil, r.err
	}
	if err = g.Close(); err != nil {
		r.err = fmt.Errorf("cannot finish compression: %w", err)
		return nil, r.err
	}

	req, err := http.NewRequest(r.requestType, r.uri, &buf)
	if err != nil {
		r.err = fmt.Errorf("failed to create NewRequest: %w", err)
		return nil, r.err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Encoding", "gzip")
	req.Header.Add("User-Agent", UserAgent)

	return req, nil
}
