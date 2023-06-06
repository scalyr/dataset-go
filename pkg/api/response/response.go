package response

import (
	"fmt"
	"net/http"
)

type APIResponse struct {
	Message     string `json:"message"`
	Status      string `json:"status"`
	ResponseObj *http.Response
}

func (response *APIResponse) SetResponse(resp *http.Response) {
	response.ResponseObj = resp
}

type ResponseSetter interface {
	SetResponse(resp *http.Response)
}

type APITokenForDelegatingAccountRequest struct {
	DelegatingAccount string `json:"delegatingAccount"`
	TokenType         string `json:"logRead"`
}

func ValidateAPIResponse(response *APIResponse, message string) error {
	if response.Status != "success" {
		return fmt.Errorf("API Failure: %v - %v", message, response.Message)
	}
	return nil
}
