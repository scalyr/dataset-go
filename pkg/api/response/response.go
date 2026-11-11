package response

import "net/http"

// ApiResponse represents a generic DataSet REST API response
type ApiResponse struct {
	Message     string `json:"message"`
	Status      string `json:"status"`
	ResponseObj *http.Response
}

func (response *ApiResponse) SetResponseObj(resp *http.Response) {
	response.ResponseObj = resp
}

type ResponseObjSetter interface {
	SetResponseObj(resp *http.Response)
}

type APITokenForDelegatingAccountRequest struct {
	DelegatingAccount string `json:"delegatingAccount"`
	TokenType         string `json:"logRead"`
}
