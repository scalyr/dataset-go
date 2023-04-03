package response

import (
	"testing"

	"github.com/maxatome/go-testdeep/helpers/tdsuite"
	"github.com/maxatome/go-testdeep/td"
)

type SuiteResponse struct{}

func TestSuiteResponse(t *testing.T) {
	td.NewT(t)
	tdsuite.Run(t, &SuiteResponse{})
}

func (s *SuiteResponse) TestAPIResponseSuccess(assert, require *td.T) {
	success := &APIResponse{Status: "success"}
	assert.Nil(ValidateAPIResponse(success, ""))
}

func (s *SuiteResponse) TestAPIResponseFailed(assert, require *td.T) {
	success := &APIResponse{Status: "meh"}
	assert.NotNil(ValidateAPIResponse(success, ""))
}

func (s *SuiteResponse) TestAPIResponseSuccessWithMessage(assert, require *td.T) {
	success := &APIResponse{Status: "success", Message: "meh"}
	assert.Nil(ValidateAPIResponse(success, ""))
}
