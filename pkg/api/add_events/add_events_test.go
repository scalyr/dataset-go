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

package add_events

import (
	"testing"

	"github.com/maxatome/go-testdeep/helpers/tdsuite"
	"github.com/maxatome/go-testdeep/td"
)

type SuiteAddEvents struct{}

func TestSuiteAddEvents(t *testing.T) {
	td.NewT(t)
	tdsuite.Run(t, &SuiteAddEvents{})
}

func (s *SuiteAddEvents) TestTrimAttrsKeepEverything(assert, require *td.T) {
	attrs := map[string]interface{}{
		"foo": "aaaaaa",
		"bar": "bbbb",
		"baz": "cc",
	}

	assert.Cmp(TrimAttrs(attrs, 10000), attrs)
}

func (s *SuiteAddEvents) TestTrimAttrsTrimLongest(assert, require *td.T) {
	attrs := map[string]interface{}{
		"foo": "aaaaaa",
		"bar": "bbbb",
		"baz": "cc",
	}

	expected := map[string]interface{}{
		"foo": "aaaa",
		"bar": "bbbb",
		"baz": "cc",
	}

	assert.Cmp(TrimAttrs(attrs, 44), expected)
}

func (s *SuiteAddEvents) TestTrimAttrsSkipLongest(assert, require *td.T) {
	attrs := map[string]interface{}{
		"foo": "aaaaaa",
		"bar": "bbbb",
		"baz": "cc",
	}

	expected := map[string]interface{}{
		"bar": "bbbb",
		"baz": "cc",
	}

	assert.Cmp(TrimAttrs(attrs, 36), expected)
}

func (s *SuiteAddEvents) TestTrimAttrsSkipLongestAndTrim(assert, require *td.T) {
	attrs := map[string]interface{}{
		"foo": "aaaaaa",
		"bar": "bbbb",
		"baz": "cc",
	}

	expected := map[string]interface{}{
		"bar": "bb",
		"baz": "cc",
	}

	assert.Cmp(TrimAttrs(attrs, 26), expected)
}

func (s *SuiteAddEvents) TestTrimAttrsSkipLongestAndTrimFully(assert, require *td.T) {
	attrs := map[string]interface{}{
		"foo": "aaaaaa",
		"bar": "bbbb",
		"baz": "cc",
	}

	expected := map[string]interface{}{
		"bar": "",
		"baz": "cc",
	}

	assert.Cmp(TrimAttrs(attrs, 24), expected)
}

func (s *SuiteAddEvents) TestTrimAttrsSkipAll(assert, require *td.T) {
	attrs := map[string]interface{}{
		"foo": "aaaaaa",
		"bar": "bbbb",
		"baz": "cc",
	}

	expected := map[string]interface{}{}

	assert.Cmp(TrimAttrs(attrs, 1), expected)
}
