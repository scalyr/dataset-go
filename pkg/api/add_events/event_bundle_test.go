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

type SuiteEventsBundle struct{}

func TestSuiteEventsBundle(t *testing.T) {
	td.NewT(t)
	tdsuite.Run(t, &SuiteEventsBundle{})
}

func (s *SuiteEventsBundle) TestEventBundle(assert, require *td.T) {
	event := &Event{
		Thread: "5",
		Sev:    3,
		Ts:     "0",
		Attrs: map[string]interface{}{
			"foo": "a",
			"bar": "b",
			"baz": "a",
		},
	}
	bundle := &EventBundle{Event: event}

	keyFoo := bundle.Key([]string{"foo"})
	keyBar := bundle.Key([]string{"bar"})
	keyBaz := bundle.Key([]string{"baz"})
	keyNotThere1 := bundle.Key([]string{"notThere1"})
	keyNotThere2 := bundle.Key([]string{"notThere2"})

	assert.Cmp(keyFoo, "ef9faec68698672038857b2647429002")
	assert.Cmp(keyBar, "55a2f7ebf2af8927837c599131d32d07")
	assert.Cmp(keyBaz, "6dd515483537f552fd5fa604cd60f0d9")
	assert.Cmp(keyNotThere1, "d41d8cd98f00b204e9800998ecf8427e")
	assert.Cmp(keyNotThere2, "d41d8cd98f00b204e9800998ecf8427e")

	// although the value is same, key should be different because attributes differ
	assert.Not(keyBaz, keyFoo)
	// non-existing attributes should have the same key
	assert.Cmp(keyNotThere1, keyNotThere2)
}
