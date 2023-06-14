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
	"crypto/md5"
	"encoding/hex"
	"fmt"
)

// EventBundle represents a single DataSet event wrapper structure (see https://app.scalyr.com/help/api#addEvents)
// Event - Zero or more events (log messages) to upload.
// Thread - Optional. Lets you create a readable name for each thread in Event.
// Log - Optional. Lets you set constant metadata, whose value does not change in multiple events in the request.
// see also AddEventsRequest which represent full AddEvent DataSet event wrapper structure
type EventBundle struct {
	Event  *Event
	Thread *Thread
	Log    *Log
}

func (bundle *EventBundle) Key(groupBy []string) string {
	// construct key
	key := ""
	for _, k := range groupBy {
		val, ok := bundle.Event.Attrs[k]
		if ok {
			key += fmt.Sprintf("%s:%s", k, val)
		}
	}

	// use md5 to shorten the key
	hash := md5.Sum([]byte(key))
	bundleKey := hex.EncodeToString(hash[:])

	// add the key as attribute
	bundle.Event.Attrs["bundle_key"] = bundleKey

	// return the key
	return bundleKey
}
