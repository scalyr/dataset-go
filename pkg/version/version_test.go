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

package version

import (
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Version and ReleasedDate are rewritten by scripts/bump-version.sh on every
// release, so these tests check their format, not fixed values.

var semver = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func validDate(s string) bool {
	_, err := time.Parse(time.DateOnly, s)
	return err == nil
}

func TestVersion(t *testing.T) {
	assert.Regexp(t, semver, Version, "Version must be X.Y.Z without a 'v' prefix")
	assert.True(t, validDate(ReleasedDate), "ReleasedDate %q must be YYYY-MM-DD", ReleasedDate)
}

// The checks above must reject what bump-version.sh rejects; otherwise a
// broken constant would pass.
func TestVersionChecks(t *testing.T) {
	for _, v := range []string{"0.21.1", "0.0.0", "10.200.3000"} {
		assert.Regexp(t, semver, v)
	}
	for _, v := range []string{"", "v0.21.1", "0.21", "0.21.1-rc1", "0.21.1.2", "01.2.3", " 0.21.1", "dev"} {
		assert.NotRegexp(t, semver, v)
	}
	for _, d := range []string{"2024-06-27", "2024-02-29"} {
		assert.True(t, validDate(d), d)
	}
	for _, d := range []string{"", "2026-1-11", "11.11.2026", "2026/11/11", "2026-13-01", "2026-02-30", "2025-02-29"} {
		assert.False(t, validDate(d), d)
	}
}
