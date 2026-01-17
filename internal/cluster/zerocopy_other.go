//go:build !linux && !darwin
// +build !linux,!darwin

/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster

import (
	"fmt"
)

// sendfileImpl fallback for platforms without sendfile support
func (zcm *ZeroCopyManager) sendfileImpl(sockfd int, filefd int, size int64) (int64, error) {
	return 0, fmt.Errorf("sendfile not supported on this platform")
}

// Splice fallback for platforms without splice support
func (zcm *ZeroCopyManager) Splice(infd int, outfd int, size int64) (int64, error) {
	return 0, fmt.Errorf("splice not supported on this platform")
}

