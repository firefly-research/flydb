//go:build darwin
// +build darwin

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
	"syscall"
	"unsafe"
)

// sendfileImpl implements zero-copy file transfer using macOS sendfile()
// Note: macOS sendfile has different signature than Linux
func (zcm *ZeroCopyManager) sendfileImpl(sockfd int, filefd int, size int64) (int64, error) {
	var offset int64 = 0
	var sent int64 = 0
	
	for sent < size {
		remaining := size - sent
		
		// sendfile can transfer up to 2GB at a time
		chunkSize := remaining
		if chunkSize > 1<<30 {
			chunkSize = 1 << 30
		}
		
		// macOS sendfile: sendfile(fd, s, offset, len, hdtr, flags)
		var sbytes int64 = chunkSize
		
		_, _, errno := syscall.Syscall9(
			syscall.SYS_SENDFILE,
			uintptr(filefd),
			uintptr(sockfd),
			uintptr(offset),
			uintptr(unsafe.Pointer(&sbytes)),
			0, // hdtr
			0, // flags
			0, 0, 0,
		)
		
		if errno != 0 {
			if errno == syscall.EAGAIN || errno == syscall.EINTR {
				continue
			}
			return sent, fmt.Errorf("sendfile failed: %w", errno)
		}
		
		if sbytes == 0 {
			break
		}
		
		sent += sbytes
		offset += sbytes
	}
	
	return sent, nil
}

// Splice is not available on macOS, fallback to regular copy
func (zcm *ZeroCopyManager) Splice(infd int, outfd int, size int64) (int64, error) {
	return 0, fmt.Errorf("splice not supported on macOS")
}

