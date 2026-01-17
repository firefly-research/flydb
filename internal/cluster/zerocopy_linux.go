//go:build linux
// +build linux

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
)

// sendfileImpl implements zero-copy file transfer using Linux sendfile()
func (zcm *ZeroCopyManager) sendfileImpl(sockfd int, filefd int, size int64) (int64, error) {
	var offset int64 = 0
	var sent int64 = 0
	
	for sent < size {
		remaining := size - sent
		
		// sendfile can transfer up to 2GB at a time
		chunkSize := remaining
		if chunkSize > 1<<30 { // 1GB chunks
			chunkSize = 1 << 30
		}
		
		n, err := syscall.Sendfile(sockfd, filefd, &offset, int(chunkSize))
		if err != nil {
			if err == syscall.EAGAIN || err == syscall.EINTR {
				continue // Retry
			}
			return sent, fmt.Errorf("sendfile failed: %w", err)
		}
		
		if n == 0 {
			break // EOF
		}
		
		sent += int64(n)
	}
	
	return sent, nil
}

// Splice transfers data between two file descriptors without copying to userspace
func (zcm *ZeroCopyManager) Splice(infd int, outfd int, size int64) (int64, error) {
	var sent int64 = 0
	
	for sent < size {
		remaining := size - sent
		
		// Splice up to 1GB at a time
		chunkSize := remaining
		if chunkSize > 1<<30 {
			chunkSize = 1 << 30
		}
		
		n, err := syscall.Splice(infd, nil, outfd, nil, int(chunkSize), 
			syscall.SPLICE_F_MOVE|syscall.SPLICE_F_MORE)
		if err != nil {
			if err == syscall.EAGAIN || err == syscall.EINTR {
				continue
			}
			return sent, fmt.Errorf("splice failed: %w", err)
		}
		
		if n == 0 {
			break
		}
		
		sent += int64(n)
	}
	
	return sent, nil
}

