// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package flowcontrol

import (
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// memoryQuota is used to track the memory usage.
type memoryQuota struct {
	capacity uint64
	used     atomic.Uint64
	stopped  atomic.Bool

	blockAcquireCond *sync.Cond
}

// quota: max advised memory consumption in bytes.
func newMemoryQuota(quota uint64) *memoryQuota {
	return &memoryQuota{
		capacity:         quota,
		blockAcquireCond: sync.NewCond(&sync.Mutex{}),
	}
}

// BlockAcquire is used to block the request when the memory quota is not available.
func (m *memoryQuota) blockAcquire(nBytes uint64) error {
	for {
		if m.stopped.Load() {
			return errors.ErrMemoryQuotaAborted.GenWithStackByArgs()
		}
		inuse := m.used.Load()
		if inuse+nBytes > m.capacity {
			m.blockAcquireCond.L.Lock()
			m.blockAcquireCond.Wait()
			m.blockAcquireCond.L.Unlock()
			continue
		}
		if m.used.CompareAndSwap(inuse, inuse+nBytes) {
			return nil
		}
	}
}

// forceConsume is called when blocking is not acceptable and the limit can be violated
// for the sake of avoid deadlock. It merely records the increased memory consumption.
func (m *memoryQuota) forceAcquire(nBytes uint64) error {
	if m.stopped.Load() {
		return errors.ErrMemoryQuotaAborted.GenWithStackByArgs()
	}
	m.used.Add(nBytes)
	return nil
}

// release is called when a chuck of memory is done being used.
func (m *memoryQuota) release(nBytes uint64) {
	if nBytes == 0 {
		return
	}
	inuse := m.used.Load()
	if inuse < nBytes {
		log.Panic("memory quota release failed, since used bytes is less than released bytes",
			zap.Uint64("inuse", inuse), zap.Uint64("releasing", nBytes))
	}

	if m.used.Sub(nBytes) < m.capacity {
		// todo: Broadcast or Signal?
		m.blockAcquireCond.Broadcast()
	}
}

// abort interrupts any ongoing consumeWithBlocking call
func (m *memoryQuota) close() {
	if m.stopped.CompareAndSwap(false, true) {
		m.blockAcquireCond.Broadcast()
	}
}

// getConsumption returns the current memory consumption
func (m *memoryQuota) getUsed() uint64 {
	return m.used.Load()
}

func (m *memoryQuota) getAvailable() uint64 {
	if m.stopped.Load() {
		return 0
	}
	return m.capacity - m.used.Load()
}
