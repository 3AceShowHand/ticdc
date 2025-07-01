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
	capacity  uint64
	isAborted atomic.Bool

	consumed struct {
		sync.Mutex
		bytes uint64
	}
	consumedCond *sync.Cond
}

// quota: max advised memory consumption in bytes.
func newMemoryQuota(quota uint64) *memoryQuota {
	ret := &memoryQuota{
		capacity: quota,
	}

	ret.consumedCond = sync.NewCond(&ret.consumed)
	return ret
}

// consumeWithBlocking is called when a hard-limit is needed. The method will
// block until enough memory has been freed up by release.
// blockCallBack will be called if the function will block.
// Should be used with care to prevent deadlock.
func (c *memoryQuota) acquire(nBytes uint64, blockCallBack func() error) error {
	if nBytes >= c.capacity {
		// todo: can we simply force acquire here?
		return errors.ErrAcquireLargerThanMemoryQuota.GenWithStackByArgs(nBytes, c.capacity)
	}

	c.consumed.Lock()
	if c.consumed.bytes+nBytes >= c.capacity {
		c.consumed.Unlock()
		err := blockCallBack()
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		c.consumed.Unlock()
	}

	c.consumed.Lock()
	defer c.consumed.Unlock()

	for {
		if c.isAborted.Load() {
			return errors.ErrMemoryQuotaAborted.GenWithStackByArgs()
		}

		if c.consumed.bytes+nBytes < c.capacity {
			break
		}
		c.consumedCond.Wait()
	}

	c.consumed.bytes += nBytes
	return nil
}

// forceConsume is called when blocking is not acceptable and the limit can be violated
// for the sake of avoid deadlock. It merely records the increased memory consumption.
func (c *memoryQuota) forceAcquire(nBytes uint64) error {
	c.consumed.Lock()
	defer c.consumed.Unlock()

	if c.isAborted.Load() {
		return errors.ErrMemoryQuotaAborted.GenWithStackByArgs()
	}

	c.consumed.bytes += nBytes
	return nil
}

// release is called when a chuck of memory is done being used.
func (c *memoryQuota) release(nBytes uint64) {
	c.consumed.Lock()

	if c.consumed.bytes < nBytes {
		c.consumed.Unlock()
		log.Panic("memoryQuota: releasing more than consumed, report a bug",
			zap.Uint64("consumed", c.consumed.bytes),
			zap.Uint64("released", nBytes))
	}

	c.consumed.bytes -= nBytes
	if c.consumed.bytes < c.capacity {
		c.consumed.Unlock()
		c.consumedCond.Signal()
		return
	}

	c.consumed.Unlock()
}

// abort interrupts any ongoing consumeWithBlocking call
func (c *memoryQuota) abort() {
	c.isAborted.Store(true)
	c.consumedCond.Signal()
}

// getConsumption returns the current memory consumption
func (c *memoryQuota) getConsumption() uint64 {
	c.consumed.Lock()
	defer c.consumed.Unlock()

	return c.consumed.bytes
}
