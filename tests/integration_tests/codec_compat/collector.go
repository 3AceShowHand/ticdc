// Copyright 2026 PingCAP, Inc.
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

package main

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Collector struct {
	client   *kgo.Client
	protocol protocolSpec
	cancel   context.CancelFunc
	done     chan struct{}
	wakeCh   chan struct{}

	mu       sync.Mutex
	buffered []CapturedMessage
	lastErr  error
}

func NewCollector(addrs []string, topic string, spec protocolSpec) (*Collector, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &Collector{
		client:   client,
		protocol: spec,
		cancel:   cancel,
		done:     make(chan struct{}),
		wakeCh:   make(chan struct{}, 1),
	}
	go c.run(ctx)
	return c, nil
}

func (c *Collector) Close() {
	if c == nil {
		return
	}
	c.cancel()
	c.client.Close()
	<-c.done
}

func (c *Collector) Drain(ctx context.Context, idleTimeout time.Duration) ([]CapturedMessage, error) {
	timer := time.NewTimer(idleTimeout)
	defer timer.Stop()

	collected := c.takeBuffered()
	for {
		if err := c.err(); err != nil {
			return nil, err
		}

		select {
		case <-ctx.Done():
			return nil, errors.Trace(ctx.Err())
		case <-timer.C:
			collected = append(collected, c.takeBuffered()...)
			return collected, c.err()
		case <-c.wakeCh:
			collected = append(collected, c.takeBuffered()...)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(idleTimeout)
		}
	}
}

func (c *Collector) run(ctx context.Context) {
	defer close(c.done)

	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}

		fetchErrs := fetches.Errors()
		if len(fetchErrs) > 0 {
			for _, fetchErr := range fetchErrs {
				if isRetriableFetchErr(fetchErr.Err) {
					continue
				}
				c.setErr(fetchErr.Err)
				return
			}
		}

		fetches.EachRecord(func(record *kgo.Record) {
			message, keep, err := filterCapturedMessage(c.protocol, record.Key, record.Value)
			if err != nil {
				c.setErr(err)
				return
			}
			if !keep {
				return
			}
			c.append(message)
		})

		if err := c.err(); err != nil {
			return
		}
	}
}

func (c *Collector) append(message CapturedMessage) {
	c.mu.Lock()
	c.buffered = append(c.buffered, message)
	c.mu.Unlock()

	select {
	case c.wakeCh <- struct{}{}:
	default:
	}
}

func (c *Collector) setErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastErr == nil {
		c.lastErr = errors.Trace(err)
	}
}

func (c *Collector) err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastErr
}

func (c *Collector) takeBuffered() []CapturedMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.buffered) == 0 {
		return nil
	}
	out := make([]CapturedMessage, len(c.buffered))
	copy(out, c.buffered)
	c.buffered = c.buffered[:0]
	return out
}

func isRetriableFetchErr(err error) bool {
	switch err {
	case kerr.UnknownTopicOrPartition,
		kerr.LeaderNotAvailable,
		kerr.NotLeaderForPartition:
		return true
	default:
		return false
	}
}
