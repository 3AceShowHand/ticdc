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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	configpkg "github.com/pingcap/ticdc/pkg/config"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	simplecodec "github.com/pingcap/ticdc/pkg/sink/codec/simple"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

const tidbWatermarkType = "TIDB_WATERMARK"

type CapturedMessage struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

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

func NewCollector(addrs []string, topic string, requestTimeout time.Duration, spec protocolSpec) (*Collector, error) {
	_ = requestTimeout
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

func filterCapturedMessage(spec protocolSpec, key, value []byte) (CapturedMessage, bool, error) {
	switch spec.protocol {
	case protocolCanalJSON:
		return filterCanalJSONMessage(key, value)
	case protocolDebezium:
		return filterDebeziumMessage(key, value)
	case protocolOpen:
		return filterOpenProtocolMessage(key, value)
	case protocolSimple:
		if spec.encodingFormat == encodingFormatAvro {
			return filterSimpleAvroMessage(value)
		}
		return filterSimpleJSONMessage(key, value)
	case protocolAvro:
		return filterAvroMessage(key, value)
	default:
		return CapturedMessage{}, false, errors.Errorf("unsupported protocol %s", spec.protocol)
	}
}

func filterCanalJSONMessage(key, value []byte) (CapturedMessage, bool, error) {
	if len(value) == 0 {
		return CapturedMessage{}, false, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()

	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return CapturedMessage{}, false, errors.Trace(err)
	}

	isDDL, _ := payload["isDdl"].(bool)
	if isDDL {
		return CapturedMessage{Key: cloneBytes(key), Value: cloneBytes(value)}, true, nil
	}

	eventType, _ := payload["type"].(string)
	switch strings.ToUpper(eventType) {
	case "INSERT", "UPDATE", "DELETE":
		return CapturedMessage{Key: cloneBytes(key), Value: cloneBytes(value)}, true, nil
	case tidbWatermarkType:
		return CapturedMessage{}, false, nil
	default:
		log.Info("ignore unsupported canal json message",
			zap.String("eventType", eventType),
			zap.ByteString("value", value))
		return CapturedMessage{}, false, nil
	}
}

func filterDebeziumMessage(key, value []byte) (CapturedMessage, bool, error) {
	if len(value) == 0 {
		return CapturedMessage{}, false, nil
	}

	envelope, err := decodeJSONObject(value)
	if err != nil {
		return CapturedMessage{}, false, errors.Trace(err)
	}
	payload, ok := envelope["payload"].(map[string]any)
	if !ok {
		return CapturedMessage{}, false, errors.New("decode debezium payload failed")
	}

	op, _ := payload["op"].(string)
	switch op {
	case "c", "u", "d":
		return CapturedMessage{Key: cloneBytes(key), Value: cloneBytes(value)}, true, nil
	case "m":
		return CapturedMessage{}, false, nil
	case "":
		return CapturedMessage{Key: cloneBytes(key), Value: cloneBytes(value)}, true, nil
	default:
		log.Info("ignore unsupported debezium message",
			zap.String("op", op),
			zap.ByteString("value", value))
		return CapturedMessage{}, false, nil
	}
}

func filterSimpleJSONMessage(key, value []byte) (CapturedMessage, bool, error) {
	if len(value) == 0 {
		return CapturedMessage{}, false, nil
	}

	payload, err := decodeJSONObject(value)
	if err != nil {
		return CapturedMessage{}, false, errors.Trace(err)
	}
	eventType, _ := payload["type"].(string)
	switch strings.ToUpper(eventType) {
	case "INSERT", "UPDATE", "DELETE":
		return CapturedMessage{Key: cloneBytes(key), Value: cloneBytes(value)}, true, nil
	case "WATERMARK", "BOOTSTRAP":
		return CapturedMessage{}, false, nil
	default:
		if _, ok := payload["sql"]; ok {
			return CapturedMessage{Key: cloneBytes(key), Value: cloneBytes(value)}, true, nil
		}
		log.Info("ignore unsupported simple json message",
			zap.String("eventType", eventType),
			zap.ByteString("value", value))
		return CapturedMessage{}, false, nil
	}
}

func filterOpenProtocolMessage(key, value []byte) (CapturedMessage, bool, error) {
	message, err := decodeOpenProtocolMessage(key, value)
	if err != nil {
		return CapturedMessage{}, false, err
	}
	if len(message.Entries) == 0 {
		return CapturedMessage{}, false, nil
	}

	firstKey, err := decodeJSONObject(message.Entries[0].Key)
	if err != nil {
		return CapturedMessage{}, false, errors.Annotate(err, "decode open protocol key")
	}
	messageType, err := decodeJSONIntegralField(firstKey["t"])
	if err != nil {
		return CapturedMessage{}, false, errors.Annotate(err, "decode open protocol message type")
	}
	switch messageType {
	case openProtocolMessageRow, openProtocolMessageDDL:
		return CapturedMessage{Key: cloneBytes(key), Value: cloneBytes(value)}, true, nil
	case openProtocolMessageMarker:
		return CapturedMessage{}, false, nil
	default:
		log.Info("ignore unsupported open protocol message",
			zap.Int64("messageType", messageType),
			zap.ByteString("key", key))
		return CapturedMessage{}, false, nil
	}
}

func filterSimpleAvroMessage(value []byte) (CapturedMessage, bool, error) {
	if len(value) == 0 {
		return CapturedMessage{}, false, nil
	}

	cfg := codeccommon.NewConfig(configpkg.ProtocolSimple)
	cfg.EncodingFormat = codeccommon.EncodingFormatAvro
	decoder, err := simplecodec.NewDecoder(context.Background(), cfg, nil)
	if err != nil {
		return CapturedMessage{}, false, errors.Trace(err)
	}
	decoder.AddKeyValue(nil, value)
	messageType, hasNext := decoder.HasNext()
	if !hasNext {
		return CapturedMessage{}, false, nil
	}

	switch messageType {
	case codeccommon.MessageTypeRow, codeccommon.MessageTypeDDL:
		return CapturedMessage{Value: cloneBytes(value)}, true, nil
	case codeccommon.MessageTypeResolved:
		return CapturedMessage{}, false, nil
	default:
		return CapturedMessage{}, false, nil
	}
}

func filterAvroMessage(key, value []byte) (CapturedMessage, bool, error) {
	if len(key) > 0 {
		return CapturedMessage{Key: cloneBytes(key), Value: cloneBytes(value)}, true, nil
	}
	if len(value) == 0 {
		return CapturedMessage{}, false, nil
	}

	switch value[0] {
	case 0:
		return CapturedMessage{Key: cloneBytes(key), Value: cloneBytes(value)}, true, nil
	case 1:
		return CapturedMessage{Key: cloneBytes(key), Value: cloneBytes(value)}, true, nil
	case 2:
		if len(value) < 9 {
			return CapturedMessage{}, false, errors.New("invalid avro checkpoint message")
		}
		_ = binary.BigEndian.Uint64(value[1:])
		return CapturedMessage{}, false, nil
	default:
		return CapturedMessage{}, false, errors.Errorf("unsupported avro message prefix %d", value[0])
	}
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
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
