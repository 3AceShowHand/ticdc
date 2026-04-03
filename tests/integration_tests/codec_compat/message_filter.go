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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/simple"
	"go.uber.org/zap"
)

const tidbWatermarkType = "TIDB_WATERMARK"

type CapturedMessage struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
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
		if spec.usesRawEncoding() {
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
		return capturedMessage(key, value), true, nil
	}

	eventType, _ := payload["type"].(string)
	switch strings.ToUpper(eventType) {
	case "INSERT", "UPDATE", "DELETE":
		return capturedMessage(key, value), true, nil
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
	case "c", "u", "d", "":
		return capturedMessage(key, value), true, nil
	case "m":
		return CapturedMessage{}, false, nil
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
		return capturedMessage(key, value), true, nil
	case "WATERMARK", "BOOTSTRAP":
		return CapturedMessage{}, false, nil
	default:
		if _, ok := payload["sql"]; ok {
			return capturedMessage(key, value), true, nil
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
		return capturedMessage(key, value), true, nil
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

	cfg := common.NewConfig(config.ProtocolSimple)
	cfg.EncodingFormat = common.EncodingFormatAvro
	decoder, err := simple.NewDecoder(context.Background(), cfg, nil)
	if err != nil {
		return CapturedMessage{}, false, errors.Trace(err)
	}
	decoder.AddKeyValue(nil, value)
	messageType, hasNext := decoder.HasNext()
	if !hasNext {
		return CapturedMessage{}, false, nil
	}

	switch messageType {
	case common.MessageTypeRow, common.MessageTypeDDL:
		return capturedMessage(nil, value), true, nil
	case common.MessageTypeResolved:
		return CapturedMessage{}, false, nil
	default:
		return CapturedMessage{}, false, nil
	}
}

func filterAvroMessage(key, value []byte) (CapturedMessage, bool, error) {
	if len(key) > 0 {
		return capturedMessage(key, value), true, nil
	}
	if len(value) == 0 {
		return CapturedMessage{}, false, nil
	}

	switch value[0] {
	case 0, 1:
		return capturedMessage(key, value), true, nil
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

func capturedMessage(key, value []byte) CapturedMessage {
	return CapturedMessage{
		Key:   cloneBytes(key),
		Value: cloneBytes(value),
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
