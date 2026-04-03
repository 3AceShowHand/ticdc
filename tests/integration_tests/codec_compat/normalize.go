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
	"encoding/base64"
	"encoding/json"

	"github.com/pingcap/errors"
)

type NormalizedMessage struct {
	Key   any `json:"key"`
	Value any `json:"value"`
}

func NormalizeCapturedMessages(spec protocolSpec, messages []CapturedMessage) ([]NormalizedMessage, error) {
	result := make([]NormalizedMessage, 0, len(messages))
	for _, message := range messages {
		normalized, err := NormalizeCapturedMessage(spec, message)
		if err != nil {
			return nil, err
		}
		result = append(result, normalized)
	}
	return result, nil
}

func NormalizeCapturedMessage(spec protocolSpec, message CapturedMessage) (NormalizedMessage, error) {
	switch {
	case spec.usesOpenProtocol():
		normalized, err := normalizeOpenProtocolMessage(message)
		if err != nil {
			return NormalizedMessage{}, err
		}
		return NormalizedMessage{Value: normalized}, nil
	case spec.usesRawEncoding():
		return NormalizedMessage{
			Key:   normalizeRawBytes(message.Key),
			Value: normalizeRawBytes(message.Value),
		}, nil
	default:
		key, err := NormalizeJSONBytes(message.Key)
		if err != nil {
			return NormalizedMessage{}, errors.Annotate(err, "normalize key")
		}
		value, err := NormalizeJSONBytes(message.Value)
		if err != nil {
			return NormalizedMessage{}, errors.Annotate(err, "normalize value")
		}
		return NormalizedMessage{Key: key, Value: value}, nil
	}
}

func NormalizeJSONBytes(raw []byte) (any, error) {
	return decodeJSONBytes(raw)
}

func decodeJSONBytes(raw []byte) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, errors.Trace(err)
	}
	return value, nil
}

func decodeJSONObject(raw []byte) (map[string]any, error) {
	value, err := decodeJSONBytes(raw)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("json payload is not an object")
	}
	return object, nil
}

func normalizeOpenProtocolMessage(message CapturedMessage) (openProtocolNormalizedMessage, error) {
	decoded, err := decodeOpenProtocolMessage(message.Key, message.Value)
	if err != nil {
		return openProtocolNormalizedMessage{}, errors.Trace(err)
	}

	normalized := openProtocolNormalizedMessage{
		Version: decoded.Version,
		Entries: make([]openProtocolNormalizedEntry, 0, len(decoded.Entries)),
	}
	for _, entry := range decoded.Entries {
		key, err := NormalizeJSONBytes(entry.Key)
		if err != nil {
			return openProtocolNormalizedMessage{}, errors.Annotate(err, "normalize open protocol key")
		}
		value, err := NormalizeJSONBytes(entry.Value)
		if err != nil {
			return openProtocolNormalizedMessage{}, errors.Annotate(err, "normalize open protocol value")
		}
		normalized.Entries = append(normalized.Entries, openProtocolNormalizedEntry{
			Key:   key,
			Value: value,
		})
	}
	return normalized, nil
}

func normalizeRawBytes(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func decodeJSONIntegralField(value any) (int64, error) {
	switch typed := value.(type) {
	case json.Number:
		result, err := typed.Int64()
		return result, errors.Trace(err)
	case int64:
		return typed, nil
	case float64:
		return int64(typed), nil
	default:
		return 0, errors.Errorf("unsupported integral json type %T", value)
	}
}

func CanonicalMessageString(message NormalizedMessage) (string, error) {
	data, err := json.Marshal(message)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(data), nil
}
