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
	"encoding/json"

	"github.com/pingcap/errors"
)

type NormalizedMessage struct {
	Key   any `json:"key"`
	Value any `json:"value"`
}

func NormalizeCapturedMessages(messages []CapturedMessage) ([]NormalizedMessage, error) {
	result := make([]NormalizedMessage, 0, len(messages))
	for _, message := range messages {
		normalized, err := NormalizeCapturedMessage(message)
		if err != nil {
			return nil, err
		}
		result = append(result, normalized)
	}
	return result, nil
}

func NormalizeCapturedMessage(message CapturedMessage) (NormalizedMessage, error) {
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

func NormalizeJSONBytes(raw []byte) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, errors.Trace(err)
	}

	return normalizeJSONValue(value), nil
}

func normalizeJSONValue(value any) any {
	switch typed := value.(type) {
	case []any:
		result := make([]any, 0, len(typed))
		for _, item := range typed {
			result = append(result, normalizeJSONValue(item))
		}
		return result
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[key] = normalizeJSONValue(item)
		}
		return result
	default:
		return typed
	}
}

func CanonicalMessageString(message NormalizedMessage) (string, error) {
	data, err := json.Marshal(message)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(data), nil
}
