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
	"encoding/binary"

	"github.com/pingcap/errors"
)

const (
	openProtocolBatchVersion1 = uint64(1)
	openProtocolMessageRow    = int64(1)
	openProtocolMessageDDL    = int64(2)
	openProtocolMessageMarker = int64(3)
)

type openProtocolMessageEntry struct {
	Key   []byte
	Value []byte
}

type openProtocolMessage struct {
	Version uint64
	Entries []openProtocolMessageEntry
}

type openProtocolNormalizedEntry struct {
	Key   any `json:"key"`
	Value any `json:"value"`
}

type openProtocolNormalizedMessage struct {
	Version uint64                        `json:"version"`
	Entries []openProtocolNormalizedEntry `json:"entries"`
}

func decodeOpenProtocolMessage(key, value []byte) (openProtocolMessage, error) {
	if len(key) < 8 {
		return openProtocolMessage{}, errors.New("open protocol key is too short")
	}

	version := binary.BigEndian.Uint64(key[:8])
	if version != openProtocolBatchVersion1 {
		return openProtocolMessage{}, errors.Errorf("unsupported open protocol batch version %d", version)
	}
	keyParts, err := decodeLengthPrefixedBytes(key[8:])
	if err != nil {
		return openProtocolMessage{}, errors.Annotate(err, "decode open protocol keys")
	}
	valueParts, err := decodeLengthPrefixedBytes(value)
	if err != nil {
		return openProtocolMessage{}, errors.Annotate(err, "decode open protocol values")
	}
	if len(keyParts) != len(valueParts) {
		return openProtocolMessage{}, errors.Errorf(
			"open protocol entry count mismatch: keys=%d values=%d",
			len(keyParts), len(valueParts))
	}

	entries := make([]openProtocolMessageEntry, 0, len(keyParts))
	for i := range keyParts {
		entries = append(entries, openProtocolMessageEntry{
			Key:   keyParts[i],
			Value: valueParts[i],
		})
	}
	return openProtocolMessage{Version: version, Entries: entries}, nil
}

func decodeLengthPrefixedBytes(data []byte) ([][]byte, error) {
	parts := make([][]byte, 0, 1)
	for len(data) > 0 {
		if len(data) < 8 {
			return nil, errors.New("length-prefixed payload is truncated")
		}
		partLen := binary.BigEndian.Uint64(data[:8])
		data = data[8:]
		if uint64(len(data)) < partLen {
			return nil, errors.New("length-prefixed payload is truncated")
		}
		part := data[:int(partLen)]
		parts = append(parts, part)
		data = data[int(partLen):]
	}
	return parts, nil
}
