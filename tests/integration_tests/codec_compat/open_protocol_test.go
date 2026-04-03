package main

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func buildOpenProtocolMessage(t *testing.T, entries []openProtocolMessageEntry) ([]byte, []byte) {
	t.Helper()

	return mustOpenProtocolKey(t, entries), mustOpenProtocolValue(t, entries)
}

func mustOpenProtocolKey(t *testing.T, entries []openProtocolMessageEntry) []byte {
	t.Helper()

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, openProtocolBatchVersion1)
	for _, entry := range entries {
		key = appendLengthPrefixed(key, entry.Key)
	}
	return key
}

func mustOpenProtocolValue(t *testing.T, entries []openProtocolMessageEntry) []byte {
	t.Helper()

	var value []byte
	for _, entry := range entries {
		value = appendLengthPrefixed(value, entry.Value)
	}
	return value
}

func appendLengthPrefixed(dst, payload []byte) []byte {
	length := make([]byte, 8)
	binary.BigEndian.PutUint64(length, uint64(len(payload)))
	dst = append(dst, length...)
	dst = append(dst, payload...)
	return dst
}

func TestDecodeOpenProtocolMessageRejectsMismatchedBatch(t *testing.T) {
	key, value := buildOpenProtocolMessage(t, []openProtocolMessageEntry{
		{
			Key:   []byte(`{"t":1}`),
			Value: []byte(`{"u":{"id":{"v":1}}}`),
		},
	})
	value = append(value, make([]byte, 8)...)

	_, err := decodeOpenProtocolMessage(key, value)
	require.Error(t, err)
}
