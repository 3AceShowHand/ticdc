package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeJSONObjectOrder(t *testing.T) {
	spec := mustResolveProtocolSpec(t, "canal-json", "")

	left := CapturedMessage{
		Value: []byte(`{"type":"INSERT","data":[{"a":1,"b":2}],"isDdl":false}`),
	}
	right := CapturedMessage{
		Value: []byte(`{"isDdl":false,"data":[{"b":2,"a":1}],"type":"INSERT"}`),
	}

	leftNormalized, err := NormalizeCapturedMessage(spec, left)
	require.NoError(t, err)
	rightNormalized, err := NormalizeCapturedMessage(spec, right)
	require.NoError(t, err)

	leftCanonical, err := CanonicalMessageString(leftNormalized)
	require.NoError(t, err)
	rightCanonical, err := CanonicalMessageString(rightNormalized)
	require.NoError(t, err)

	require.Equal(t, leftCanonical, rightCanonical)
}

func TestNormalizeOpenProtocolMessage(t *testing.T) {
	spec := mustResolveProtocolSpec(t, "open-protocol", "")

	leftKey, leftValue := buildOpenProtocolMessage(t, []openProtocolMessageEntry{
		{
			Key:   []byte(`{"ts":123,"scm":"codec_compat","tbl":"t","t":1}`),
			Value: []byte(`{"u":{"id":{"t":3,"f":0,"v":1},"name":{"t":15,"f":0,"v":"alice"}}}`),
		},
		{
			Key:   []byte(`{"tbl":"t","t":1,"ts":124,"scm":"codec_compat"}`),
			Value: []byte(`{"u":{"name":{"v":"bob","f":0,"t":15},"id":{"v":2,"t":3,"f":0}}}`),
		},
	})
	rightKey, rightValue := buildOpenProtocolMessage(t, []openProtocolMessageEntry{
		{
			Key:   []byte(`{"t":1,"tbl":"t","scm":"codec_compat","ts":123}`),
			Value: []byte(`{"u":{"name":{"v":"alice","t":15,"f":0},"id":{"f":0,"v":1,"t":3}}}`),
		},
		{
			Key:   []byte(`{"scm":"codec_compat","ts":124,"t":1,"tbl":"t"}`),
			Value: []byte(`{"u":{"id":{"t":3,"v":2,"f":0},"name":{"t":15,"f":0,"v":"bob"}}}`),
		},
	})

	leftNormalized, err := NormalizeCapturedMessage(spec, CapturedMessage{Key: leftKey, Value: leftValue})
	require.NoError(t, err)
	rightNormalized, err := NormalizeCapturedMessage(spec, CapturedMessage{Key: rightKey, Value: rightValue})
	require.NoError(t, err)

	leftCanonical, err := CanonicalMessageString(leftNormalized)
	require.NoError(t, err)
	rightCanonical, err := CanonicalMessageString(rightNormalized)
	require.NoError(t, err)

	require.Equal(t, leftCanonical, rightCanonical)
}

func TestNormalizeRawMessage(t *testing.T) {
	spec := mustResolveProtocolSpec(t, "avro", "")

	normalized, err := NormalizeCapturedMessage(spec, CapturedMessage{
		Key:   []byte{0x00, 0x01, 0x02},
		Value: []byte{0x01, 0x02, 0x03},
	})
	require.NoError(t, err)
	require.Equal(t, "AAEC", normalized.Key)
	require.Equal(t, "AQID", normalized.Value)
}
