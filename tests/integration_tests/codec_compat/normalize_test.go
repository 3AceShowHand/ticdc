package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeJSONObjectOrder(t *testing.T) {
	left := CapturedMessage{
		Value: []byte(`{"type":"INSERT","data":[{"a":1,"b":2}],"isDdl":false}`),
	}
	right := CapturedMessage{
		Value: []byte(`{"isDdl":false,"data":[{"b":2,"a":1}],"type":"INSERT"}`),
	}

	leftNormalized, err := NormalizeCapturedMessage(left)
	require.NoError(t, err)
	rightNormalized, err := NormalizeCapturedMessage(right)
	require.NoError(t, err)

	leftCanonical, err := CanonicalMessageString(leftNormalized)
	require.NoError(t, err)
	rightCanonical, err := CanonicalMessageString(rightNormalized)
	require.NoError(t, err)

	require.Equal(t, leftCanonical, rightCanonical)
}
