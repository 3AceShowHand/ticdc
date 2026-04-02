package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompareNormalizedMessages(t *testing.T) {
	a := NormalizedMessage{Value: map[string]any{"type": "INSERT", "id": 1}}
	b := NormalizedMessage{Value: map[string]any{"type": "DELETE", "id": 2}}

	diff, err := CompareNormalizedMessages(
		[]NormalizedMessage{a, b},
		[]NormalizedMessage{b, a},
	)
	require.NoError(t, err)
	require.True(t, diff.Equal())

	diff, err = CompareNormalizedMessages(
		[]NormalizedMessage{a},
		[]NormalizedMessage{a, b},
	)
	require.NoError(t, err)
	require.False(t, diff.Equal())
	require.Len(t, diff.Unexpected, 1)
}
