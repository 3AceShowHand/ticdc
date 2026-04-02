package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCollectorFiltersMessages(t *testing.T) {
	ddl := []byte(`{"database":"codec_compat","table":"t","isDdl":true,"type":"CREATE","sql":"CREATE TABLE t (id INT PRIMARY KEY)"}`)
	dml := []byte(`{"database":"codec_compat","table":"t","isDdl":false,"type":"INSERT","data":[{"id":"1"}]}`)
	watermark := []byte(`{"database":"","table":"","isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":123}}`)

	_, keep, err := filterCanalJSONMessage(nil, ddl)
	require.NoError(t, err)
	require.True(t, keep)

	_, keep, err = filterCanalJSONMessage(nil, dml)
	require.NoError(t, err)
	require.True(t, keep)

	_, keep, err = filterCanalJSONMessage(nil, watermark)
	require.NoError(t, err)
	require.False(t, keep)
}
