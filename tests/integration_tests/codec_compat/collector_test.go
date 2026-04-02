package main

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCollectorFiltersMessages(t *testing.T) {
	spec := mustResolveProtocolSpec(t, "canal-json", "")

	ddl := []byte(`{"database":"codec_compat","table":"t","isDdl":true,"type":"CREATE","sql":"CREATE TABLE t (id INT PRIMARY KEY)"}`)
	dml := []byte(`{"database":"codec_compat","table":"t","isDdl":false,"type":"INSERT","data":[{"id":"1"}]}`)
	watermark := []byte(`{"database":"","table":"","isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":123}}`)

	_, keep, err := filterCapturedMessage(spec, nil, ddl)
	require.NoError(t, err)
	require.True(t, keep)

	_, keep, err = filterCapturedMessage(spec, nil, dml)
	require.NoError(t, err)
	require.True(t, keep)

	_, keep, err = filterCapturedMessage(spec, nil, watermark)
	require.NoError(t, err)
	require.False(t, keep)
}

func TestCollectorFiltersDebeziumMessages(t *testing.T) {
	spec := mustResolveProtocolSpec(t, "debezium", "")

	ddl := []byte(`{"schema":{"type":"struct"},"payload":{"source":{"commit_ts":123,"db":"codec_compat","table":"t"},"ddl":"CREATE TABLE t (id INT PRIMARY KEY)"}}`)
	dml := []byte(`{"schema":{"type":"struct"},"payload":{"before":null,"after":{"id":1},"source":{"commit_ts":123,"db":"codec_compat","table":"t"},"op":"c"}}`)
	watermark := []byte(`{"schema":{"type":"struct"},"payload":{"source":{"commit_ts":123},"op":"m"}}`)

	_, keep, err := filterCapturedMessage(spec, nil, ddl)
	require.NoError(t, err)
	require.True(t, keep)

	_, keep, err = filterCapturedMessage(spec, nil, dml)
	require.NoError(t, err)
	require.True(t, keep)

	_, keep, err = filterCapturedMessage(spec, nil, watermark)
	require.NoError(t, err)
	require.False(t, keep)
}

func TestCollectorFiltersSimpleJSONMessages(t *testing.T) {
	spec := mustResolveProtocolSpec(t, "simple", "")

	ddl := []byte(`{"version":1,"type":"ALTER","sql":"ALTER TABLE t ADD COLUMN c INT","commitTs":123}`)
	dml := []byte(`{"version":1,"database":"codec_compat","table":"t","type":"INSERT","commitTs":123,"data":{"id":1}}`)
	watermark := []byte(`{"version":1,"type":"WATERMARK","commitTs":123}`)

	_, keep, err := filterCapturedMessage(spec, nil, ddl)
	require.NoError(t, err)
	require.True(t, keep)

	_, keep, err = filterCapturedMessage(spec, nil, dml)
	require.NoError(t, err)
	require.True(t, keep)

	_, keep, err = filterCapturedMessage(spec, nil, watermark)
	require.NoError(t, err)
	require.False(t, keep)
}

func TestCollectorFiltersOpenProtocolMessages(t *testing.T) {
	spec := mustResolveProtocolSpec(t, "open-protocol", "")

	rowKey, rowValue := buildOpenProtocolMessage(t, []openProtocolMessageEntry{
		{
			Key:   []byte(`{"ts":123,"scm":"codec_compat","tbl":"t","t":1}`),
			Value: []byte(`{"u":{"id":{"t":3,"f":0,"v":1}}}`),
		},
	})
	_, keep, err := filterCapturedMessage(spec, rowKey, rowValue)
	require.NoError(t, err)
	require.True(t, keep)

	resolvedKey, resolvedValue := buildOpenProtocolMessage(t, []openProtocolMessageEntry{
		{
			Key:   []byte(`{"ts":123,"t":3}`),
			Value: nil,
		},
	})
	_, keep, err = filterCapturedMessage(spec, resolvedKey, resolvedValue)
	require.NoError(t, err)
	require.False(t, keep)
}

func TestCollectorFiltersAvroMessages(t *testing.T) {
	spec := mustResolveProtocolSpec(t, "avro", "")

	_, keep, err := filterCapturedMessage(spec, []byte{0x00, 0x00}, nil)
	require.NoError(t, err)
	require.True(t, keep)

	_, keep, err = filterCapturedMessage(spec, nil, []byte{0x01, '{', '}'})
	require.NoError(t, err)
	require.True(t, keep)

	checkpoint := make([]byte, 9)
	checkpoint[0] = 0x02
	binary.BigEndian.PutUint64(checkpoint[1:], 123)
	_, keep, err = filterCapturedMessage(spec, nil, checkpoint)
	require.NoError(t, err)
	require.False(t, keep)
}
