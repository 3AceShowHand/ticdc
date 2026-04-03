package main

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterCapturedMessage(t *testing.T) {
	openProtocolRow := []openProtocolMessageEntry{
		{
			Key:   []byte(`{"ts":123,"scm":"codec_compat","tbl":"t","t":1}`),
			Value: []byte(`{"u":{"id":{"t":3,"f":0,"v":1}}}`),
		},
	}
	openProtocolResolved := []openProtocolMessageEntry{
		{
			Key: []byte(`{"ts":123,"t":3}`),
		},
	}

	tests := []struct {
		name   string
		spec   protocolSpec
		key    []byte
		value  []byte
		expect bool
	}{
		{
			name:   "canal json ddl",
			spec:   mustResolveProtocolSpec(t, "canal-json", ""),
			value:  []byte(`{"database":"codec_compat","table":"t","isDdl":true,"type":"CREATE","sql":"CREATE TABLE t (id INT PRIMARY KEY)"}`),
			expect: true,
		},
		{
			name:   "canal json watermark",
			spec:   mustResolveProtocolSpec(t, "canal-json", ""),
			value:  []byte(`{"database":"","table":"","isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":123}}`),
			expect: false,
		},
		{
			name:   "debezium dml",
			spec:   mustResolveProtocolSpec(t, "debezium", ""),
			value:  []byte(`{"schema":{"type":"struct"},"payload":{"before":null,"after":{"id":1},"source":{"commit_ts":123,"db":"codec_compat","table":"t"},"op":"c"}}`),
			expect: true,
		},
		{
			name:   "debezium watermark",
			spec:   mustResolveProtocolSpec(t, "debezium", ""),
			value:  []byte(`{"schema":{"type":"struct"},"payload":{"source":{"commit_ts":123},"op":"m"}}`),
			expect: false,
		},
		{
			name:   "simple json ddl",
			spec:   mustResolveProtocolSpec(t, "simple", ""),
			value:  []byte(`{"version":1,"type":"ALTER","sql":"ALTER TABLE t ADD COLUMN c INT","commitTs":123}`),
			expect: true,
		},
		{
			name:   "simple json watermark",
			spec:   mustResolveProtocolSpec(t, "simple", ""),
			value:  []byte(`{"version":1,"type":"WATERMARK","commitTs":123}`),
			expect: false,
		},
		{
			name:   "open protocol row",
			spec:   mustResolveProtocolSpec(t, "open-protocol", ""),
			key:    mustOpenProtocolKey(t, openProtocolRow),
			value:  mustOpenProtocolValue(t, openProtocolRow),
			expect: true,
		},
		{
			name:   "open protocol resolved",
			spec:   mustResolveProtocolSpec(t, "open-protocol", ""),
			key:    mustOpenProtocolKey(t, openProtocolResolved),
			value:  mustOpenProtocolValue(t, openProtocolResolved),
			expect: false,
		},
		{
			name:   "avro ddl",
			spec:   mustResolveProtocolSpec(t, "avro", ""),
			value:  []byte{0x01, '{', '}'},
			expect: true,
		},
	}

	checkpoint := make([]byte, 9)
	checkpoint[0] = 0x02
	binary.BigEndian.PutUint64(checkpoint[1:], 123)
	tests = append(tests, struct {
		name   string
		spec   protocolSpec
		key    []byte
		value  []byte
		expect bool
	}{
		name:   "avro checkpoint",
		spec:   mustResolveProtocolSpec(t, "avro", ""),
		value:  checkpoint,
		expect: false,
	})

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			_, keep, err := filterCapturedMessage(test.spec, test.key, test.value)
			require.NoError(t, err)
			require.Equal(t, test.expect, keep)
		})
	}
}
