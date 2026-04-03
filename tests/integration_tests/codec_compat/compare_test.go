package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompareFileFixturesIgnoresMessageOrder(t *testing.T) {
	expected := FileFixture{
		Protocol:   protocolCanalJSON,
		SourceFile: "sql/dml/basic.sql",
		Statements: []StatementFixture{
			{
				SQL: "insert into t values (1)",
				ExpectedMessages: []NormalizedMessage{
					{Value: map[string]any{"type": "INSERT", "id": 1}},
					{Value: map[string]any{"type": "DELETE", "id": 2}},
				},
			},
		},
	}
	actual := FileFixture{
		Protocol:   protocolCanalJSON,
		SourceFile: "sql/dml/basic.sql",
		Statements: []StatementFixture{
			{
				SQL: "insert into t values (1)",
				ExpectedMessages: []NormalizedMessage{
					{Value: map[string]any{"type": "DELETE", "id": 2}},
					{Value: map[string]any{"type": "INSERT", "id": 1}},
				},
			},
		},
	}

	require.NoError(t, CompareFileFixtures(expected, actual))
}

func TestCompareFileFixturesReturnsReadableDiff(t *testing.T) {
	expected := FileFixture{
		Protocol:       protocolSimple,
		EncodingFormat: encodingFormatAvro,
		SourceFile:     "sql/dml/basic.sql",
		Statements: []StatementFixture{
			{
				SQL: "delete from t where id = 1",
				ExpectedMessages: []NormalizedMessage{
					{Value: "AQID"},
				},
			},
		},
	}
	actual := FileFixture{
		Protocol:       protocolSimple,
		EncodingFormat: encodingFormatAvro,
		SourceFile:     "sql/dml/basic.sql",
		Statements: []StatementFixture{
			{
				SQL: "delete from t where id = 1",
				ExpectedMessages: []NormalizedMessage{
					{Value: "BAUG"},
				},
			},
		},
	}

	err := CompareFileFixtures(expected, actual)
	require.Error(t, err)
	require.Contains(t, err.Error(), "protocol=simple")
	require.Contains(t, err.Error(), "encoding_format=avro")
	require.Contains(t, err.Error(), "unexpected_messages")
}
