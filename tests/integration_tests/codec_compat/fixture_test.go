package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFixtureRoundTrip(t *testing.T) {
	store := NewFixtureStore(t.TempDir(), mustResolveProtocolSpec(t, "canal-json", ""))
	fixture := FileFixture{
		Protocol:   "canal-json",
		SourceFile: "sql/dml/basic.sql",
		Statements: []StatementFixture{
			{
				SQL: "INSERT INTO codec_compat.dml_basic_table (id, c1, c2) VALUES (1, 10, 'alpha');",
				ExpectedMessages: []NormalizedMessage{
					{Value: map[string]any{"type": "INSERT"}},
				},
			},
		},
	}

	require.NoError(t, store.Write(fixture))
	readBack, err := store.Read(fixture.SourceFile)
	require.NoError(t, err)
	require.Equal(t, fixture.Protocol, readBack.Protocol)
	require.Equal(t, fixture.SourceFile, readBack.SourceFile)
	require.Len(t, readBack.Statements, 1)
	require.Equal(t, "INSERT INTO codec_compat.dml_basic_table (id, c1, c2) VALUES (1, 10, 'alpha');", readBack.Statements[0].SQL)
	require.Equal(t, store.FixturePath("sql/dml/basic.sql"), store.FixturePath(readBack.SourceFile))
}

func TestFixturePathUsesProtocolVariant(t *testing.T) {
	store := NewFixtureStore(t.TempDir(), mustResolveProtocolSpec(t, "simple", "avro"))
	require.Equal(t, filepath.Join(store.root, "simple-avro", "dml", "basic.json"), store.FixturePath("sql/dml/basic.sql"))
}
