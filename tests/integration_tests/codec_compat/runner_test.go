package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadStatementsPreservesSQLOrder(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "dml"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "dml", "ordered.sql"), []byte(`
insert into t values (1);
update t set c = 2 where id = 1;
delete from t where id = 1;
`), 0o644))

	statements, err := LoadStatements(tempDir, PlannedFile{
		Kind:         StepKindDML,
		RelativePath: "dml/ordered.sql",
	})
	require.NoError(t, err)
	require.Len(t, statements, 3)
	require.Contains(t, statements[0].SQL, "INSERT")
	require.Contains(t, statements[1].SQL, "UPDATE")
	require.Contains(t, statements[2].SQL, "DELETE")
}
