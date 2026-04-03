package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultPlanValidate(t *testing.T) {
	plan := DefaultPlan()
	require.NoError(t, plan.Validate("sql"))
}

func TestDefaultPlanHasNoDuplicateFiles(t *testing.T) {
	plan := DefaultPlan()
	seen := make(map[string]struct{}, len(plan.OrderedFiles()))
	for _, file := range plan.OrderedFiles() {
		_, ok := seen[file.RelativePath]
		require.Falsef(t, ok, "duplicate planned file %s", file.RelativePath)
		seen[file.RelativePath] = struct{}{}
	}
}

func TestLoadStatements(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "dml"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "dml", "sample.sql"), []byte("insert into t values (1);\nupdate t set c = 2 where id = 1;"), 0o644))

	statements, err := LoadStatements(tempDir, PlannedFile{
		Kind:         StepKindDML,
		RelativePath: "dml/sample.sql",
	})
	require.NoError(t, err)
	require.Len(t, statements, 2)
	require.Equal(t, "sql/dml/sample.sql", statements[0].SourceFile)
	require.Equal(t, 1, statements[0].Ordinal)
	require.Equal(t, 2, statements[1].Ordinal)
}

func TestLoadDDLRequiresSingleStatement(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "ddl"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "ddl", "bad.sql"), []byte("create table t1 (id int primary key);\ncreate table t2 (id int primary key);"), 0o644))

	_, err := LoadStatements(tempDir, PlannedFile{
		Kind:         StepKindDDL,
		RelativePath: "ddl/bad.sql",
	})
	require.Error(t, err)
}
