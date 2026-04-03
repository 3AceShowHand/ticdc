// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"path/filepath"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/common/event"
)

type StepKind string

const (
	StepKindDDL StepKind = "ddl"
	StepKindDML StepKind = "dml"
)

type PlannedFile struct {
	Kind         StepKind
	RelativePath string
}

type Statement struct {
	Kind       StepKind
	SourceFile string
	Ordinal    int
	SQL        string
}

type Plan struct {
	DDLBeforeDML []string
	DMLFiles     []string
	DDLAfterDML  []string
}

func DefaultPlan() Plan {
	return Plan{
		DDLBeforeDML: []string{
			"ddl/create-database.sql",
			"ddl/alter-database-charset.sql",
			"ddl/create-dml-base-table.sql",
			"ddl/create-dml-types-table.sql",
			"ddl/create-view-base-table.sql",
			"ddl/create-view.sql",
			"ddl/create-partition-table.sql",
			"ddl/add-partition.sql",
			"ddl/reorganize-partition.sql",
			"ddl/truncate-partition.sql",
			"ddl/drop-partition.sql",
			"ddl/remove-partitioning.sql",
			"ddl/repartition-by-hash.sql",
			"ddl/create-exchange-partition-table.sql",
			"ddl/create-exchange-plain-table.sql",
			"ddl/exchange-partition.sql",
			"ddl/create-alter-table.sql",
			"ddl/add-column.sql",
			"ddl/modify-column.sql",
			"ddl/set-default.sql",
			"ddl/drop-column.sql",
			"ddl/modify-table-comment.sql",
			"ddl/modify-table-charset.sql",
			"ddl/rebase-auto-increment.sql",
			"ddl/create-index-table.sql",
			"ddl/add-index.sql",
			"ddl/rename-index.sql",
			"ddl/alter-index-visibility.sql",
			"ddl/drop-index.sql",
			"ddl/create-primary-key-table.sql",
			"ddl/drop-primary-key.sql",
			"ddl/add-primary-key.sql",
			"ddl/create-multi-schema-table.sql",
			"ddl/multi-schema-change.sql",
			"ddl/create-foreign-key-parent.sql",
			"ddl/create-foreign-key-child.sql",
			"ddl/add-foreign-key.sql",
			"ddl/drop-foreign-key.sql",
			"ddl/create-rename-single-source.sql",
			"ddl/create-rename-multi-source-a.sql",
			"ddl/create-rename-multi-source-b.sql",
			"ddl/rename-table.sql",
			"ddl/rename-tables.sql",
			"ddl/truncate-table.sql",
		},
		DMLFiles: []string{
			"dml/basic.sql",
			"dml/types.sql",
		},
		DDLAfterDML: []string{
			"ddl/drop-view.sql",
			"ddl/drop-table.sql",
			"ddl/drop-database.sql",
		},
	}
}

func (p Plan) OrderedFiles() []PlannedFile {
	files := make([]PlannedFile, 0, len(p.DDLBeforeDML)+len(p.DMLFiles)+len(p.DDLAfterDML))
	for _, file := range p.DDLBeforeDML {
		files = append(files, PlannedFile{Kind: StepKindDDL, RelativePath: file})
	}
	for _, file := range p.DMLFiles {
		files = append(files, PlannedFile{Kind: StepKindDML, RelativePath: file})
	}
	for _, file := range p.DDLAfterDML {
		files = append(files, PlannedFile{Kind: StepKindDDL, RelativePath: file})
	}
	return files
}

func (p Plan) Validate(sqlRoot string) error {
	for _, file := range p.OrderedFiles() {
		path := filepath.Join(sqlRoot, file.RelativePath)
		info, err := os.Stat(path)
		if err != nil {
			return errors.Annotatef(err, "stat sql file %s", path)
		}
		if info.IsDir() {
			return errors.Errorf("sql file %s is a directory", path)
		}
	}
	return nil
}

func LoadStatements(sqlRoot string, file PlannedFile) ([]Statement, error) {
	path := filepath.Join(sqlRoot, file.RelativePath)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Annotatef(err, "read sql file %s", path)
	}

	queries, err := event.SplitQueries(string(data))
	if err != nil {
		return nil, errors.Annotatef(err, "split sql file %s", path)
	}

	if file.Kind == StepKindDDL && len(queries) != 1 {
		return nil, errors.Errorf("ddl file %s must contain exactly one statement", path)
	}

	statements := make([]Statement, 0, len(queries))
	for i, query := range queries {
		statements = append(statements, Statement{
			Kind:       file.Kind,
			SourceFile: filepath.ToSlash(filepath.Join("sql", file.RelativePath)),
			Ordinal:    i + 1,
			SQL:        query,
		})
	}

	return statements, nil
}
