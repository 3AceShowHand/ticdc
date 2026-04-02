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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/pingcap/errors"
)

type StatementFixture struct {
	SQL              string              `json:"sql"`
	ExpectedMessages []NormalizedMessage `json:"expected_messages"`
}

type FileFixture struct {
	Protocol       string             `json:"protocol"`
	EncodingFormat string             `json:"encoding_format,omitempty"`
	SourceFile     string             `json:"source_file"`
	Statements     []StatementFixture `json:"statements"`
}

type FixtureStore struct {
	root string
	spec protocolSpec
}

func NewFixtureStore(root string, spec protocolSpec) *FixtureStore {
	return &FixtureStore{root: root, spec: spec}
}

func (s *FixtureStore) FixturePath(sourceFile string) string {
	relative := strings.TrimPrefix(sourceFile, "sql/")
	ext := filepath.Ext(relative)
	base := strings.TrimSuffix(relative, ext) + ".json"
	return filepath.Join(s.root, s.spec.fixtureDir, base)
}

func (s *FixtureStore) Write(fixture FileFixture) error {
	path := s.FixturePath(fixture.SourceFile)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return errors.Trace(err)
	}

	data, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		return errors.Trace(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *FixtureStore) Read(sourceFile string) (FileFixture, error) {
	path := s.FixturePath(sourceFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return FileFixture{}, errors.Annotatef(err, "read fixture %s", path)
	}

	var fixture FileFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		return FileFixture{}, errors.Trace(err)
	}
	return fixture, nil
}
