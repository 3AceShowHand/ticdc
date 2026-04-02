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
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/errors"
)

type CompareDiff struct {
	Missing    []string
	Unexpected []string
}

func (d CompareDiff) Equal() bool {
	return len(d.Missing) == 0 && len(d.Unexpected) == 0
}

func CompareNormalizedMessages(expected, actual []NormalizedMessage) (CompareDiff, error) {
	expectedCounts, err := multisetCounts(expected)
	if err != nil {
		return CompareDiff{}, err
	}
	actualCounts, err := multisetCounts(actual)
	if err != nil {
		return CompareDiff{}, err
	}

	diff := CompareDiff{}
	for message, count := range expectedCounts {
		actualCount := actualCounts[message]
		if count > actualCount {
			for i := 0; i < count-actualCount; i++ {
				diff.Missing = append(diff.Missing, message)
			}
		}
	}
	for message, count := range actualCounts {
		expectedCount := expectedCounts[message]
		if count > expectedCount {
			for i := 0; i < count-expectedCount; i++ {
				diff.Unexpected = append(diff.Unexpected, message)
			}
		}
	}

	sort.Strings(diff.Missing)
	sort.Strings(diff.Unexpected)
	return diff, nil
}

func CompareFileFixtures(expected, actual FileFixture) error {
	if expected.Protocol != actual.Protocol {
		return errors.Errorf("fixture protocol mismatch: expected=%s actual=%s", expected.Protocol, actual.Protocol)
	}
	if expected.EncodingFormat != actual.EncodingFormat {
		return errors.Errorf(
			"fixture encoding-format mismatch: expected=%s actual=%s",
			expected.EncodingFormat, actual.EncodingFormat)
	}
	if expected.SourceFile != actual.SourceFile {
		return errors.Errorf("fixture source file mismatch: expected=%s actual=%s", expected.SourceFile, actual.SourceFile)
	}
	if len(expected.Statements) != len(actual.Statements) {
		return errors.Errorf(
			"statement count mismatch for %s: expected=%d actual=%d",
			actual.SourceFile, len(expected.Statements), len(actual.Statements))
	}

	for i := range expected.Statements {
		expectedStmt := expected.Statements[i]
		actualStmt := actual.Statements[i]
		if expectedStmt.SQL != actualStmt.SQL {
			return errors.Errorf(
				"sql mismatch for %s stmt#%d: expected=%s actual=%s",
				actual.SourceFile, i+1, expectedStmt.SQL, actualStmt.SQL)
		}

		diff, err := CompareNormalizedMessages(expectedStmt.ExpectedMessages, actualStmt.ExpectedMessages)
		if err != nil {
			return err
		}
		if diff.Equal() {
			continue
		}

		return errors.New(formatStatementDiff(
			actual.Protocol,
			actual.EncodingFormat,
			actual.SourceFile,
			i+1,
			actualStmt.SQL,
			len(expectedStmt.ExpectedMessages),
			len(actualStmt.ExpectedMessages),
			diff))
	}

	return nil
}

func multisetCounts(messages []NormalizedMessage) (map[string]int, error) {
	counts := make(map[string]int, len(messages))
	for _, message := range messages {
		canonical, err := CanonicalMessageString(message)
		if err != nil {
			return nil, err
		}
		counts[canonical]++
	}
	return counts, nil
}

func formatStatementDiff(
	protocol string,
	encodingFormat string,
	sourceFile string,
	ordinal int,
	sql string,
	expectedCount int,
	actualCount int,
	diff CompareDiff,
) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("protocol=%s\n", protocol))
	if encodingFormat != "" {
		builder.WriteString(fmt.Sprintf("encoding_format=%s\n", encodingFormat))
	}
	builder.WriteString(fmt.Sprintf("source_file=%s\nstatement=%d\nsql=%s\nexpected_count=%d\nactual_count=%d\n", sourceFile, ordinal, sql, expectedCount, actualCount))
	if len(diff.Missing) > 0 {
		builder.WriteString("\nmissing_messages:\n")
		for _, message := range diff.Missing {
			builder.WriteString("  - ")
			builder.WriteString(message)
			builder.WriteByte('\n')
		}
	}
	if len(diff.Unexpected) > 0 {
		builder.WriteString("\nunexpected_messages:\n")
		for _, message := range diff.Unexpected {
			builder.WriteString("  - ")
			builder.WriteString(message)
			builder.WriteByte('\n')
		}
	}
	return builder.String()
}
