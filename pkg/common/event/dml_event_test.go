// Copyright 2025 PingCAP, Inc.
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

package event

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeEvents(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")

	createTable := `create table test.t(a int primary key, b int)`
	ddlJob := helper.DDL2Job(createTable)
	require.NotNil(t, ddlJob)

	dmlEvent1 := helper.DML2Event("test", "t", `insert into test.t values (1, 1)`)
	require.NotNil(t, dmlEvent1)

	dmlEvent2 := helper.DML2Event("test", "t", `insert into test.t values (2, 2)`)
	require.NotNil(t, dmlEvent2)

	events := []*DMLEvent{dmlEvent1, dmlEvent2}
	mergedEvent := MergeDMLEvent(events)
	require.NotNil(t, mergedEvent)
	require.Equal(t, mergedEvent.Length, int32(2))
	require.Equal(t, mergedEvent.Rows.NumRows(), 2)
	require.Equal(t, len(mergedEvent.RowTypes), 2)
}

// TestDMLEvent test the Marshal and Unmarshal of DMLEvent.
func TestDMLEvent(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(createTableSQL)
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", insertDataSQL)
	dmlEvent.State = EventSenderStatePaused
	require.NotNil(t, dmlEvent)

	data, err := dmlEvent.Marshal()
	require.NoError(t, err)

	reverseEvent := &DMLEvent{}
	// Set the TableInfo before unmarshal, it is used in Unmarshal.
	err = reverseEvent.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, len(data), int(reverseEvent.eventSize))
	reverseEvent.AssembleRows(dmlEvent.TableInfo)
	// Compare the content of the two event's rows.
	require.Equal(t, dmlEvent.Rows.ToString(dmlEvent.TableInfo.GetFieldSlice()), reverseEvent.Rows.ToString(dmlEvent.TableInfo.GetFieldSlice()))
	for i := 0; i < dmlEvent.Rows.NumRows(); i++ {
		for j := 0; j < dmlEvent.Rows.NumCols(); j++ {
			require.Equal(t, dmlEvent.Rows.GetRow(i).GetRaw(j), reverseEvent.Rows.GetRow(i).GetRaw(j))
		}
	}

	require.True(t, reverseEvent.IsPaused())

	// Compare the remaining content of the two events.
	dmlEvent.Rows = nil
	reverseEvent.Rows = nil
	reverseEvent.eventSize = 0
	require.Equal(t, dmlEvent, reverseEvent)
}

func TestEncodeAndDecodeV0(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(createTableSQL)
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, dmlEvent)

	data, err := dmlEvent.encodeV0()
	require.NoError(t, err)

	reverseEvent := &DMLEvent{}
	// Set the TableInfo before decode, it is used in decode.
	err = reverseEvent.decodeV0(data)
	require.NoError(t, err)
	reverseEvent.AssembleRows(dmlEvent.TableInfo)
	require.Equal(t, dmlEvent.Rows.ToString(dmlEvent.TableInfo.GetFieldSlice()), reverseEvent.Rows.ToString(dmlEvent.TableInfo.GetFieldSlice()))
	for i := 0; i < dmlEvent.Rows.NumRows(); i++ {
		for j := 0; j < dmlEvent.Rows.NumCols(); j++ {
			require.Equal(t, dmlEvent.Rows.GetRow(i).GetRaw(j), reverseEvent.Rows.GetRow(i).GetRaw(j))
		}
	}

	require.False(t, reverseEvent.IsPaused())

	// Compare the remaining content of the two events.
	dmlEvent.Rows = nil
	reverseEvent.Rows = nil
	reverseEvent.eventSize = 0
	require.Equal(t, dmlEvent, reverseEvent)
}
