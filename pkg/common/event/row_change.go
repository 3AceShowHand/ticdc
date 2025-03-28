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
	"unsafe"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	timodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/integrity"
	"go.uber.org/zap"
)

//go:generate msgp
//
//msgp:ignore DDLEvent

// ColumnData represents a column value in row changed event
//
//msgp:ignore ColumnData
type ColumnData struct {
	// ColumnID may be just a mock id, because we don't store it in redo log.
	// So after restore from redo log, we need to give every a column a mock id.
	// The only guarantee is that the column id is unique in a RowChangedEvent
	ColumnID int64
	Value    interface{}

	// ApproximateBytes is approximate bytes consumed by the column.
	ApproximateBytes int
}

type RowChangedEvent struct {
	PhysicalTableID int64

	StartTs  uint64
	CommitTs uint64

	// NOTICE: We probably store the logical ID inside TableInfo's TableName,
	// not the physical ID.
	// For normal table, there is only one ID, which is the physical ID.
	// AKA TIDB_TABLE_ID.
	// For partitioned table, there are two kinds of ID:
	// 1. TIDB_PARTITION_ID is the physical ID of the partition.
	// 2. TIDB_TABLE_ID is the logical ID of the table.
	// In general, we always use the physical ID to represent a table, but we
	// record the logical ID from the DDL event(job.BinlogInfo.TableInfo).
	// So be careful when using the TableInfo.
	TableInfo *common.TableInfo `msg:"-"`

	Columns    []*common.Column `msg:"columns"`
	PreColumns []*common.Column `msg:"pre-columns"`

	// ReplicatingTs is ts when a table starts replicating events to downstream.
	ReplicatingTs uint64 `msg:"replicating-ts"`

	// Checksum for the event, only not nil if the upstream TiDB enable the row level checksum
	// and TiCDC set the integrity check level to the correctness.
	Checksum *integrity.Checksum
}

// GetTableID returns the table ID of the event.
func (r *RowChangedEvent) GetTableID() int64 {
	return r.PhysicalTableID
}

// GetColumns returns the columns of the event
func (r *RowChangedEvent) GetColumns() []*common.Column {
	return r.Columns
}

// GetPreColumns returns the pre columns of the event
func (r *RowChangedEvent) GetPreColumns() []*common.Column {
	return r.PreColumns
}

func (r *RowChangedEvent) ApproximateBytes() int {
	const sizeOfRowEvent = int(unsafe.Sizeof(*r))
	const sizeOfTable = int(unsafe.Sizeof(*r.TableInfo))
	const sizeOfInt = int(unsafe.Sizeof(int(0)))
	size := sizeOfRowEvent + sizeOfTable + 2*sizeOfInt

	// Size of cols
	for i := range r.Columns {
		size += r.Columns[i].ApproximateBytes
	}
	// Size of pre cols
	for i := range r.PreColumns {
		if r.PreColumns[i] != nil {
			size += r.PreColumns[i].ApproximateBytes
		}
	}
	return size
}

func ColumnDatas2Columns(cols []*timodel.ColumnData, tableInfo *common.TableInfo) []*common.Column {
	if cols == nil {
		return nil
	}
	columns := make([]*common.Column, len(cols))
	for i, colData := range cols {
		if colData == nil {
			log.Warn("meet nil column data, should not happened in production env",
				zap.Any("cols", cols),
				zap.Any("tableInfo", tableInfo))
			continue
		}
		columns[i] = columnData2Column(colData, tableInfo)
	}
	return columns
}

func columnData2Column(col *timodel.ColumnData, tableInfo *common.TableInfo) *common.Column {
	colID := col.ColumnID
	offset, ok := tableInfo.GetColumnsOffset()[colID]
	if !ok {
		log.Warn("invalid column id",
			zap.Int64("columnID", colID),
			zap.Any("tableInfo", tableInfo))
	}
	colInfo := tableInfo.GetColumns()[offset]
	return &common.Column{
		Name:      colInfo.Name.O,
		Type:      colInfo.GetType(),
		Charset:   colInfo.GetCharset(),
		Collation: colInfo.GetCollate(),
		Flag:      *tableInfo.GetColumnsFlag()[colID],
		Value:     col.Value,
		Default:   common.GetColumnDefaultValue(colInfo),
	}
}

// IsDelete returns true if the row is a delete event
func (r *RowChangedEvent) IsDelete() bool {
	return len(r.PreColumns) != 0 && len(r.Columns) == 0
}

// IsInsert returns true if the row is an insert event
func (r *RowChangedEvent) IsInsert() bool {
	return len(r.PreColumns) == 0 && len(r.Columns) != 0
}

// IsUpdate returns true if the row is an update event
func (r *RowChangedEvent) IsUpdate() bool {
	return len(r.PreColumns) != 0 && len(r.Columns) != 0
}

// HandleKeyColInfos returns the column(s) and colInfo(s) corresponding to the handle key(s)
func (r *RowChangedEvent) HandleKeyColInfos() ([]*common.Column, []rowcodec.ColInfo) {
	pkeyCols := make([]*common.Column, 0)
	pkeyColInfos := make([]rowcodec.ColInfo, 0)

	var cols []*common.Column
	if r.IsDelete() {
		cols = r.PreColumns
	} else {
		cols = r.Columns
	}

	tableInfo := r.TableInfo
	colInfos := tableInfo.GetColInfosForRowChangedEvent()
	for i, col := range cols {
		if col != nil && col.Flag.IsHandleKey() {
			pkeyCols = append(pkeyCols, col)
			pkeyColInfos = append(pkeyColInfos, colInfos[i])
		}
	}

	// It is okay not to have handle keys, so the empty array is an acceptable result
	return pkeyCols, pkeyColInfos
}

// PrimaryKeyColumnNames return all primary key's name
func (r *RowChangedEvent) PrimaryKeyColumnNames() []string {
	var result []string

	var cols []*common.Column
	if r.IsDelete() {
		cols = r.PreColumns
	} else {
		cols = r.Columns
	}

	result = make([]string, 0)
	for _, col := range cols {
		if col != nil && col.Flag.IsPrimaryKey() {
			result = append(result, col.Name)
		}
	}
	return result
}

type MQRowEvent struct {
	Key      timodel.TopicPartitionKey
	RowEvent RowEvent
}

type RowEvent struct {
	TableInfo      *common.TableInfo
	CommitTs       uint64
	Event          RowChange
	ColumnSelector columnselector.Selector
	Callback       func()
}

func (e *RowEvent) IsDelete() bool {
	return !e.Event.PreRow.IsEmpty() && e.Event.Row.IsEmpty()
}

func (e *RowEvent) IsUpdate() bool {
	return !e.Event.PreRow.IsEmpty() && !e.Event.Row.IsEmpty()
}

func (e *RowEvent) IsInsert() bool {
	return e.Event.PreRow.IsEmpty() && !e.Event.Row.IsEmpty()
}

func (e *RowEvent) GetRows() *chunk.Row {
	return &e.Event.Row
}

func (e *RowEvent) GetPreRows() *chunk.Row {
	return &e.Event.PreRow
}

// PrimaryKeyColumnNames return all primary key's name
// TODO: need a test for delete / insert / update event
func (e *RowEvent) PrimaryKeyColumnNames() []string {
	var result []string

	result = make([]string, 0)
	tableInfo := e.TableInfo
	columns := e.TableInfo.GetColumns()
	for _, col := range columns {
		if col != nil && tableInfo.ForceGetColumnFlagType(col.ID).IsPrimaryKey() {
			result = append(result, tableInfo.ForceGetColumnName(col.ID))
		}
	}
	return result
}
