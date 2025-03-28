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

package open

import (
	"context"
	"database/sql"
	"encoding/binary"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// BatchDecoder decodes the byte of a batch into the original messages.
type BatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte

	nextKey   *messageKey
	nextEvent *commonEvent.DMLEvent

	storage storage.ExternalStorage

	config *common.Config

	upstreamTiDB *sql.DB

	tableIDAllocator *common.FakeTableIDAllocator
}

// NewBatchDecoder creates a new BatchDecoder.
func NewBatchDecoder(ctx context.Context, config *common.Config, db *sql.DB) (common.RowEventDecoder, error) {
	var (
		externalStorage storage.ExternalStorage
		err             error
	)
	if config.LargeMessageHandle.EnableClaimCheck() {
		storageURI := config.LargeMessageHandle.ClaimCheckStorageURI
		externalStorage, err = util.GetExternalStorage(ctx, storageURI, nil, util.NewS3Retryer(10, 10*time.Second, 10*time.Second))
		if err != nil {
			return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
		}
	}

	if config.LargeMessageHandle.HandleKeyOnly() {
		if db == nil {
			log.Warn("handle-key-only is enabled, but upstream TiDB is not provided")
		}
	}

	return &BatchDecoder{
		config:           config,
		storage:          externalStorage,
		upstreamTiDB:     db,
		tableIDAllocator: common.NewFakeTableIDAllocator(),
	}, nil
}

// AddKeyValue implements the RowEventDecoder interface
func (b *BatchDecoder) AddKeyValue(key, value []byte) error {
	if len(b.keyBytes) != 0 || len(b.valueBytes) != 0 {
		return errors.ErrOpenProtocolCodecInvalidData.
			GenWithStack("decoder key and value not nil")
	}
	version := binary.BigEndian.Uint64(key[:8])
	key = key[8:]
	if version != batchVersion1 {
		return errors.ErrOpenProtocolCodecInvalidData.
			GenWithStack("unexpected key format version")
	}

	b.keyBytes = key
	b.valueBytes = value
	return nil
}

func (b *BatchDecoder) hasNext() bool {
	keyLen := len(b.keyBytes)
	valueLen := len(b.valueBytes)

	if keyLen > 0 && valueLen > 0 {
		return true
	}

	if keyLen == 0 && valueLen != 0 || keyLen != 0 && valueLen == 0 {
		log.Panic("open-protocol meet invalid data",
			zap.Int("keyLen", keyLen), zap.Int("valueLen", valueLen))
	}

	return false
}

func (b *BatchDecoder) decodeNextKey() error {
	keyLen := binary.BigEndian.Uint64(b.keyBytes[:8])
	key := b.keyBytes[8 : keyLen+8]
	msgKey := new(messageKey)
	err := msgKey.Decode(key)
	if err != nil {
		return errors.Trace(err)
	}
	b.nextKey = msgKey

	b.keyBytes = b.keyBytes[keyLen+8:]
	return nil
}

// HasNext implements the RowEventDecoder interface
func (b *BatchDecoder) HasNext() (common.MessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}

	if b.nextKey.Type == common.MessageTypeRow {
		valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
		value := b.valueBytes[8 : valueLen+8]
		b.valueBytes = b.valueBytes[valueLen+8:]

		rowMsg := new(messageRow)
		value, err := common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, value)
		if err != nil {
			return common.MessageTypeUnknown, false, cerror.ErrOpenProtocolCodecInvalidData.
				GenWithStack("decompress data failed")
		}
		if err = rowMsg.decode(value); err != nil {
			return b.nextKey.Type, false, errors.Trace(err)
		}
		b.nextEvent = b.msgToRowChange(b.nextKey, rowMsg)
	}

	return b.nextKey.Type, true, nil
}

// NextResolvedEvent implements the RowEventDecoder interface
func (b *BatchDecoder) NextResolvedEvent() (uint64, error) {
	if b.nextKey.Type != common.MessageTypeResolved {
		return 0, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found resolved event message")
	}
	resolvedTs := b.nextKey.Ts
	b.nextKey = nil
	// resolved ts event's value part is empty, can be ignored.
	b.valueBytes = nil
	return resolvedTs, nil
}

// NextDDLEvent implements the RowEventDecoder interface
func (b *BatchDecoder) NextDDLEvent() (*commonEvent.DDLEvent, error) {
	if b.nextKey.Type != common.MessageTypeDDL {
		return nil, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found ddl event message")
	}

	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]

	value, err := common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		return nil, cerror.ErrOpenProtocolCodecInvalidData.
			GenWithStack("decompress DDL event failed")
	}

	m := new(messageDDL)
	if err = m.decode(value); err != nil {
		return nil, errors.Trace(err)
	}

	result := new(commonEvent.DDLEvent)
	result.Query = m.Query
	result.Type = byte(m.Type)
	result.FinishedTs = b.nextKey.Ts
	result.SchemaName = b.nextKey.Schema
	result.TableName = b.nextKey.Table
	b.nextKey = nil
	b.valueBytes = nil
	return result, nil
}

// NextRowChangedEvent implements the RowEventDecoder interface
func (b *BatchDecoder) NextDMLEvent() (*commonEvent.DMLEvent, error) {
	if b.nextKey.Type != common.MessageTypeRow {
		return nil, errors.ErrOpenProtocolCodecInvalidData.GenWithStack("not found row event message")
	}

	ctx := context.Background()
	// claim-check message found
	if b.nextKey.ClaimCheckLocation != "" {
		return b.assembleEventFromClaimCheckStorage(ctx)
	}

	event := b.nextEvent
	if b.nextKey.OnlyHandleKey {
		event = b.assembleHandleKeyOnlyEvent(ctx, event)
	}

	b.nextKey = nil
	return event, nil
}

func (b *BatchDecoder) buildColumns(
	holder *common.ColumnsHolder, handleKeyColumns map[string]interface{},
) []*model.Column {
	columnsCount := holder.Length()
	columns := make([]*model.Column, 0, columnsCount)
	for i := 0; i < columnsCount; i++ {
		columnType := holder.Types[i]
		name := columnType.Name()
		mysqlType := types.StrToType(strings.ToLower(columnType.DatabaseTypeName()))

		var value interface{}
		value = holder.Values[i].([]uint8)

		switch mysqlType {
		case mysql.TypeJSON:
			value = string(value.([]uint8))
		case mysql.TypeBit:
			value = common.MustBinaryLiteralToInt(value.([]uint8))
		}

		column := &model.Column{
			Name:  name,
			Type:  mysqlType,
			Value: value,
		}

		if _, ok := handleKeyColumns[name]; ok {
			column.Flag = model.PrimaryKeyFlag | model.HandleKeyFlag
		}
		columns = append(columns, column)
	}
	return columns
}

func (b *BatchDecoder) assembleHandleKeyOnlyEvent(
	ctx context.Context, handleKeyOnlyEvent *commonEvent.DMLEvent,
) *commonEvent.DMLEvent {
	var (
		schema   = handleKeyOnlyEvent.TableInfo.GetSchemaName()
		table    = handleKeyOnlyEvent.TableInfo.GetTableName()
		commitTs = handleKeyOnlyEvent.CommitTs
	)

	tableInfo := handleKeyOnlyEvent.TableInfo
	if handleKeyOnlyEvent.IsInsert() {
		conditions := make(map[string]interface{}, len(handleKeyOnlyEvent.Columns))
		for _, col := range handleKeyOnlyEvent.Columns {
			colName := tableInfo.ForceGetColumnName(col.ColumnID)
			conditions[colName] = col.Value
		}
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
		columns := b.buildColumns(holder, conditions)
		indexColumns := model.GetHandleAndUniqueIndexOffsets4Test(columns)
		handleKeyOnlyEvent.TableInfo = model.BuildTableInfo(schema, table, columns, indexColumns)
		handleKeyOnlyEvent.Columns = model.Columns2ColumnDatas(columns, handleKeyOnlyEvent.TableInfo)
	} else if handleKeyOnlyEvent.IsDelete() {
		conditions := make(map[string]interface{}, len(handleKeyOnlyEvent.PreColumns))
		for _, col := range handleKeyOnlyEvent.PreColumns {
			colName := tableInfo.ForceGetColumnName(col.ColumnID)
			conditions[colName] = col.Value
		}
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
		preColumns := b.buildColumns(holder, conditions)
		indexColumns := model.GetHandleAndUniqueIndexOffsets4Test(preColumns)
		handleKeyOnlyEvent.TableInfo = model.BuildTableInfo(schema, table, preColumns, indexColumns)
		handleKeyOnlyEvent.PreColumns = model.Columns2ColumnDatas(preColumns, handleKeyOnlyEvent.TableInfo)
	} else if handleKeyOnlyEvent.IsUpdate() {
		conditions := make(map[string]interface{}, len(handleKeyOnlyEvent.Columns))
		for _, col := range handleKeyOnlyEvent.Columns {
			colName := tableInfo.ForceGetColumnName(col.ColumnID)
			conditions[colName] = col.Value
		}
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
		columns := b.buildColumns(holder, conditions)
		indexColumns := model.GetHandleAndUniqueIndexOffsets4Test(columns)
		handleKeyOnlyEvent.TableInfo = model.BuildTableInfo(schema, table, columns, indexColumns)
		handleKeyOnlyEvent.Columns = model.Columns2ColumnDatas(columns, handleKeyOnlyEvent.TableInfo)

		conditions = make(map[string]interface{}, len(handleKeyOnlyEvent.PreColumns))
		for _, col := range handleKeyOnlyEvent.PreColumns {
			colName := tableInfo.ForceGetColumnName(col.ColumnID)
			conditions[colName] = col.Value
		}
		holder = common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
		preColumns := b.buildColumns(holder, conditions)
		handleKeyOnlyEvent.PreColumns = model.Columns2ColumnDatas(preColumns, handleKeyOnlyEvent.TableInfo)
	}

	return handleKeyOnlyEvent
}

func (b *BatchDecoder) assembleEventFromClaimCheckStorage(ctx context.Context) (*commonEvent.DMLEvent, error) {
	_, claimCheckFileName := filepath.Split(b.nextKey.ClaimCheckLocation)
	b.nextKey = nil
	data, err := b.storage.ReadFile(ctx, claimCheckFileName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	claimCheckM, err := common.UnmarshalClaimCheckMessage(data)
	if err != nil {
		return nil, errors.Trace(err)
	}

	version := binary.BigEndian.Uint64(claimCheckM.Key[:8])
	if version != codec.BatchVersion1 {
		return nil, errors.ErrOpenProtocolCodecInvalidData.
			GenWithStack("unexpected key format version")
	}

	key := claimCheckM.Key[8:]
	keyLen := binary.BigEndian.Uint64(key[:8])
	key = key[8 : keyLen+8]
	msgKey := new(messageKey)
	if err = msgKey.Decode(key); err != nil {
		return nil, errors.Trace(err)
	}

	valueLen := binary.BigEndian.Uint64(claimCheckM.Value[:8])
	value := claimCheckM.Value[8 : valueLen+8]
	value, err = common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		return nil, errors.WrapError(errors.ErrOpenProtocolCodecInvalidData, err)
	}

	rowMsg := new(messageRow)
	if err = rowMsg.decode(value); err != nil {
		return nil, errors.Trace(err)
	}

	event := b.msgToRowChange(msgKey, rowMsg)

	return event, nil
}

func (b *BatchDecoder) msgToRowChange(key *messageKey, value *messageRow) *commonEvent.DMLEvent {
	e := new(commonEvent.DMLEvent)
	// TODO: we lost the startTs from kafka message
	// startTs-based txn filter is out of work
	e.CommitTs = key.Ts

	if len(value.Delete) != 0 {
		preCols := codecColumns2RowChangeColumns(value.Delete)
		indexColumns := model.GetHandleAndUniqueIndexOffsets4Test(preCols)
		e.TableInfo = model.BuildTableInfo(key.Schema, key.Table, preCols, indexColumns)
		e.PreColumns = model.Columns2ColumnDatas(preCols, e.TableInfo)
	} else {
		cols := codecColumns2RowChangeColumns(value.Update)
		preCols := codecColumns2RowChangeColumns(value.PreColumns)
		indexColumns := model.GetHandleAndUniqueIndexOffsets4Test(cols)
		e.TableInfo = model.BuildTableInfo(key.Schema, key.Table, cols, indexColumns)
		e.Columns = model.Columns2ColumnDatas(cols, e.TableInfo)
		e.PreColumns = model.Columns2ColumnDatas(preCols, e.TableInfo)
	}

	// TODO: we lost the tableID from kafka message
	if key.Partition != nil {
		e.PhysicalTableID = *key.Partition
		e.TableInfo.TableName.IsPartition = true
	} else {
		e.PhysicalTableID = b.tableIDAllocator.AllocateTableID(key.Schema, key.Table)
	}
	return e
}

func codecColumns2RowChangeColumns(cols map[string]column) []*commonType.Column {
	if len(cols) == 0 {
		return nil
	}
	columns := make([]*commonType.Column, 0, len(cols))
	for name, col := range cols {
		c := col.toRowChangeColumn(name)
		columns = append(columns, c)
	}
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].Name < columns[j].Name
	})
	return columns
}
