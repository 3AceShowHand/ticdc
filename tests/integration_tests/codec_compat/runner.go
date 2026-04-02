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
	"context"
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
)

type Runner struct {
	db             *sql.DB
	statementLimit time.Duration
}

func NewRunner(dsn string, statementLimit time.Duration) (*Runner, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	return &Runner{
		db:             db,
		statementLimit: statementLimit,
	}, nil
}

func (r *Runner) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return errors.Trace(r.db.Close())
}

func (r *Runner) Execute(ctx context.Context, stmt Statement) (uint64, error) {
	execCtx, cancel := context.WithTimeout(ctx, r.statementLimit)
	defer cancel()

	if _, err := r.db.ExecContext(execCtx, stmt.SQL); err != nil {
		return 0, errors.Annotatef(err, "execute %s stmt#%d", stmt.SourceFile, stmt.Ordinal)
	}

	var tso uint64
	if err := r.db.QueryRowContext(execCtx, "select @@tidb_current_ts").Scan(&tso); err != nil {
		return 0, errors.Annotatef(err, "query current tso after %s stmt#%d", stmt.SourceFile, stmt.Ordinal)
	}

	return tso, nil
}
