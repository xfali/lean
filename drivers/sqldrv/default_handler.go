/*
 * Copyright (C) 2024-2025, Xiongfa Li.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sqldrv

import (
	"context"
	"database/sql"
	"github.com/xfali/lean/errors"
	"github.com/xfali/lean/resultset"
	"github.com/xfali/lean/statement"
)

type defaultHandler sql.DB

func (conn *defaultHandler) Prepare(sqlStr string) (statement.Statement, error) {
	db := (*sql.DB)(conn)
	s, err := db.Prepare(sqlStr)
	if err != nil {
		return nil, errors.ConnectionPrepareError
	}
	return (*sqlStatement)(s), nil
}

func (conn *defaultHandler) Query(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	db := (*sql.DB)(conn)
	rows, err := db.QueryContext(ctx, stmt, params...)
	if err != nil {
		return nil, errors.HandlerQueryError.Format(err)
	}
	return NewSqlQueryResultSet(rows), nil
}

func (conn *defaultHandler) Execute(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	db := (*sql.DB)(conn)
	r, err := db.ExecContext(ctx, stmt, params...)
	if err != nil {
		return nil, errors.HandlerExecuteError.Format(err)
	}
	return NewSqlExecResultSet(r), nil
}

type transactionHandler struct {
	tx *sql.Tx
}

func (transConnection *transactionHandler) Prepare(sqlStr string) (statement.Statement, error) {
	ret := &transactionStatement{
		tx:  transConnection.tx,
		sql: sqlStr,
	}
	return ret, nil
}

func (transConnection *transactionHandler) Query(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	db := transConnection.tx
	rows, err := db.QueryContext(ctx, stmt, params...)
	if err != nil {
		return nil, errors.HandlerQueryError.Format(err)
	}
	return NewSqlQueryResultSet(rows), nil
}

func (transConnection *transactionHandler) Execute(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	db := transConnection.tx
	ret, err := db.ExecContext(ctx, stmt, params...)
	if err != nil {
		return nil, errors.HandlerExecuteError.Format(err)
	}
	return NewSqlExecResultSet(ret), nil
}
