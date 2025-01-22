/*
 * Copyright (C) 2025, Xiongfa Li.
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
	"github.com/xfali/lean/resultset"
)

type sqlStatement sql.Stmt

type transactionStatement struct {
	tx  *sql.Tx
	sql string
}

func (transStatement *transactionStatement) Query(ctx context.Context, params ...interface{}) (resultset.Result, error) {
	rows, err := transStatement.tx.QueryContext(ctx, transStatement.sql, params...)
	if err != nil {
		return nil, err
	}
	return NewSqlQueryResultSet(rows), nil
}

func (transStatement *transactionStatement) Execute(ctx context.Context, params ...interface{}) (resultset.Result, error) {
	r, err := transStatement.tx.ExecContext(ctx, transStatement.sql, params...)
	if err != nil {
		return nil, err
	}
	return NewSqlExecResultSet(r), nil
}

func (transStatement *transactionStatement) Close() error {
	//Will be closed when commit or rollback
	return nil
}

func (s *sqlStatement) Query(ctx context.Context, params ...interface{}) (resultset.Result, error) {
	stmt := (*sql.Stmt)(s)
	rows, err := stmt.QueryContext(ctx, params...)
	if err != nil {
		return nil, err
	}
	return NewSqlQueryResultSet(rows), nil
}

func (s *sqlStatement) Execute(ctx context.Context, params ...interface{}) (resultset.Result, error) {
	stmt := (*sql.Stmt)(s)
	r, err := stmt.ExecContext(ctx, params...)
	if err != nil {
		return nil, err
	}
	return NewSqlExecResultSet(r), nil
}

func (s *sqlStatement) Close() error {
	stmt := (*sql.Stmt)(s)
	return stmt.Close()
}
