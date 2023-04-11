/*
 * Copyright (C) 2023, Xiongfa Li.
 * All rights reserved.
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
	"database/sql"
	"errors"
)

type sqlQueryResultSet struct {
	rows *sql.Rows
}

func NewSqlQueryResultSet(rows *sql.Rows) *sqlQueryResultSet {
	return &sqlQueryResultSet{
		rows: rows,
	}
}

func (r *sqlQueryResultSet) Columns() ([]string, error) {
	return r.rows.Columns()
}

func (r *sqlQueryResultSet) Next() bool {
	return r.rows.Next()
}

func (r *sqlQueryResultSet) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

func (r *sqlQueryResultSet) Close() error {
	return nil
}

func (r *sqlQueryResultSet) LastInsertId() (int64, error) {
	return 0, errors.New("Not support ")
}

func (r *sqlQueryResultSet) RowsAffected() (int64, error) {
	return 0, errors.New("Not support ")
}

type sqlExecResultSet struct {
	ret sql.Result
}

func NewSqlExecResultSet(ret sql.Result) *sqlExecResultSet {
	return &sqlExecResultSet{
		ret: ret,
	}
}

func (r *sqlExecResultSet) Columns() ([]string, error) {
	return nil, errors.New("SQL exec result not support Columns ")
}

func (r *sqlExecResultSet) Next() bool {
	return false
}

func (r *sqlExecResultSet) Scan(dest ...interface{}) error {
	return errors.New("SQL exec result not support Scan ")
}

func (r *sqlExecResultSet) Close() error {
	return nil
}

func (r *sqlExecResultSet) LastInsertId() (int64, error) {
	return r.ret.LastInsertId()
}

func (r *sqlExecResultSet) RowsAffected() (int64, error) {
	return r.ret.RowsAffected()
}
