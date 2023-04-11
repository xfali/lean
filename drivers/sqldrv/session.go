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
	"context"
	"database/sql"
	"github.com/xfali/lean/resultset"
)

type sqlSession struct {
	db *sql.DB
}

func NewSqlSession(db *sql.DB) *sqlSession {
	return &sqlSession{
		db: db,
	}
}

func (s *sqlSession) Query(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	v, err := s.db.QueryContext(ctx, stmt, params...)
	if err != nil {
		return nil, err
	}
	return NewSqlQueryResultSet(v), nil
}

func (s *sqlSession) Execute(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	v, err := s.db.ExecContext(ctx, stmt, params...)
	if err != nil {
		return nil, err
	}

	return NewSqlExecResultSet(v), nil
}

func (s *sqlSession) Close() error {
	return nil
}
