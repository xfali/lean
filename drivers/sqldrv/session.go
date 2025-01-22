/*
 * Copyright (C) 2023-2025, Xiongfa Li.
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
	"github.com/xfali/lean/executor"
	"github.com/xfali/lean/resultset"
	"time"
)

type ExecutorFactory func(db *sql.DB) (executor.Executor, error)

type SessionOpt func(*sqlSession)

type sqlSession struct {
	exec    executor.Executor
	execFac ExecutorFactory
}

func NewSqlSession(db *sql.DB, opts ...SessionOpt) *sqlSession {
	ret := &sqlSession{
		execFac: defaultExecutorFactory,
	}
	for _, opt := range opts {
		opt(ret)
	}
	exec, err := ret.execFac(db)
	if err != nil {
		return nil
	}
	ret.exec = exec
	return ret
}

func defaultExecutorFactory(db *sql.DB) (executor.Executor, error) {
	tx := NewDefaultTransaction(db)
	exec := executor.NewSimpleExecutor(tx)
	return exec, nil
}

func (s *sqlSession) Ping(ctx context.Context) bool {
	return s.exec.Ping(ctx)
}

func (s *sqlSession) Query(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	return s.exec.Query(ctx, stmt, params)
}

func (s *sqlSession) Execute(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	return s.exec.Execute(ctx, stmt, params)
}

func (s *sqlSession) Begin(ctx context.Context) error {
	return s.exec.Begin(ctx)
}

func (s *sqlSession) Commit(ctx context.Context) error {
	return s.exec.Commit(ctx, true)
}

func (s *sqlSession) Rollback(ctx context.Context) error {
	return s.exec.Rollback(ctx, true)
}

func (s *sqlSession) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.exec.Close(ctx, false)
}

type sessOpts struct {
}

var SessOpts sessOpts

func (o sessOpts) SetExecutorFactory(execFac ExecutorFactory) SessionOpt {
	return func(session *sqlSession) {
		session.execFac = execFac
	}
}
