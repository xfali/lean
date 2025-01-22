/*
 * Copyright (C) 2023-2025, Xiongfa Li.
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

package executor

import (
	"context"
	"github.com/xfali/lean/errors"
	"github.com/xfali/lean/handler"
	"github.com/xfali/lean/logger"
	"github.com/xfali/lean/resultset"
	"github.com/xfali/lean/transaction"
	"github.com/xfali/xlog"
)

type PrepareExecutor struct {
	logger      xlog.Logger
	transaction transaction.Transaction
	closed      bool

	pool handler.StatementPool
}

func NewPrepareExecutor(transaction transaction.Transaction, pool handler.StatementPool) *PrepareExecutor {
	return &PrepareExecutor{
		logger:      logger.GetLogger(),
		transaction: transaction,
		pool:        pool,
	}
}

func (exec *PrepareExecutor) Close(ctx context.Context, rollback bool) error {
	defer func() {
		exec.pool.PurgeAll()
		if exec.transaction != nil {
			err := exec.transaction.Close()
			if err != nil {
				exec.logger.Errorln(err)
			}
		}
		exec.transaction = nil
		exec.closed = true
	}()

	if rollback {
		return exec.Rollback(ctx, true)
	}
	return nil
}

func (exec *PrepareExecutor) Ping(ctx context.Context) bool {
	if exec.closed {
		return false
	}

	return exec.transaction.Ping(ctx)
}

func (exec *PrepareExecutor) Query(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	if exec.closed {
		return nil, errors.ExecutorQueryError
	}

	conn := exec.transaction.GetHandler()
	if conn == nil {
		return nil, errors.ExecutorGetConnectionError
	}

	statement, have := exec.pool.Get(conn, stmt)
	if !have {
		st, err := conn.Prepare(ctx, stmt)
		if err != nil {
			return nil, err
		}
		statement = st
		exec.pool.Put(conn, stmt, statement)
	}
	return statement.Query(ctx, params...)
}

func (exec *PrepareExecutor) Execute(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	if exec.closed {
		return nil, errors.ExecutorQueryError
	}

	conn := exec.transaction.GetHandler()
	if conn == nil {
		return nil, errors.ExecutorGetConnectionError
	}

	statement, have := exec.pool.Get(conn, stmt)
	if !have {
		st, err := conn.Prepare(ctx, stmt)
		if err != nil {
			return nil, err
		}
		statement = st
		exec.pool.Put(conn, stmt, statement)
	}
	return statement.Execute(ctx, params...)
}

func (exec *PrepareExecutor) Begin(ctx context.Context) error {
	if exec.closed {
		return errors.ExecutorBeginError
	}

	return exec.transaction.Begin(ctx, nil)
}

func (exec *PrepareExecutor) Commit(ctx context.Context, require bool) error {
	if exec.closed {
		return errors.ExecutorCommitError
	}

	if require {
		return exec.transaction.Commit(ctx, func(handler handler.Handler) error {
			exec.pool.Purge(handler)
			return nil
		})
	}

	return nil
}

func (exec *PrepareExecutor) Rollback(ctx context.Context, require bool) error {
	if !exec.closed {
		if require {
			return exec.transaction.Rollback(ctx, func(handler handler.Handler) error {
				exec.pool.Purge(handler)
				return nil
			})
		}
	}
	return nil
}
