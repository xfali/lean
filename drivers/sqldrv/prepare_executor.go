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
	"github.com/xfali/lean/errors"
	"github.com/xfali/lean/resultset"
	"github.com/xfali/lean/transaction"
	"github.com/xfali/xlog"
)

type PrepareExecutor struct {
	logger      xlog.Logger
	transaction transaction.Transaction
	closed      bool
}

func NewPrepareExecutor(transaction transaction.Transaction) *PrepareExecutor {
	return &PrepareExecutor{
		logger:      xlog.GetLogger(),
		transaction: transaction,
	}
}

func (exec *PrepareExecutor) Close(ctx context.Context, rollback bool) error {
	defer func() {
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

func (exec *PrepareExecutor) Query(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	if exec.closed {
		return nil, errors.ExecutorQueryError
	}

	conn := exec.transaction.GetHandler()
	if conn == nil {
		return nil, errors.ExecutorGetConnectionError
	}

	//FIXME: stmt must be close, use stmtCache instead
	statement, err := conn.Prepare(stmt)
	defer func() {
		err := statement.Close()
		if err != nil {
			exec.logger.Errorln(err)
		}
	}()
	if err != nil {
		return nil, err
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

	//FIXME: stmt must be close, use stmtCache instead
	statement, err := conn.Prepare(stmt)
	defer func() {
		err := statement.Close()
		if err != nil {
			exec.logger.Errorln(err)
		}
	}()

	if err != nil {
		return nil, err
	}
	return statement.Execute(ctx, params...)
}

func (exec *PrepareExecutor) Begin(ctx context.Context) error {
	if exec.closed {
		return errors.ExecutorBeginError
	}

	return exec.transaction.Begin(ctx)
}

func (exec *PrepareExecutor) Commit(ctx context.Context, require bool) error {
	if exec.closed {
		return errors.ExecutorCommitError
	}

	if require {
		return exec.transaction.Commit(ctx)
	}

	return nil
}

func (exec *PrepareExecutor) Rollback(ctx context.Context, require bool) error {
	if !exec.closed {
		if require {
			return exec.transaction.Rollback(ctx)
		}
	}
	return nil
}
