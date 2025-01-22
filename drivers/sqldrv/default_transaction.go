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

package sqldrv

import (
	"context"
	"database/sql"
	"github.com/xfali/lean/errors"
	"github.com/xfali/lean/handler"
	"github.com/xfali/lean/transaction"
	"sync"
	"sync/atomic"
)

type defaultTransaction struct {
	db *sql.DB
	tx *sql.Tx

	state  int32
	locker sync.Mutex
}

func NewDefaultTransaction(db *sql.DB) *defaultTransaction {
	ret := &defaultTransaction{
		db:    db,
		state: transaction.StateUnknown,
	}
	return ret
}

func (trans *defaultTransaction) GetHandler() handler.Handler {
	trans.locker.Lock()
	defer trans.locker.Unlock()

	if trans.tx == nil {
		return (*defaultHandler)(trans.db)
	} else {
		return (*transactionHandler)(trans.tx)
	}
}

func (trans *defaultTransaction) Close() error {
	return nil
}

func (trans *defaultTransaction) Ping(ctx context.Context) bool {
	return trans.db.PingContext(ctx) == nil
}

func (trans *defaultTransaction) Begin(ctx context.Context, successCallback func(handler.Handler) error) error {
	if !atomic.CompareAndSwapInt32(&trans.state, transaction.StateUnknown, transaction.StateBegin) {
		return errors.TransactionHaveBegin
	}
	tx, err := trans.db.Begin()

	if err != nil {
		return errors.TransactionBeginError.Format(err)
	}
	trans.locker.Lock()
	trans.tx = tx
	trans.locker.Unlock()
	if successCallback != nil {
		return successCallback((*transactionHandler)(trans.tx))
	}
	return nil
}

func (trans *defaultTransaction) Commit(ctx context.Context, successCallback func(handler.Handler) error) error {
	if !atomic.CompareAndSwapInt32(&trans.state, transaction.StateBegin, transaction.StateCommitting) {
		return errors.TransactionWithoutBegin
	}

	trans.locker.Lock()
	tx := trans.tx
	trans.locker.Unlock()

	if tx == nil {
		atomic.StoreInt32(&trans.state, transaction.StateBegin)
		return errors.TransactionWithoutBegin
	}

	defer func() {
		if o := recover(); o != nil {
			atomic.StoreInt32(&trans.state, transaction.StateBegin)
			panic(o)
		}
	}()
	err := tx.Commit()
	if err != nil {
		atomic.StoreInt32(&trans.state, transaction.StateBegin)
		return errors.TransactionCommitError.Format(err)
	} else {
		if successCallback != nil {
			err = successCallback((*transactionHandler)(trans.tx))
		}
		atomic.StoreInt32(&trans.state, transaction.StateUnknown)
		return err
	}
}

func (trans *defaultTransaction) Rollback(ctx context.Context, successCallback func(handler.Handler) error) error {
	if !atomic.CompareAndSwapInt32(&trans.state, transaction.StateBegin, transaction.StateRollbacking) {
		return errors.TransactionWithoutBegin
	}

	trans.locker.Lock()
	tx := trans.tx
	trans.locker.Unlock()

	if tx == nil {
		atomic.StoreInt32(&trans.state, transaction.StateBegin)
		return errors.TransactionWithoutBegin
	}

	defer func() {
		if o := recover(); o != nil {
			atomic.StoreInt32(&trans.state, transaction.StateBegin)
			panic(o)
		}
	}()
	err := tx.Rollback()
	if err != nil {
		atomic.StoreInt32(&trans.state, transaction.StateBegin)
		return errors.TransactionRollbackError.Format(err)
	} else {
		if successCallback != nil {
			err = successCallback((*transactionHandler)(trans.tx))
		}
		atomic.StoreInt32(&trans.state, transaction.StateUnknown)
		return err
	}
}
