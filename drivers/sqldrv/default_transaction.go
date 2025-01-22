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
	"database/sql"
	"github.com/xfali/lean/errors"
	"github.com/xfali/lean/handler"
)

type defaultTransaction struct {
	db *sql.DB
	tx *sql.Tx
}

func NewDefaultTransaction(db *sql.DB) *defaultTransaction {
	ret := &defaultTransaction{db: db}
	return ret
}

func (trans *defaultTransaction) GetHandler() handler.Handler {
	if trans.tx == nil {
		return (*defaultHandler)(trans.db)
	} else {
		return &transactionHandler{tx: trans.tx}
	}
}

func (trans *defaultTransaction) Close() error {
	return nil
}

func (trans *defaultTransaction) Begin() error {
	tx, err := trans.db.Begin()
	if err != nil {
		return errors.TransactionBeginError.Format(err)
	}
	trans.tx = tx
	return nil
}

func (trans *defaultTransaction) Commit() error {
	if trans.tx == nil {
		return errors.TransactionWithoutBegin
	}

	err := trans.tx.Commit()
	if err != nil {
		return errors.TransactionCommitError.Format(err)
	}
	return nil
}

func (trans *defaultTransaction) Rollback() error {
	if trans.tx == nil {
		return errors.TransactionWithoutBegin
	}

	err := trans.tx.Rollback()
	if err != nil {
		return errors.TransactionRollbackError.Format(err)
	}
	return nil
}
