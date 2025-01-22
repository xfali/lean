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

package errors

import "fmt"

type errCode struct {
	code    string
	message string
}

var (
	ExecutorCommitError        = gobatisError("21001", "executor commit error")
	ExecutorBeginError         = gobatisError("21002", "executor was closed when transaction begin")
	ExecutorQueryError         = gobatisError("21003", "executor was closed when exec sql")
	ExecutorGetConnectionError = gobatisError("21003", "executor get connection error")
	TransactionWithoutBegin    = gobatisError("22001", "Transaction without begin")
	TransactionCommitError     = gobatisError("22002", "Transaction commit error")
	TransactionBusinessError   = gobatisError("22003", "Business error in transaction")
	TransactionBeginError      = gobatisError("22004", "Transaction begin error")
	TransactionRollbackError   = gobatisError("22005", "Transaction rollback error")
	ConnectionPrepareError     = gobatisError("23001", "Connection prepare error")
	StatementQueryError        = gobatisError("24001", "statement query error")
	StatementExecError         = gobatisError("24002", "statement exec error")
	QueryTypeError             = gobatisError("25001", "select data convert error")
	HandlerQueryError          = gobatisError("26001", "Connection prepare error")
	HandlerExecuteError        = gobatisError("26002", "statement query error")
	ResultPointerIsNil         = gobatisError("31000", "result type is a nil pointer")
	ResultIsnotPointer         = gobatisError("31001", "result type is not pointer")
	ResultPtrValueIsPointer    = gobatisError("31002", "result type is pointer of pointer")
	RunnerNotReady             = gobatisError("31003", "Runner not ready, may sql or param have some error")
	ResultNameNotFound         = gobatisError("31004", "result name not found")
	ResultSelectEmptyValue     = gobatisError("31005", "select return empty value")
	ResultSetValueFailed       = gobatisError("31006", "result set value failed")
)

func gobatisError(code, message string) errCode {
	return errCode{
		code:    code,
		message: message,
	}
}

func (e errCode) Error() string {
	return fmt.Sprintf("%s - %s", e.code, e.message)
}

func (e *errCode) Format(err error) *errCode {
	e.message = fmt.Sprintf("%s bussiness error: %v ", e.message, err)
	return e
}
