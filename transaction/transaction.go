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

package transaction

import (
	"context"
	"github.com/xfali/lean/handler"
)

const (
	StateUnknown     = 0
	StateBegin       = 1
	StateCommitting  = 2
	StateRollbacking = 4
)

type Transaction interface {
	Close() error

	Ping(ctx context.Context) bool

	GetHandler() handler.Handler

	Begin(ctx context.Context, successCallback func(handler.Handler) error) error

	Commit(ctx context.Context, successCallback func(handler.Handler) error) error

	Rollback(ctx context.Context, successCallback func(handler.Handler) error) error
}
