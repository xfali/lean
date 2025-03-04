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

package handler

import (
	"context"
	"github.com/xfali/lean/resultset"
	"github.com/xfali/lean/statement"
)

type Handler interface {
	Prepare(ctx context.Context, sql string) (statement.Statement, error)

	Query(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error)

	Execute(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error)
}
