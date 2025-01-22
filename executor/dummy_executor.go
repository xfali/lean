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
	"github.com/xfali/lean/resultset"
	"time"
)

type dummyExecutor struct {
	sleep time.Duration
}

func NewDummyExecutor(sleep time.Duration) *dummyExecutor {
	return &dummyExecutor{
		sleep: sleep,
	}
}

func (d dummyExecutor) Close(ctx context.Context, rollback bool) error {
	if d.sleep > 0 {
		time.Sleep(d.sleep)
	}
	return nil
}

func (d dummyExecutor) Ping(ctx context.Context) bool {
	if d.sleep > 0 {
		time.Sleep(d.sleep)
	}
	return true
}

func (d dummyExecutor) Query(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	if d.sleep > 0 {
		time.Sleep(d.sleep)
	}
	return nil, nil
}

func (d dummyExecutor) Execute(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	if d.sleep > 0 {
		time.Sleep(d.sleep)
	}
	return nil, nil
}

func (d dummyExecutor) Begin(ctx context.Context) error {
	if d.sleep > 0 {
		time.Sleep(d.sleep)
	}
	return nil
}

func (d dummyExecutor) Commit(ctx context.Context, require bool) error {
	if d.sleep > 0 {
		time.Sleep(d.sleep)
	}
	return nil
}

func (d dummyExecutor) Rollback(ctx context.Context, require bool) error {
	if d.sleep > 0 {
		time.Sleep(d.sleep)
	}
	return nil
}
