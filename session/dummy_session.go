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

package session

import (
	"context"
	"github.com/xfali/lean/resultset"
	"time"
)

type dummySession struct {
	sleep time.Duration
}

func NewDummySession(sleep time.Duration) *dummySession {
	return &dummySession{
		sleep: sleep,
	}
}

func (d dummySession) Ping(ctx context.Context) bool {
	if d.sleep > 0 {
		time.Sleep(d.sleep)
	}
	return true
}

func (d dummySession) Query(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	if d.sleep > 0 {
		time.Sleep(d.sleep)
	}
	return nil, nil
}

func (d dummySession) Execute(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	if d.sleep > 0 {
		time.Sleep(d.sleep)
	}
	return nil, nil
}

func (d dummySession) Begin(ctx context.Context) error {
	if d.sleep > 0 {
		time.Sleep(d.sleep)
	}
	return nil
}

func (d dummySession) Commit(ctx context.Context) error {
	if d.sleep > 0 {
		time.Sleep(d.sleep)
	}
	return nil
}

func (d dummySession) Rollback(ctx context.Context) error {
	if d.sleep > 0 {
		time.Sleep(d.sleep)
	}
	return nil
}

func (d dummySession) Close() error {
	if d.sleep > 0 {
		time.Sleep(d.sleep)
	}
	return nil
}
