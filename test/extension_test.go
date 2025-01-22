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

package test

import (
	"context"
	"fmt"
	"github.com/xfali/aop"
	"github.com/xfali/lean/executor"
	"github.com/xfali/lean/extensions"
	"github.com/xfali/lean/session"
	"testing"
	"time"
)

func TestExecutorExtensions(t *testing.T) {
	runExecutor(createExecutorEx(t.Log))
}

func BenchmarkExtensionsExecutor(t *testing.B) {
	for i := 0; i < t.N; i++ {
		runExecutor(createExecutorEx(none))
	}
}

func BenchmarkNormalExecutor(t *testing.B) {
	for i := 0; i < t.N; i++ {
		runExecutor(executor.NewDummyExecutor(2 * time.Millisecond))
	}
}

func none(...any) {

}

func createExecutorEx(t func(...any)) executor.Executor {
	exec := extensions.NewExecutorEx(executor.NewDummyExecutor(2 * time.Millisecond))
	exec.Extend(aop.PointCutRegExp("", "(.*?)", nil, nil), func(invocation aop.Invocation, params []interface{}) (ret []interface{}) {
		t(invocation.MethodName(), " Params: ", fmt.Sprintln(params...))
		ret = invocation.Invoke(params)
		if len(ret) > 0 {
			t(invocation.MethodName(), " results: ", fmt.Sprintln(ret...))
		} else {
			t(invocation.MethodName(), " results: nil")
		}
		return ret
	})
	return exec
}

func runExecutor(exec executor.Executor) {
	ctx := context.Background()
	exec.Begin(ctx)
	exec.Query(ctx, "select * from tbl where id = ? and name = ?", "hello", "world")
	exec.Execute(ctx, "update tbl where id = ? and name = ?", "hello", "world")
	exec.Commit(ctx, true)
	exec.Rollback(ctx, true)
	exec.Close(ctx, false)
}

func TestSessionExtensions(t *testing.T) {
	runSession(createSessionEx(t.Log))
}

func BenchmarkExtensionsSession(t *testing.B) {
	for i := 0; i < t.N; i++ {
		runSession(createSessionEx(none))
	}
}

func BenchmarkNormalSession(t *testing.B) {
	for i := 0; i < t.N; i++ {
		runSession(session.NewDummySession(2 * time.Millisecond))
	}
}

func createSessionEx(t func(...any)) session.Session {
	exec := extensions.NewSessionEx(session.NewDummySession(2 * time.Millisecond))
	exec.Extend(aop.PointCutRegExp("", "(.*?)", nil, nil), func(invocation aop.Invocation, params []interface{}) (ret []interface{}) {
		t(invocation.MethodName(), " Params: ", fmt.Sprintln(params...))
		ret = invocation.Invoke(params)
		if len(ret) > 0 {
			t(invocation.MethodName(), " results: ", fmt.Sprintln(ret...))
		} else {
			t(invocation.MethodName(), " results: nil")
		}
		return ret
	})
	return exec
}

func runSession(sess session.Session) {
	ctx := context.Background()
	sess.Begin(ctx)
	sess.Query(ctx, "select * from tbl where id = ? and name = ?", "hello", "world")
	sess.Execute(ctx, "update tbl where id = ? and name = ?", "hello", "world")
	sess.Commit(ctx)
	sess.Rollback(ctx)
	sess.Close()
}
