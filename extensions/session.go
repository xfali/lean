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

package extensions

import (
	"context"
	"github.com/xfali/aop"
	"github.com/xfali/lean/resultset"
	"github.com/xfali/lean/session"
)

type SessionEx struct {
	sess  session.Session
	proxy aop.Proxy
}

func NewSessionEx(sess session.Session) *SessionEx {
	ret := &SessionEx{
		sess:  sess,
		proxy: aop.New(sess),
	}
	return ret
}

func (s *SessionEx) Extend(cut aop.PointCut, advice aop.Advice) Extension {
	s.proxy.AddAdvisor(cut, advice)
	return s
}

func (s *SessionEx) Ping(ctx context.Context) (ret bool) {
	r, err := s.proxy.Call(Caller(), ctx)
	if err != nil {
		return false
	}
	if r[0] != nil {
		ret = r[0].(bool)
	}
	return
}

func (s *SessionEx) Query(ctx context.Context, stmt string, params ...interface{}) (ret resultset.Result, e error) {
	r, err := s.proxy.Call(Caller(), ctx, stmt, params)
	if err != nil {
		return nil, err
	}
	if r[0] != nil {
		ret = r[0].(resultset.Result)
	}
	if r[1] != nil {
		e = r[1].(error)
	}
	return
}

func (s *SessionEx) Execute(ctx context.Context, stmt string, params ...interface{}) (ret resultset.Result, e error) {
	r, err := s.proxy.Call(Caller(), ctx, stmt, params)
	if err != nil {
		return nil, err
	}
	if r[0] != nil {
		ret = r[0].(resultset.Result)
	}
	if r[1] != nil {
		e = r[1].(error)
	}
	return
}

func (s *SessionEx) Begin(ctx context.Context) (e error) {
	r, err := s.proxy.Call(Caller(), ctx)
	if err != nil {
		return err
	}
	if r[0] != nil {
		e = r[0].(error)
	}
	return
}

func (s *SessionEx) Commit(ctx context.Context) (e error) {
	r, err := s.proxy.Call(Caller(), ctx)
	if err != nil {
		return err
	}
	if r[0] != nil {
		e = r[0].(error)
	}
	return
}

func (s *SessionEx) Rollback(ctx context.Context) (e error) {
	r, err := s.proxy.Call(Caller(), ctx)
	if err != nil {
		return err
	}
	if r[0] != nil {
		e = r[0].(error)
	}
	return
}

func (s *SessionEx) Close() (e error) {
	r, err := s.proxy.Call(Caller())
	if err != nil {
		return err
	}
	if r[0] != nil {
		e = r[0].(error)
	}
	return
}
