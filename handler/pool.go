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
	"github.com/xfali/lean/statement"
	"sync"
)

type StatementPool interface {
	Get(h Handler, sql string) (statement.Statement, bool)

	Put(h Handler, sql string, stmt statement.Statement)

	Purge(h Handler)

	PurgeAll()
}

type defaultPool struct {
	stmtMap    map[Handler]map[string]statement.Statement
	stmtLocker sync.RWMutex
}

func NewPool() *defaultPool {
	return &defaultPool{
		stmtMap: map[Handler]map[string]statement.Statement{},
	}
}

func (p *defaultPool) Get(h Handler, sql string) (statement.Statement, bool) {
	p.stmtLocker.RLock()
	defer p.stmtLocker.RUnlock()

	if v, ok := p.stmtMap[h]; ok {
		if len(v) > 0 {
			s, have := v[sql]
			return s, have
		}
	}
	return nil, false
}

func (p *defaultPool) PurgeAll() {
	p.stmtLocker.Lock()
	defer p.stmtLocker.Unlock()

	for h, ss := range p.stmtMap {
		if len(ss) > 0 {
			for _, v := range ss {
				_ = v.Close()
			}
		}
		delete(p.stmtMap, h)
	}
	p.stmtMap = map[Handler]map[string]statement.Statement{}
}

func (p *defaultPool) Purge(h Handler) {
	p.stmtLocker.Lock()
	defer p.stmtLocker.Unlock()

	if ss, ok := p.stmtMap[h]; ok {
		if len(ss) > 0 {
			for _, v := range ss {
				_ = v.Close()
			}
			delete(p.stmtMap, h)
		}
	}
}

func (p *defaultPool) Put(h Handler, sql string, stmt statement.Statement) {
	p.stmtLocker.Lock()
	defer p.stmtLocker.Unlock()

	v, ok := p.stmtMap[h]
	if !ok {
		v = map[string]statement.Statement{}
		p.stmtMap[h] = v
	}
	v[sql] = stmt
}
