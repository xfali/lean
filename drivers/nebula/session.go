/*
 * Copyright (C) 2023, Xiongfa Li.
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

package nebula

import (
	"context"
	"fmt"
	nebula_go "github.com/vesoft-inc/nebula-go/v3"
	"github.com/xfali/lean/resultset"
)

type nebulaSession struct {
	sess *nebula_go.Session
}

func (s *nebulaSession) Query(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	return s.Execute(ctx, stmt, params...)
}

func (s *nebulaSession) Execute(ctx context.Context, stmt string, params ...interface{}) (resultset.Result, error) {
	var rs *nebula_go.ResultSet
	var err error
	if len(params) == 0 {
		rs, err = s.sess.Execute(stmt)
	} else {
		pm, err2 := slice2map(params...)
		if err2 != nil {
			return nil, err2
		}
		rs, err = s.sess.ExecuteWithParameter(stmt, pm)
	}
	err = checkResultSet(rs, err)
	if err != nil {
		return nil, err
	}

	return NewNebulaResultSet(rs), nil
}

func (s *nebulaSession) Close() error {
	s.sess.Release()
	return nil
}

func slice2map(params ...interface{}) (map[string]interface{}, error) {
	pm := make(map[string]interface{}, len(params)>>1)
	var k string
	for i := range params {
		if i%2 == 0 {
			if kk, ok := params[i].(string); ok {
				k = kk
			} else {
				return nil, fmt.Errorf("Key [%v] not a string ", params[i])
			}
		} else {
			pm[k] = params[i]
		}
	}
	return pm, nil
}
