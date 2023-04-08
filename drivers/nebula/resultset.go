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
	"errors"
	"fmt"
	nebula_go "github.com/vesoft-inc/nebula-go/v3"
	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/xfali/reflection"
)

type nebulaResultSet struct {
	rs    *nebula_go.ResultSet
	rows  []*nebula.Row
	index int
}

func NewNebulaResultSet(rs *nebula_go.ResultSet) *nebulaResultSet {
	rs.GetRowValuesByIndex()
	ret := &nebulaResultSet{
		rs:    rs,
		rows:  rs.GetRows(),
		index: 0,
	}
	return ret
}

func (r *nebulaResultSet) Columns() ([]string, error) {
	return r.rs.GetColNames(), nil
}

func (r *nebulaResultSet) Next() bool {
	return r.index != len(r.rows)
}

func (r *nebulaResultSet) Scan(dest ...interface{}) error {
	row := r.rows[r.index]
	for i, v := range row.GetValues() {
		if i < len(dest)-1 {
			if err := set2Value(dest[i], v); err != nil {
				return err
			}
		}
	}
	r.index++
	return nil
}

func (r *nebulaResultSet) Close() error {
	return nil
}

func (r *nebulaResultSet) LastInsertId() (int64, error) {
	return 0, errors.New("Not support ")
}

func (r *nebulaResultSet) RowsAffected() (int64, error) {
	return 0, errors.New("Not support ")
}

func set2Value(dest interface{}, value *nebula.Value) error {
	//if value.IsSetNVal() {
	//	return nil
	//} else if value.IsSetBVal() {
	//	return "bool"
	//} else if value.IsSetIVal() {
	//	return "int"
	//} else if value.IsSetFVal() {
	//	return "float"
	//} else if value.IsSetSVal() {
	//	return "string"
	//} else if value.IsSetDVal() {
	//	return "date"
	//} else if value.IsSetTVal() {
	//	return "time"
	//} else if value.IsSetDtVal() {
	//	return "datetime"
	//} else if value.IsSetVVal() {
	//	return "vertex"
	//} else if value.IsSetEVal() {
	//	return "edge"
	//} else if value.IsSetPVal() {
	//	return "path"
	//} else if value.IsSetLVal() {
	//	return "list"
	//} else if value.IsSetMVal() {
	//	return "map"
	//} else if value.IsSetUVal() {
	//	return "set"
	//} else if value.IsSetGgVal() {
	//	return "geography"
	//} else if value.IsSetDuVal() {
	//	return "duration"
	//}
	//return "empty"
	return nil
}

func checkResultSet(rs *nebula_go.ResultSet, err error) error {
	if !reflection.IsNil(err) {
		return err
	}

	if !rs.IsSucceed() {
		return fmt.Errorf("Nebula execute failed, code: %d message: %s ", rs.GetErrorCode(), rs.GetErrorMsg())
	}
	return nil
}
