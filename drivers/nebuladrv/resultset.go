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

package nebuladrv

import (
	"errors"
	"fmt"
	nebula "github.com/vesoft-inc/nebula-go/v3"
	"github.com/xfali/reflection"
	"reflect"
	"time"
)

type nebulaResultSet struct {
	rs    *nebula.ResultSet
	index int
}

func NewNebulaResultSet(rs *nebula.ResultSet) *nebulaResultSet {
	ret := &nebulaResultSet{
		rs:    rs,
		index: 0,
	}
	return ret
}

func (r *nebulaResultSet) Columns() ([]string, error) {
	return r.rs.GetColNames(), nil
}

func (r *nebulaResultSet) Next() bool {
	return r.index != r.rs.GetRowSize()
}

func (r *nebulaResultSet) Scan(dest ...interface{}) error {
	values, err := r.rs.GetRowValuesByIndex(r.index)
	if err != nil {
		return err
	}
	for i := 0; i < r.rs.GetColSize(); i++ {
		v, err := values.GetValueByIndex(i)
		if err != nil {
			return err
		}
		if err := set2Value(dest[i], v); err != nil {
			return err
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

func set2Value(dest interface{}, value *nebula.ValueWrapper) error {
	if dst, ok := dest.(*interface{}); ok {
		if value.IsNull() {
			return nil
		} else if value.IsBool() {
			*dst, _ = value.AsBool()
			return nil
		} else if value.IsInt() {
			*dst, _ = value.AsInt()
			return nil
		} else if value.IsFloat() {
			*dst, _ = value.AsFloat()
			return nil
		} else if value.IsString() {
			*dst, _ = value.AsString()
			return nil
		} else if value.IsDate() {
			d, _ := value.AsDate()
			t, err := time.Parse("2006-01-02", fmt.Sprintf("%04d-%02d-%02d", d.GetYear(), d.GetMonth(), d.GetDay()))
			if err != nil {
				return err
			}
			*dst = t
			return nil
		} else if value.IsTime() {
			return errors.New("Not support nebula value type [time] ")
		} else if value.IsDateTime() {
			dt, _ := value.AsDateTime()
			d, _ := dt.GetLocalDateTimeWithTimezoneName("UTC")
			t, err := time.ParseInLocation("2006-01-02 15:04:05", fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", d.GetYear(), d.GetMonth(), d.GetDay(), d.GetHour(), d.GetMinute(), d.GetSec()), time.UTC)
			if err != nil {
				return err
			}
			*dst = t
			return nil
		} else if value.IsVertex() {
			return errors.New("Not support nebula value type [vertex] ")
		} else if value.IsEdge() {
			return errors.New("Not support nebula value type [edge] ")
		} else if value.IsPath() {
			return errors.New("Not support nebula value type [path] ")
		} else if value.IsList() {
			l, _ := value.AsList()
			vv := make([]interface{}, len(l))
			for i := range l {
				err := set2Value(&vv[i], &l[i])
				if err != nil {
					return err
				}
			}
			*dst = vv
			return nil
		} else if value.IsMap() {
			l, _ := value.AsMap()
			vv := make(map[string]interface{}, len(l))
			for k, v := range l {
				var o interface{}
				err := set2Value(&o, &v)
				if err != nil {
					return err
				}
				vv[k] = o
			}
			*dst = vv
			return nil
		} else if value.IsSet() {
			return errors.New("Not support nebula value type [set] ")
		} else if value.IsGeography() {
			return errors.New("Not support nebula value type [geography] ")
		} else if value.IsDuration() {
			return errors.New("Not support nebula value type [duration] ")
		}
	} else {
		return fmt.Errorf("Only support Dest type *interface{} but get [%s] ", reflect.TypeOf(dest).String())
	}

	return nil
}

func CheckResultSet(rs *nebula.ResultSet, err error) error {
	if !reflection.IsNil(err) {
		return err
	}

	if !rs.IsSucceed() {
		return fmt.Errorf("Nebula execute failed, code: %d message: %s ", rs.GetErrorCode(), rs.GetErrorMsg())
	}
	return nil
}
