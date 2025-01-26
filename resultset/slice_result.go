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

package resultset

import (
	"fmt"
	"github.com/xfali/reflection"
	"reflect"
)

type ValueSetter func(d interface{}, columns []string, dest []interface{}) error

type SliceResult[T any] struct {
	data    []T
	index   int
	columns []string

	setter ValueSetter
}

func NewSliceResult[T any](data []T, columns []string, setter ValueSetter) *SliceResult[T] {
	ret := &SliceResult[T]{
		data:    data,
		columns: columns,
		setter:  setter,
	}
	return ret
}

func (r *SliceResult[T]) Columns() ([]string, error) {
	return r.columns, nil
}

func (r *SliceResult[T]) Next() bool {
	return r.index != len(r.data)
}

func (r *SliceResult[T]) Scan(dest ...interface{}) error {
	if len(r.data) == 0 {
		return nil
	}

	v := r.data[r.index]
	err := r.setter(v, r.columns, dest)
	r.index++
	return err
}

func (r *SliceResult[T]) Close() error {
	return nil
}

func (r *SliceResult[T]) LastInsertId() (int64, error) {
	return int64(len(r.data) - 1), nil
}

func (r *SliceResult[T]) RowsAffected() (int64, error) {
	return 0, nil
}

type SturctSetter struct {
	tag string
}

func NewSturctSetter(tagName string) *SturctSetter {
	return &SturctSetter{tag: tagName}
}

func (s *SturctSetter) Set(d interface{}, columns []string, dest []interface{}) error {
	rv := reflect.ValueOf(d)
	rt := rv.Type()

	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
		rv = rv.Elem()
	}

	if rt.Kind() != reflect.Struct {
		return fmt.Errorf("Expect struct but get %s ", rt.String())
	}

	for i := range columns {
		num := rt.NumField()
		for j := 0; j < num; j++ {
			ft := rt.Field(j)
			name := ft.Name
			if tn, ok := ft.Tag.Lookup(s.tag); ok {
				name = tn
			}
			if name == columns[i] {
				dst := dest[i]
				dv := reflect.ValueOf(dst)
				if dv.Kind() == reflect.Ptr {
					dv = dv.Elem()
				}
				if ft.Type.AssignableTo(dv.Type()) {
					if dv.CanSet() {
						dv.Set(rv.Field(j))
					}
				}
				break
			}
		}
	}
	return nil
}

func InterfaceSetter(d interface{}, columns []string, dest []interface{}) error {
	if src, ok := d.([]interface{}); ok {
		if len(src) != len(dest) {
			return fmt.Errorf("Length are inconsistent: src %d dest %d ", len(src), len(dest))
		}

		for i := range src {
			_ = reflection.SetValueInterface(dest[i], src[i])
		}
		return nil
	}
	return fmt.Errorf("dest type expect []interface{} but get %s ", reflect.TypeOf(d).String())
}
