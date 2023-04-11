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

package mapping

import (
	"github.com/xfali/lean/resultset"
	"github.com/xfali/reflection"
	"reflect"
)

const (
	FieldAliasTagName = "column"
)

var (
	InterfaceType = reflect.TypeOf((*interface{})(nil)).Elem()
)

func ScanRows(dst interface{}, result resultset.QueryResult) (int64, error) {
	return ScanRows2Value(reflect.ValueOf(dst), result)
}

func ScanRows2Value(dst reflect.Value, result resultset.QueryResult) (int64, error) {
	columns, err := result.Columns()
	if err != nil {
		return 0, err
	}

	values := make([]interface{}, len(columns))
	scanVs := make([]interface{}, len(columns))
	for i := range values {
		scanVs[i] = &values[i]
	}

	var count int64 = 0
	for result.Next() {
		err = result.Scan(scanVs...)
		if err != nil {
			return count, err
		}
		if !deserialize(dst, columns, values) {
			break
		}
		count++
	}

	return count, nil
}

func deserialize(dst reflect.Value, columns []string, values []interface{}) bool {
	vv := make([]reflect.Value, len(values))
	for i, v := range values {
		vv[i] = reflect.ValueOf(v)
	}
	return deserializeValue(dst, columns, vv)
}

func deserializeValue(dst reflect.Value, columns []string, values []reflect.Value) bool {
	rv := dst
	rt := rv.Type()
	if rt.Kind() == reflect.Slice {
		rv = rv.Elem()
	}
	for i := range columns {
		switch rv.Kind() {
		case reflect.Map:
			rv.SetMapIndex(reflect.ValueOf(columns[i]), values[i])
		case reflect.Struct:
			tt := rv.Type()
			s := tt.NumField()
			for j := 0; j < s; j++ {
				ft := tt.Field(j)
				name := ft.Name
				if tn, ok := ft.Tag.Lookup(FieldAliasTagName); ok {
					name = tn
				}
				if name == columns[i] {
					fv := rv.Field(j)
					fv.Set(values[i])
					break
				}
			}
		default:
			reflection.SetValue(rv, values[0])
			break
		}
	}
	if rt.Kind() == reflect.Slice {
		dst.Set(reflect.Append(dst, rv))
		return true
	}
	return false
}

func setValue(dst, v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Map:
		kvs := v.MapKeys()
		columns := make([]string, len(kvs))
		values := make([]reflect.Value, len(kvs))
		for i, k := range kvs {
			if k.Kind() == reflect.String {
				columns[i] = k.Interface().(string)
				values[i] = v.MapIndex(k)
			}
		}
		return deserializeValue(dst, columns, values)
	case reflect.Struct:
		tt := v.Type()
		s := tt.NumField()
		columns := make([]string, s)
		values := make([]reflect.Value, s)
		for i := 0; i < s; i++ {
			ft := tt.Field(i)
			name := ft.Name
			if tn, ok := ft.Tag.Lookup(FieldAliasTagName); ok {
				name = tn
			}
			columns[i] = name
			values[i] = v.Field(i)
		}
		return deserializeValue(dst, columns, values)
	default:
		reflection.SetValue(dst, v)
		return false
	}
}
