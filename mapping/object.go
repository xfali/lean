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

package mapping

import (
	"github.com/xfali/lean/resultset"
	"github.com/xfali/reflection"
	"reflect"
)

var (
	FieldAliasTagName = "column"
	SliceDummyColumn  = ""
)

var (
	InterfaceType = reflect.TypeOf((*interface{})(nil)).Elem()
)

func ScanRows(dst interface{}, result resultset.QueryResult) (int64, error) {
	return ScanRows2Value(reflect.ValueOf(dst), result)
}

func ScanRows2Value(dst reflect.Value, result resultset.QueryResult) (int64, error) {
	dst = reflect.Indirect(dst)
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
		count++
		if !deserialize(dst, columns, values) {
			break
		}
	}

	return count, nil
}

func deserialize(dst reflect.Value, columns []string, values []interface{}) bool {
	vv := make([]reflect.Value, len(values))
	for i, v := range values {
		vv[i] = reflect.ValueOf(v)
	}
	rv := dst
	rt := rv.Type()
	if BinaryType.AssignableTo(rt) {
		return deserializeValue(rv, columns, vv)
	} else {
		if rt.Kind() == reflect.Slice {
			et := rt.Elem()
			rv = reflect.New(et).Elem()
		}
		next := deserializeValue(rv, columns, vv)
		if rt.Kind() == reflect.Slice {
			dst.Set(reflect.Append(dst, rv))
			return true
		}
		return next
	}
}

func deserializeValue(rv reflect.Value, columns []string, values []reflect.Value) bool {
	rt := rv.Type()
	for i := range columns {
		if !values[i].IsValid() {
			continue
		}
		switch rv.Kind() {
		case reflect.Map:
			if rv.IsNil() {
				rv.Set(reflect.MakeMap(rt))
			}
			et := rt.Elem()
			gv := getValue(et, values[i])
			if gv.IsValid() {
				if gv.Type().AssignableTo(et) {
					rv.SetMapIndex(reflect.ValueOf(columns[i]), gv)
				}
			}
		case reflect.Slice:
			if BinaryType.AssignableTo(rt) {
				_ = reflection.SetValue(rv, values[0])
				return false
			} else {
				gv := getValue(rt.Elem(), values[i])
				if gv.IsValid() {
					rv.Set(gv)
				}
			}
		case reflect.Struct:
			if TimeType.AssignableTo(rt) {
				_ = reflection.SetValue(rv, values[0])
				return false
			} else {
				tt := rv.Type()
				s := tt.NumField()
				for j := 0; j < s; j++ {
					ff := tt.Field(j)
					name := ff.Name
					if tn, ok := ff.Tag.Lookup(FieldAliasTagName); ok {
						name = tn
					}
					if name == columns[i] {
						fv := rv.Field(j)
						ft := fv.Type()
						gv := getValue(ft, values[i])
						if gv.IsValid() {
							if gv.Type().AssignableTo(ft) {
								fv.Set(gv)
							}
						}
						break
					}
				}
			}
		default:
			_ = reflection.SetValue(rv, values[0])
			return false
		}
	}
	return false
}

func interfaceValue(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Interface {
		return reflect.ValueOf(v.Interface())
	}
	return v
}

func getValue(et reflect.Type, v reflect.Value) reflect.Value {
	if !v.IsValid() {
		return v
	}
	v = interfaceValue(v)
	vt := v.Type()
	if vt.AssignableTo(et) {
		return v
	} else {
		if !reflection.CheckValueNilSafe(v) {
			switch v.Kind() {
			case reflect.Map:
				kvs := v.MapKeys()
				columns := make([]string, len(kvs))
				values := make([]reflect.Value, len(kvs))
				for i, k := range kvs {
					if k.Kind() == reflect.String {
						columns[i] = k.Interface().(string)
						values[i] = interfaceValue(v.MapIndex(k))
					}
				}
				ret := reflect.New(et).Elem()
				_ = deserializeValue(ret, columns, values)
				return ret
			case reflect.Slice:
				if BinaryType.AssignableTo(vt) {
					vv := reflect.New(et).Elem()
					_ = reflection.SetValue(vv, v)
					return vv
				} else {
					ret := reflect.MakeSlice(reflect.SliceOf(et), 0, v.Len())
					for i := 0; i < v.Len(); i++ {
						retV := reflect.New(et).Elem()
						_ = deserializeValue(retV, []string{SliceDummyColumn}, []reflect.Value{v.Index(i)})
						ret.Set(reflect.Append(ret, retV))
					}
					return ret
				}
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
				ret := reflect.New(et).Elem()
				_ = deserializeValue(ret, columns, values)
				return ret
			default:
				vv := reflect.New(et).Elem()
				_ = reflection.SetValue(vv, v)
				return vv
			}
		} else {
			return reflect.New(et).Elem()
		}
	}
}
