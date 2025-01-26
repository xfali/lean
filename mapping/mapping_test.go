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
	"encoding/json"
	"github.com/xfali/lean/resultset"
	"testing"
	"time"
)

type srcData struct {
	Id         int64     `column:"id"`
	Name       string    `column:"name"`
	Score      float64   `column:"score"`
	CreateTime time.Time `column:"create_time"`
	UpdateTime []byte    `column:"update_time"`
}

type dstData struct {
	Id         int64     `column:"id"`
	Name       string    `column:"name"`
	Score      float64   `column:"score"`
	CreateTime time.Time `column:"create_time"`
	UpdateTime time.Time `column:"update_time"`
}

func TestMapping(t *testing.T) {
	now := time.Now()
	r := resultset.NewSliceResult[srcData]([]srcData{
		{
			Id:         1,
			Name:       "hello",
			Score:      100.0,
			CreateTime: now.Add(-2 * time.Hour),
			UpdateTime: []byte(now.Format(time.RFC3339)),
		},
		{
			Id:         2,
			Name:       "world",
			Score:      50.0,
			CreateTime: now.Add(-3 * time.Hour),
			UpdateTime: []byte(now.Format(time.RFC3339)),
		},
	}, []string{"id", "name", "score", "create_time", "update_time"}, resultset.NewSturctSetter(FieldAliasTagName).Set)
	defer r.Close()

	t.Run("slice", func(t *testing.T) {
		var ret []dstData
		_, err := ScanRows(&ret, r)
		if err != nil {
			t.Fatal(err)
		}

		for _, v := range ret {
			d, _ := json.MarshalIndent(v, "", "    ")
			t.Log(string(d))
		}
	})

	t.Run("single", func(t *testing.T) {
		var ret dstData
		_, err := ScanRows(&ret, r)
		if err != nil {
			t.Fatal(err)
		}
		d, _ := json.MarshalIndent(ret, "", "    ")
		t.Log(string(d))
	})
}
