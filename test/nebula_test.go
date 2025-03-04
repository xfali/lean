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
	"encoding/json"
	nebula "github.com/vesoft-inc/nebula-go/v3"
	"github.com/xfali/lean/drivers/nebuladrv"
	"github.com/xfali/lean/mapping"
	"github.com/xfali/lean/session"
	"testing"
	"time"
)

var (
	TestHost = "127.0.0.1"
	TestPort = 9669
)

func RunWithSession(f func(sess session.Session) error) error {
	//pool, err := nebuladrv.NebulaConnPoolCreator([]nebula.HostAddress{
	//	{
	//		Host: TestHost,
	//		Port: TestPort,
	//	},
	//}, nebula.GetDefaultConf())(logger.GetLogger())
	//if err != nil {
	//	return err
	//}
	conn := nebuladrv.NewNebulaConnection(
		nebuladrv.ConnOpts.WithUserInfo("root", "test"),
		nebuladrv.ConnOpts.AddAddress(TestHost, TestPort),
		nebuladrv.ConnOpts.SetConnectConfig(nebula.GetDefaultConf()),
	)
	err := conn.Open()
	if err != nil {
		return err
	}
	defer conn.Close()
	sess, err := conn.GetSession()
	if err != nil {
		return err
	}
	defer sess.Close()
	return f(sess)
}

func TestNebulaTag(t *testing.T) {
	err := RunWithSession(func(sess session.Session) error {
		ctx := context.Background()
		_, err := sess.Execute(ctx, "USE entities")
		if err != nil {
			t.Fatal(err)
		}
		ret, err := sess.Query(ctx, "MATCH (v:Entity) RETURN v.Entity.name as name LIMIT 3")
		if err != nil {
			t.Fatal(err)
		}
		defer ret.Close()
		var v []string
		_, err = mapping.ScanRows(&v, ret)
		if err != nil {
			t.Fatal(err)
		}

		s, _ := json.MarshalIndent(v, "", "	")
		t.Log(string(s))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNebulaProperties(t *testing.T) {
	t.Run("one", func(t *testing.T) {
		err := RunWithSession(func(sess session.Session) error {
			ctx := context.Background()
			_, err := sess.Execute(ctx, "USE entities")
			if err != nil {
				t.Fatal(err)
			}
			ret, err := sess.Query(ctx, "MATCH (v) RETURN properties(v) as vp LIMIT 3")
			if err != nil {
				t.Fatal(err)
			}
			defer ret.Close()
			v := struct {
				R map[string]interface{} `column:"vp"`
			}{}
			_, err = mapping.ScanRows(&v, ret)
			if err != nil {
				t.Fatal(err)
			}

			s, _ := json.MarshalIndent(v, "", "	")
			t.Log(string(s))
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("slice", func(t *testing.T) {
		err := RunWithSession(func(sess session.Session) error {
			ctx := context.Background()
			_, err := sess.Execute(ctx, "USE entities")
			if err != nil {
				t.Fatal(err)
			}
			ret, err := sess.Query(ctx, "MATCH (v) RETURN properties(v) as vp LIMIT 3")
			if err != nil {
				t.Fatal(err)
			}
			defer ret.Close()
			var v []struct {
				R map[string]string `column:"vp"`
			}
			_, err = mapping.ScanRows(&v, ret)
			if err != nil {
				t.Fatal(err)
			}

			for _, o := range v {
				s, _ := json.MarshalIndent(o, "", "	")
				t.Log(string(s))
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("slice struct", func(t *testing.T) {
		err := RunWithSession(func(sess session.Session) error {
			ctx := context.Background()
			_, err := sess.Execute(ctx, "USE entities")
			if err != nil {
				t.Fatal(err)
			}
			ret, err := sess.Query(ctx, "MATCH (v) RETURN properties(v) as vp LIMIT 3")
			if err != nil {
				t.Fatal(err)
			}
			defer ret.Close()
			var v []struct {
				S struct {
					Name       string    `column:"name"`
					UpdateTime time.Time `column:"update_time"`
				} `column:"vp"`
			}
			_, err = mapping.ScanRows(&v, ret)
			if err != nil {
				t.Fatal(err)
			}

			for _, o := range v {
				s, _ := json.MarshalIndent(o, "", "	")
				t.Log(string(s))
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}
