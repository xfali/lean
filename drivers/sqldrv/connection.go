/*
 * Copyright (C) 2023-2025, Xiongfa Li.
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

package sqldrv

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/xfali/lean/session"
)

type ConnOpt func(*sqlConnection)

type sqlConnection struct {
	db             *sql.DB
	driverName     string
	dataSourceName string

	sessOpts []SessionOpt
}

func NewSqlConnection(driverName, dataSourceName string, opts ...ConnOpt) *sqlConnection {
	ret := &sqlConnection{
		driverName:     driverName,
		dataSourceName: dataSourceName,
	}

	for _, opt := range opts {
		opt(ret)
	}

	return ret
}

func (c *sqlConnection) Open() error {
	db, err := sql.Open(c.driverName, c.dataSourceName)
	if err != nil {
		return fmt.Errorf("Open %s failed: %v ", c.driverName, err)
	}
	c.db = db
	return nil
}

func (c *sqlConnection) GetSession() (session.Session, error) {
	if c.db == nil {
		return nil, errors.New("Connection not opened ")
	}
	return NewSqlSession(c.db, c.sessOpts...), nil
}

func (c *sqlConnection) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

type connOpts struct {
}

var ConnOpts connOpts

func (o connOpts) SetCreateSessionOpts(opts ...SessionOpt) ConnOpt {
	return func(connection *sqlConnection) {
		connection.sessOpts = opts
	}
}
