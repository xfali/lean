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
	"crypto/tls"
	"errors"
	"fmt"
	nebula "github.com/vesoft-inc/nebula-go/v3"
	"github.com/xfali/lean/session"
	"github.com/xfali/xlog"
)

type nebulaConnection struct {
	pool      *nebula.ConnectionPool
	addresses []nebula.HostAddress
	conf      nebula.PoolConfig
	sslConfig *tls.Config
	username  string
	password  string
}

type ConnectionOpt func(*nebulaConnection)

func NewNebulaConnection(opts ...ConnectionOpt) *nebulaConnection {
	ret := &nebulaConnection{}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func (c *nebulaConnection) Open() error {
	if c.username == "" {
		return errors.New("Nebula username is empty ")
	}

	if c.password == "" {
		return errors.New("Nebula password is empty ")
	}

	if c.pool == nil {
		p, err := nebula.NewSslConnectionPool(c.addresses, c.conf, c.sslConfig, &logger{
			log: xlog.GetLogger(),
		})
		if err != nil {
			return fmt.Errorf("Nebula connect init failed: %v ", err)
		}
		c.pool = p
	}

	return nil
}

func (c *nebulaConnection) GetSession() (session.Session, error) {
	if c.pool == nil {
		return nil, errors.New("Connection must be open before get session. ")
	}
	sess, err := c.pool.GetSession(c.username, c.password)
	if err != nil {
		return nil, fmt.Errorf("Get nebula session failed: %v ", err)
	}
	return NewNebulaSession(sess), nil
}

func (c *nebulaConnection) Close() error {
	if c.pool != nil {
		c.pool.Close()
	}
	return nil
}

type connOpts struct{}

var ConnOpts connOpts

func (connOpts) WithConnectionPool(pool *nebula.ConnectionPool) ConnectionOpt {
	return func(connection *nebulaConnection) {
		connection.pool = pool
	}
}

func (connOpts) WithUserInfo(username, password string) ConnectionOpt {
	return func(connection *nebulaConnection) {
		connection.username = username
		connection.password = password
	}
}

func (connOpts) SetAddresses(addresses []nebula.HostAddress) ConnectionOpt {
	return func(connection *nebulaConnection) {
		connection.addresses = addresses
	}
}

func (connOpts) AddAddresses(addresses ...nebula.HostAddress) ConnectionOpt {
	return func(connection *nebulaConnection) {
		connection.addresses = append(connection.addresses, addresses...)
	}
}

func (connOpts) AddAddress(host string, port int) ConnectionOpt {
	return func(connection *nebulaConnection) {
		connection.addresses = append(connection.addresses, nebula.HostAddress{
			Host: host,
			Port: port,
		})
	}
}

func (connOpts) SetConnectConfig(conf nebula.PoolConfig) ConnectionOpt {
	return func(connection *nebulaConnection) {
		connection.conf = conf
	}
}

func (connOpts) SetSslConfig(sslConfig *tls.Config) ConnectionOpt {
	return func(connection *nebulaConnection) {
		connection.sslConfig = sslConfig
	}
}

func NebulaConnPoolCreator(addresses []nebula.HostAddress, conf nebula.PoolConfig) func(logger xlog.Logger) (*nebula.ConnectionPool, error) {
	return func(log xlog.Logger) (*nebula.ConnectionPool, error) {
		return nebula.NewConnectionPool(addresses, conf, &logger{
			log: log,
		})
	}
}
