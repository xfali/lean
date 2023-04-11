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
	nebula_go "github.com/vesoft-inc/nebula-go/v3"
	"github.com/xfali/xlog"
)

type logger struct {
	log xlog.Logger
	nebula_go.Logger
}

func (l *logger) Info(msg string) {
	l.log.Infoln(msg)
}

func (l *logger) Warn(msg string) {
	l.log.Warnln(msg)
}

func (l *logger) Error(msg string) {
	l.log.Errorln(msg)
}

func (l *logger) Fatal(msg string) {
	l.log.Fatalln(msg)
}
