/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pkg

import (
	"fmt"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var once sync.Once
var core zapcore.Core

func GetLogger(production bool) (logger *zap.SugaredLogger, err error) {
	once.Do(func() {
		atom := zap.NewAtomicLevel()
		if production {
			fmt.Println("Starting with production logging.")
			encoderCfg := zap.NewProductionEncoderConfig()
			atom.SetLevel(zapcore.WarnLevel)
			core = zapcore.NewCore(
				zapcore.NewJSONEncoder(encoderCfg),
				zapcore.Lock(os.Stdout),
				atom,
			)
		} else {
			fmt.Println("Starting with DEV logging.")
			encoderCfg := zap.NewDevelopmentEncoderConfig()
			atom.SetLevel(zapcore.DebugLevel)
			core = zapcore.NewCore(
				zapcore.NewJSONEncoder(encoderCfg),
				zapcore.Lock(os.Stdout),
				atom,
			)
		}
	})
	return zap.New(core).Sugar(), nil
}
