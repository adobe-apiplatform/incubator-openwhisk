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
package executionprovider

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
)

const (
	KNative = iota
	OpenWhisk
)

func CleanConfigString(in string) ([]byte, string, error) {
	var configBytes []byte
	var pathUsed = ""
	cleanedString := strings.TrimPrefix(in, "\"")
	cleanedString = strings.TrimSuffix(cleanedString, "\"")
	cleanedString = strings.TrimSpace(cleanedString)
	if strings.TrimSpace(cleanedString)[0:1] == "{" {
		configBytes = []byte(cleanedString)
	} else {
		pathUsed = cleanedString
		configBytes, err := ioutil.ReadFile(cleanedString)
		if err != nil {
			return nil, pathUsed, err
		}
		return configBytes, pathUsed, nil
	}
	if !json.Valid(configBytes) {
		return nil, pathUsed, errors.New(fmt.Sprintf("Passed in config is not valid JSON %s", in))
	}
	return configBytes, pathUsed, nil
}
