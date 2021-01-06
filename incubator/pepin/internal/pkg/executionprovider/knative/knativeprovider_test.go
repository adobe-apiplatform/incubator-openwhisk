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

package knative

import (
	"fmt"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/pool"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
)

var dir string

func init() {
	var err error
	dir, err = os.Getwd()
	if err != nil {
		fmt.Printf("Could not find resource directory: %s", err)
	}
	for notFound := true; notFound; {
		if dir == "/" {
			fmt.Printf("Could not find resource directory.")
			break
		}
		if _, err := os.Stat(dir + "/test-resources"); os.IsNotExist(err) {
			dir = path.Clean(dir + "/..")
		} else {
			break
		}
	}
}

func TestKnativeConstructorErrors(t *testing.T) {
	var ep pool.ExecutionProvider
	var err error

	ep, err = New("routerID1", "")
	assert.Nil(t, ep, "Knative execution provider should not be created.")
	assert.NotNil(t, err, "Knative execution provider should be created.")

	ep, err = New("routerID1", "{bad-json")
	assert.Nil(t, ep, "Knative execution provider should not be created.")
	assert.NotNil(t, err, "Knative execution provider should be created.")

	ep, err = New("routerID1", dir+"/test-resources/ep-knative/missing file.txt")
	assert.Nil(t, ep, "Knative execution provider should not be created.")
	assert.NotNil(t, err, "Knative execution provider should be created.")

	ep, err = New("routerID1", dir+"/test-resources/ep-knative/bad-json.json")
	assert.Nil(t, ep, "Knative execution provider should not be created.")
	assert.NotNil(t, err, "Knative execution provider should be created.")

}

func TestKnativeConstructor(t *testing.T) {
	ep, err := New("routerID1", dir+"/test-resources/ep-knative/good-json.json")
	assert.NotNil(t, ep, "Knative execution provider should be created.")
	assert.Nil(t, err, "Knative execution provider should be created with no errors")
}
