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

package datastore

import (
	"errors"
	"fmt"
)

type ActionMetaData struct {
	Name      string
	Namespace string
	Kind      string
}

type Datastore interface {
	InitStore() error
	GetAction(namespace string, name string) (ActionMetaData, error)
}

type MemoryDatastore struct {
	actions map[string]ActionMetaData
}

func (m *MemoryDatastore) InitStore() error {
	m.actions = make(map[string]ActionMetaData)
	//TODO: init from json
	m.actions["ns1/action1"] = ActionMetaData{"action1", "ns1", "nodejs-10"}
	m.actions["ns1/action2"] = ActionMetaData{"action2", "ns1", "nodejs-10"}
	m.actions["ns1/action3"] = ActionMetaData{"action3", "ns1", "nodejs-10"}
	m.actions["ns2/action1"] = ActionMetaData{"action1", "ns2", "nodejs-ow"}
	return nil
}
func (m *MemoryDatastore) GetAction(namespace string, name string) (ActionMetaData, error) {
	key := fmt.Sprintf("%s/%s", namespace, name)
	if val, ok := m.actions[key]; ok {
		return val, nil
	} else {
		return ActionMetaData{}, errors.New(fmt.Sprintf("Action %s not found", key))
	}
}
