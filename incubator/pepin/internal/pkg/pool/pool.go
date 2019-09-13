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

package pool

import (
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/datastore"
	"github.com/valyala/fasthttp"
	"net/http"
	"time"
)
import "net/url"

type Resource struct {
	Provider    int
	Name        string
	Url         url.URL
	ExternalUrl url.URL
	Kind        string
}

type ExecutionProvider interface {
	GetId() int
	DispatchRequest(action *Resource, upstreamRequest *http.Request) (*fasthttp.Response, error)
	GetConfiguredStemCellCount() int
	AllocateStemCells([]datastore.ActionMetaData) ([]Resource, error)
	GetResources() ([]Resource, error)
	Deallocate(name string) error
	CanHandle(kind string) bool
}

type Action struct {
	Resource
	ProvidedBy *ExecutionProvider
	datastore.ActionMetaData
}

type Pool interface {
	Close()
	//GC(routerId string)
	// New
	Warm() error
	RegisterExecutionProvider(provider ExecutionProvider)
	Activate(metadata datastore.ActionMetaData, upstreamRequest *http.Request) (<-chan *fasthttp.Response, <-chan error)
}

const (
	LocalStorage = iota
	EtcdStorage
)

type PoolStorageConfig struct {
	Kind               int
	ServiceIdleTimeOut time.Duration
}
