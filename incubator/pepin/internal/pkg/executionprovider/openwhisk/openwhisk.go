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

package openwhisk

import (
	"encoding/json"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/datastore"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/executionprovider"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/pool"
	"github.com/valyala/fasthttp"
	"k8s.io/apimachinery/pkg/util/uuid"
	"net/http"
	"net/url"
)

func New(routerid string, configuration string) (pool.ExecutionProvider, error) {
	var config epConfig
	configBytes, _, err := executionprovider.CleanConfigString(configuration)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		return nil, err
	}
	return &executionProvider{config: config}, nil
}

type epConfig struct {
	ControllerUrl             string `json:"controllerurl"`
	GarbageCollectionDuration string `json:"garbagecollectionduration"`
}

type executionProvider struct {
	config epConfig
}

func (ep *executionProvider) CanHandle(kind string) bool {
	return kind == "nodejs-ow"
}

func (ep *executionProvider) GetId() int {
	return executionprovider.OpenWhisk
}

func (ep *executionProvider) DispatchRequest(action *pool.Resource, upstreamRequest *http.Request) (*fasthttp.Response, error) {
	resp := fasthttp.AcquireResponse()
	resp.SetStatusCode(504)
	resp.Header.Add("X-OW-Target-Url", action.Url.String())
	return resp, nil
}

func (ep *executionProvider) Deallocate(name string) error {
	return nil
}

func (ep *executionProvider) GetConfiguredStemCellCount() int {
	return 0
}

func (ep *executionProvider) AllocateStemCells(stems []datastore.ActionMetaData) ([]pool.Resource, error) {
	resources := make([]pool.Resource, len(stems))
	for i, meta := range stems {
		resources[i] = pool.Resource{
			Provider: ep.GetId(),
			Name:     string(uuid.NewUUID()),
			Url: url.URL{
				Scheme: "https",
				Host:   "controller.host.com",
				Path:   "/" + meta.Namespace + "/" + meta.Name,
			},
			ExternalUrl: url.URL{
				Scheme: "https",
				Host:   "controller.host.com",
				Path:   "/" + meta.Namespace + "/" + meta.Name,
			},
			Kind: "nodejs-ow",
		}
	}
	return resources, nil
}

func (ep *executionProvider) GetResources() ([]pool.Resource, error) {
	return []pool.Resource{}, nil
}

func (ep *executionProvider) Warm() error {
	return nil
}
