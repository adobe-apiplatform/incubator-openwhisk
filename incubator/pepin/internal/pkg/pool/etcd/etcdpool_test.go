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

package etcd

import (
	"context"
	"errors"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/datastore"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/pool"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func init() {
	logger, _ = pkg.GetLogger(false)
}

type kvmocktype struct {
	data      map[string]string
	thenQueue []clientv3.Op
	ifQueue   []clientv3.Cmp
	elseQueue []clientv3.Op
}

type mockEPType struct {
	services map[string]int
}

var prefixFunction = reflect.ValueOf(clientv3.WithPrefix())
var countOnlyFunction = reflect.ValueOf(clientv3.WithCountOnly())

func TestLocalServicePool_Activate(t *testing.T) {

}

func TestGcOneService(t *testing.T) {
	var err error

	etcdstate := make(map[string]string)
	mockEP := mockEPType{
		services: make(map[string]int),
	}
	mockEP.services["serviceName"] = 0
	kvMock := kvmocktype{data: etcdstate}

	etcdstate[etcdActionPrefix+"/ns1/action1"] = serializeResourceData(pool.Resource{
		Provider:    99,
		Name:        "serviceName",
		Url:         url.URL{},
		ExternalUrl: url.URL{},
		Kind:        "",
	})

	etcdstate[etcdResourcePrefix+"/99/serviceName"] = ETCD_KEY_PREFIX + "/actions/ns1/action1"

	etcdclient := clientv3.Client{
		KV: kvMock,
	}
	eps := make(map[int]pool.ExecutionProvider)
	eps[mockEP.GetId()] = &mockEP
	err = gc(&etcdclient, eps)
	assert.Empty(t, etcdstate, "There should be no more data in the etcd map.")
	assert.Empty(t, mockEP.services, "There should be no more services managed by the execution provider.")
	assert.Nil(t, err, "owies.")
}

func TestNoGCForService(t *testing.T) {
	var err error

	etcdstate := make(map[string]string)
	mockEP := mockEPType{
		services: make(map[string]int),
	}
	mockEP.services["serviceName"] = 0
	kvMock := kvmocktype{data: etcdstate}

	etcdstate[etcdActionPrefix+"/ns1/action1"] = serializeResourceData(pool.Resource{
		Provider:    99,
		Name:        "serviceName",
		Url:         url.URL{},
		ExternalUrl: url.URL{},
		Kind:        "",
	})

	etcdstate[etcdRouterPrefix+etcdActionPrefix+"/ns1/action1/mockRouter"] = "111111111"

	etcdstate[etcdResourcePrefix+"/99/serviceName"] = ETCD_KEY_PREFIX + "/actions/ns1/action1"

	etcdclient := clientv3.Client{
		KV: kvMock,
	}
	eps := make(map[int]pool.ExecutionProvider)
	eps[mockEP.GetId()] = &mockEP

	err = gc(&etcdclient, eps)
	assert.NotEmpty(t, etcdstate, "There should be data in the etcd map.")
	assert.NotEmpty(t, mockEP.services, "There should be one service managed by the execution provider.")
	assert.Equal(t, 1, len(mockEP.services), "The service should not be deallcoated")
	assert.Nil(t, err, "owies.")
}

func (c kvmocktype) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return nil, errors.New("WHAT?")
}

func (c kvmocktype) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	withPrefix := false
	//countOnly := false
	for _, o := range opts {
		if reflect.ValueOf(o) == prefixFunction {
			withPrefix = true
		}
		//if reflect.ValueOf(o) == countOnlyFunction {
		//	countOnly = true
		//}
	}
	kvs, err := c.get(key, withPrefix)
	if err != nil {
		return nil, err
	}
	return &clientv3.GetResponse{
		Kvs:   kvs,
		Count: int64(len(kvs)),
	}, nil
}

func (c kvmocktype) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	delete(c.data, key)
	return &clientv3.DeleteResponse{
		Deleted: 1,
	}, nil
}

func (c kvmocktype) Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return nil, errors.New("WHAT?")

}

func (c kvmocktype) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	return clientv3.OpResponse{}, errors.New("WHAT?")
}

func (c kvmocktype) Txn(ctx context.Context) clientv3.Txn {
	return c
}
func (c kvmocktype) get(key string, prefix bool) ([]*mvccpb.KeyValue, error) {
	if !prefix {
		d, ok := c.data[key]
		if !ok {
			return nil, errors.New("could not find key")
		}
		kv := &mvccpb.KeyValue{
			Key:   []byte(key),
			Value: []byte(d),
		}
		return []*mvccpb.KeyValue{kv}, nil
	}
	var matches []*mvccpb.KeyValue

	for k, v := range c.data {

		if strings.HasPrefix(k, key) {
			matches = append(matches, &mvccpb.KeyValue{
				Key:   []byte(k),
				Value: []byte(v),
			})
		}
	}
	return matches, nil
}

func (c kvmocktype) If(cs ...clientv3.Cmp) clientv3.Txn {
	c.ifQueue = append(c.ifQueue, cs...)
	return c
}
func (c kvmocktype) Then(ops ...clientv3.Op) clientv3.Txn {
	c.thenQueue = append(c.thenQueue, ops...)
	return c
}

func (c kvmocktype) Else(ops ...clientv3.Op) clientv3.Txn {
	c.elseQueue = append(c.elseQueue, ops...)
	return c
}

func (c kvmocktype) Commit() (*clientv3.TxnResponse, error) {
	for _, op := range c.thenQueue {
		if op.IsDelete() {
			_, err := c.Delete(nil, string(op.KeyBytes()), nil)
			if err != nil {
				return nil, err
			}
		}
	}
	return &clientv3.TxnResponse{
		Succeeded: true,
	}, nil
}

func (ep *mockEPType) GetId() int {
	return 99
}
func (ep *mockEPType) DispatchRequest(action *pool.Resource, upstreamRequest *http.Request) (*fasthttp.Response, error) {
	return nil, nil
}
func (ep *mockEPType) GetConfiguredStemCellCount() int {
	return 1
}
func (ep *mockEPType) AllocateStemCells([]datastore.ActionMetaData) ([]pool.Resource, error) {
	return nil, nil
}
func (ep *mockEPType) GetResources() ([]pool.Resource, error) {
	return nil, nil
}
func (ep *mockEPType) Deallocate(name string) error {
	delete(ep.services, name)
	return nil
}
