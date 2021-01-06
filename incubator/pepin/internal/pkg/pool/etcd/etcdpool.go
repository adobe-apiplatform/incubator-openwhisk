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
	"encoding/base64"
	"errors"
	"fmt"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/pool"
	"github.com/valyala/fasthttp"
	"go.etcd.io/etcd/clientv3/clientv3util"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/datastore"
	"github.com/coreos/etcd/clientv3"
	servingClient "github.com/knative/serving/pkg/client/clientset/versioned"
	"go.uber.org/zap"
)

var logger *zap.SugaredLogger

//TODO: once service is allocated and receiving traffic, we could reset the autoscaling.knative.dev/minScale to 0, and purge services who reach the 0 scale? Or leave the action mapped to this service indefinitely (but pods will scale to 0)?

const ETCD_KEY_PREFIX = "/ow-pepin"
const etcdRouterPrefix = ETCD_KEY_PREFIX + "/routers"
const etcdResourcePrefix = ETCD_KEY_PREFIX + "/resources"
const etcdActionPrefix = ETCD_KEY_PREFIX + "/actions"

type localServicePool struct {
	sync.RWMutex
	knownActions       map[string]pool.Action
	stemcells          []pool.Resource
	config             PoolConfig
	etcd               *clientv3.Client
	clientSet          *servingClient.Clientset
	routerId           string
	serviceTemplate    *template.Template
	executionProviders map[int]pool.ExecutionProvider
	gcTicker           *time.Ticker
}

func (s localServicePool) Close() {
	s.gcTicker.Stop()
	logger.Infof("closing etcd client...")
	s.etcd.Close()
}

type PoolConfig struct {
	pool.PoolStorageConfig
	EtcdURL    string
	GCDuration time.Duration
}

var (
	etcdDialTimeout    = 2 * time.Second
	etcdRequestTimeout = 10 * time.Second
)

func (s *localServicePool) addResourcesToStorage(resources []pool.Resource) error {
	s.Lock()
	defer s.Unlock()
	for _, aService := range resources {
		//exclude services that are already mapped in etcd
		ctx, _ := context.WithTimeout(context.Background(), etcdRequestTimeout)
		serviceKey := createResourceKey(aService)
		resp, err := s.etcd.Get(ctx, serviceKey)
		if err != nil {
			logger.Error("Could not access etcd", err)
			return err
		}
		//if no service ref in etcd, treat it as a stem cell
		if len(resp.Kvs) == 0 {
			s.stemcells = append(s.stemcells, aService)
			logger.Debugf("adding new stemcell %s size %d", aService.Name, len(s.stemcells))
		} else {
			// Could just skip this and load it lazily later I suppose.....
			knownActionKey := string(resp.Kvs[0].Value)
			resp, err := s.etcd.Get(ctx, string(resp.Kvs[0].Value))
			if err != nil {
				logger.Error("Could not access etcd", err)
				return err
			}
			if len(resp.Kvs) != 1 {
				logger.Warnf("Found live mapping for action, but could not read resource metadata")
				break
			}
			storedResource, err := deserializeResourceData(resp.Kvs[0].Value)
			if err != nil {
				logger.Errorf("Could not deserialize resource data: ", err)
				break
			}

			providerRef := s.executionProviders[storedResource.Provider]
			newAction := pool.Action{
				Resource:       storedResource,
				ProvidedBy:     &providerRef,
				ActionMetaData: datastore.ActionMetaData{},
			}
			s.knownActions[knownActionKey] = newAction
			logger.Debugf("Adding %s to known action list from etcd", knownActionKey)
		}
	}
	return nil
}

func createActionKey(metadata datastore.ActionMetaData) string {
	return etcdActionPrefix + "/" + metadata.Namespace + "/" + metadata.Name
}

func createRouterKey(metadata datastore.ActionMetaData, routerId string) string {
	return etcdRouterPrefix + createActionKey(metadata) + "/" + routerId
}

func createResourceKey(resource pool.Resource) string {
	return etcdResourcePrefix + "/" + strconv.Itoa(resource.Provider) + "/" + resource.Name
}

func serializeResourceData(resource pool.Resource) string {
	internalUrl := base64.StdEncoding.EncodeToString([]byte(resource.Url.String()))
	externalUrl := base64.StdEncoding.EncodeToString([]byte(resource.ExternalUrl.String()))
	kind := base64.StdEncoding.EncodeToString([]byte(resource.Kind))
	name := base64.StdEncoding.EncodeToString([]byte(resource.Name))
	return internalUrl + "|" + externalUrl + "|" + kind + "|" + name + "|" + strconv.Itoa(resource.Provider)
}

func decodeField(value string) (string, error) {
	ba, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(ba[:]), nil
}

func deserializeResourceData(data []byte) (pool.Resource, error) {
	dataValues := strings.Split(string(data), "|")
	rawUrl, err := decodeField(dataValues[0])
	if err != nil {
		return pool.Resource{}, errors.New(fmt.Sprintf("could not decode knownActions value, interalUrl: %s", err))
	}
	internalUrl, err := url.Parse(rawUrl)
	if err != nil {
		return pool.Resource{}, errors.New(fmt.Sprintf("could not parse url for, internalUrl: %s", err))
	}

	rawUrl, err = decodeField(dataValues[1])
	if err != nil {
		return pool.Resource{}, errors.New(fmt.Sprintf("could not decode knownActions value, externalUrl: %s", err))
	}

	externalUrl, err := url.Parse(rawUrl)
	if err != nil {
		return pool.Resource{}, errors.New(fmt.Sprintf("could not parse url for, externalUrl: %s", err))
	}

	kind, err := decodeField(dataValues[2])
	if err != nil {
		return pool.Resource{}, errors.New(fmt.Sprintf("could not decode knownActions value, kind: %s", err))
	}
	name, err := decodeField(dataValues[3])
	if err != nil {
		return pool.Resource{}, errors.New(fmt.Sprintf("could not decode knownActions value, name: %s", err))
	}
	providerId, _ := strconv.Atoi(dataValues[4])

	return pool.Resource{
		Provider:    providerId,
		Name:        name,
		Url:         *internalUrl,
		ExternalUrl: *externalUrl,
		Kind:        kind,
	}, nil
}

func (s localServicePool) addRouterToAction(metadata datastore.ActionMetaData) {
	//clientv3.OpPut(actionKey+"/"+s.routerId, strconv.FormatInt(int64(lease.ID), 10), clientv3.WithLease(lease.ID))).Commit()
	routerKey := createRouterKey(metadata, s.routerId)
	actionKey := createActionKey(metadata)
	ctx, _ := context.WithTimeout(context.Background(), etcdRequestTimeout)
	resp, err := s.etcd.Get(ctx, routerKey)
	if err != nil {
		logger.Errorf("could not register router %s to action %s due to: %s", s.routerId, actionKey, err)
		return
	}
	if len(resp.Kvs) == 0 {
		leaseTTL := int64(s.config.ServiceIdleTimeOut.Seconds())
		lease, err := s.etcd.Grant(ctx, leaseTTL)
		if err != nil {
			logger.Errorf("could not register router %s to action %s due to: %s", s.routerId, actionKey, err)
			return
		}
		_, err = s.etcd.Put(ctx, routerKey, strconv.FormatInt(int64(lease.ID), 10), clientv3.WithLease(lease.ID))
		if err != nil {
			logger.Errorf("could not register router %s to action %s due to: %s", s.routerId, actionKey, err)
			return
		}
	} else if len(resp.Kvs) == 1 {
		leaseId, err := strconv.Atoi(string(resp.Kvs[0].Value))
		if err != nil {
			logger.Errorf("Could not get lease id from etcd record.")
			return
		}
		ka, err := s.etcd.KeepAliveOnce(ctx, clientv3.LeaseID(leaseId))
		if err != nil {
			logger.Errorf("Could not keep alive router %s with lease %d", s.routerId, leaseId)
			return
		}
		logger.Debugf("Refreshed action %s for router %s for another %ds", actionKey, s.routerId, ka.TTL)
	} else {
		logger.Errorf("Too many keys located for router namespace: %s %s", s.routerId, actionKey)
		return
	}
}

func (s *localServicePool) reserveStemCellForAction(metadata datastore.ActionMetaData) (pool.Action, error) {
	//fail early if no stemcells
	if len(s.stemcells) == 0 {
		return pool.Action{}, errors.New("No stem cells available")
	}

	logger.Debugf("allocating one of the %d stemcells", len(s.stemcells))
	//actionId := fmt.Sprintf("%s/%s", metadata.Namespace, metadata.Name)
	for i, r := range s.stemcells {
		logger.Debugf("checking for kind %s; found kind %s", r.Kind, metadata.Kind)
		if s.executionProviders[r.Provider].CanHandle(metadata.Kind) {
			//TODO: consider sharding stemcells to routers, to avoid allocation collisions or extra tracking?
			providedBy := s.executionProviders[r.Provider]
			actionKey := createActionKey(metadata)
			ctx, cancel := context.WithTimeout(context.Background(), etcdRequestTimeout)
			leaseTTL := int64(s.config.ServiceIdleTimeOut.Seconds())
			lease, err := s.etcd.Grant(ctx, leaseTTL)

			resourceKey := createResourceKey(r)
			resourceData := serializeResourceData(r)
			routerKeyResp, err := s.etcd.Txn(ctx).
				If(clientv3util.KeyMissing(actionKey)).Then(
				clientv3.OpPut(actionKey, resourceData),
				clientv3.OpPut(resourceKey, actionKey),
				clientv3.OpPut(createRouterKey(metadata, s.routerId), strconv.FormatInt(int64(lease.ID), 10), clientv3.WithLease(lease.ID))).
				Else(clientv3.OpPut(createRouterKey(metadata, s.routerId), strconv.FormatInt(int64(lease.ID), 10), clientv3.WithLease(lease.ID)),
					clientv3.OpGet(actionKey)).
				Commit()
			cancel()
			if err != nil {
				return pool.Action{}, err
			}
			if !routerKeyResp.Succeeded {
				logger.Debugf("Encountered collision on key: %s, creating action from existing data", actionKey)
				// Pretty sure these hardcoded numbers are okay as long as the above transaction structure doesn't change.
				data := routerKeyResp.Responses[1].GetResponseRange().Kvs[0].Value
				existingResource, err := deserializeResourceData(data)
				if err != nil {
					return pool.Action{}, err
				}
				providedBy := s.executionProviders[existingResource.Provider]
				s.knownActions[actionKey] = pool.Action{
					Resource:       existingResource,
					ProvidedBy:     &(providedBy),
					ActionMetaData: metadata,
				}
			} else {
				logger.Debugf("Persisted stem cell allocation into etcd for %s", actionKey)
				//remove it from the available stemcells
				s.stemcells = append((s.stemcells)[:i], (s.stemcells)[i+1:]...)
				logger.Debugf("new stemcell size %d", len(s.stemcells))
				s.knownActions[actionKey] = pool.Action{
					Resource:       r,
					ProvidedBy:     &(providedBy),
					ActionMetaData: metadata,
				}
			}
			return s.knownActions[actionKey], nil
		}
	}
	return pool.Action{}, nil
}

func gc(etcd *clientv3.Client, executionProviders map[int]pool.ExecutionProvider) error {
	startTime := time.Now()
	logger.Debug("GC starting.")

	ctx, cancel := context.WithTimeout(context.Background(), etcdRequestTimeout)

	actionList, err := etcd.Get(ctx, etcdActionPrefix, clientv3.WithPrefix())
	cancel()
	if err != nil {
		logger.Errorf("Could not list actions", err)
		return err
	}
	for _, action := range actionList.Kvs {
		ctx, cancel = context.WithTimeout(context.Background(), etcdRequestTimeout)
		routerCount, err := etcd.Get(ctx, etcdRouterPrefix+string(action.Key)+"/", clientv3.WithPrefix(), clientv3.WithCountOnly())
		if err != nil {
			logger.Errorf("Could not count routers under action: %s", string(action.Key))
			continue
		}
		cancel()
		if routerCount.Count == 0 {
			logger.Debugf("Removing allocated action %s", string(action.Key))
			res, err := deserializeResourceData(action.Value)
			if err != nil {
				logger.Errorf("Could not deserialize stored resource data at key: %s", string(action.Key))
				continue
			}
			ctx, cancel = context.WithTimeout(context.Background(), etcdRequestTimeout)
			resourceKey := createResourceKey(res)
			gcResponse, err := etcd.Txn(ctx).Then(clientv3.OpDelete(resourceKey),
				clientv3.OpDelete(string(action.Key))).Commit()
			cancel()
			if err != nil {
				logger.Errorf("Could not complete GC transaction for %s", string(action.Key))
				continue
			}
			if !gcResponse.Succeeded {
				logger.Errorf("GC failed for for %s: %s", string(action.Key), gcResponse.Responses)
			} else {
				// this should maybe be async.....
				executionProviders[res.Provider].Deallocate(res.Name)
				logger.Debugf("Completed gc for: %s", string(action.Key))
			}
		} else {
			logger.Debugf("Skipping action %s as it has %d registered routers.", string(action.Key), routerCount.Count)
		}
	}
	logger.Debugf("GC complete, took: %s", time.Since(startTime))
	return nil
}

func (s *localServicePool) Warm() error {
	for _, ep := range s.executionProviders {
		// Sync the resources with etcd.
		resources, e := ep.GetResources()
		if e != nil {
			logger.Errorf("Could not start executionprovider due to: %s", e)
			return e
		}
		if len(resources) > 0 {
			logger.Debugf("Found %d services to check on.", len(resources))
			e = s.addResourcesToStorage(resources)
			if e != nil {
				logger.Errorf("Failed to store resources for stem cells:", e)
				return e
			}
		}
		if len(s.stemcells) < ep.GetConfiguredStemCellCount() {
			actionsToCreate := ep.GetConfiguredStemCellCount() - len(s.stemcells)
			newActionMetaData := make([]datastore.ActionMetaData, actionsToCreate)
			for i := 0; i < actionsToCreate; i++ {
				newActionMetaData[i] = datastore.ActionMetaData{
					Kind: "nodejs-10",
				}
			}
			resources, e = ep.AllocateStemCells(newActionMetaData)
			if e != nil {
				logger.Errorf("Failed to create stem cells:", e)
				return e
			}
			e = s.addResourcesToStorage(resources)
			if e != nil {
				logger.Errorf("Failed to store resources for stem cells:", e)
				return e
			}
		}
	}
	s.gcTicker = time.NewTicker(s.config.GCDuration)
	go func() {
		for {
			select {
			case <-s.gcTicker.C:

				err := gc(s.etcd, s.executionProviders)
				if err != nil {
					logger.Errorf("A garbage collection process failed: %s", err)
				}
			}
		}
	}()
	return nil
}

func (s *localServicePool) Activate(metadata datastore.ActionMetaData, upstreamRequest *http.Request) (<-chan *fasthttp.Response, <-chan error) {
	logger, _ = pkg.GetLogger(true)
	reply := make(chan *fasthttp.Response)
	errChan := make(chan error)
	go func(actionMeta datastore.ActionMetaData) {
		datakey := createActionKey(metadata)
		s.RLock()
		logger.Debug("Checking for known route.")
		if a, ok := s.knownActions[datakey]; ok {
			s.RUnlock()
			logger.Debugf("Sending request to known service: %s with url %s", a.Resource.Name, a.Url.String())
			resp, err := (*a.ProvidedBy).DispatchRequest(&(a.Resource), upstreamRequest)
			if err != nil {
				errChan <- err
				close(reply)
				close(errChan)
				return
			} else {
				reply <- resp
				go s.addRouterToAction(metadata)
				close(reply)
				close(errChan)
				return
				//send refresh notice to router->service mapping
				//logger.Debugf("total mappings to refresh is now %d", len(s.pendingRouterMappings.m))
				//routerMappingRefresh <- a.Resource.Name
			}
		} else {
			s.RUnlock()
			s.Lock()
			var newAction *pool.Action
			//double check (may be populated during concurrent execution in separate lock)
			//due to above lock juggling.
			if a, ok := s.knownActions[datakey]; ok {
				logger.Debugf("found mapped action after lock")
				newAction = &a
			} else {
				logger.Debugf("Checking Etcd for allocated action %s/%s", metadata.Namespace, metadata.Name)
				ctx, cancel := context.WithTimeout(context.Background(), etcdRequestTimeout)
				etcdresp, err := s.etcd.Get(ctx, createActionKey(actionMeta))
				cancel()
				if err != nil {
					errChan <- err
					s.Unlock()
					close(reply)
					close(errChan)
					return
				} else {
					if len(etcdresp.Kvs) > 0 {
						logger.Debugf("Found an ETCD record for %s/%s", metadata.Namespace, metadata.Name)
						if err != nil {
							errChan <- err
							s.Unlock()
							close(reply)
							close(errChan)
							return
						}
						var storedResource pool.Resource
						storedResource, err = deserializeResourceData(etcdresp.Kvs[0].Value)
						providerRef := s.executionProviders[storedResource.Provider]
						newAction = &pool.Action{
							Resource:       storedResource,
							ProvidedBy:     &providerRef,
							ActionMetaData: metadata,
						}
					} else {
						if len(s.stemcells) > 0 {
							logger.Debugf("Using free stem cell for action %s/%s", metadata.Namespace, metadata.Name)
							a, err := s.reserveStemCellForAction(metadata)
							if err != nil {
								errChan <- err
								s.Unlock()
								close(reply)
								close(errChan)
								return
							}
							newAction = &a
						}
					}
				}
			}
			if newAction == nil || newAction.ProvidedBy == nil {
				newAction = nil
				logger.Debugf("No free stem cells or available routes for action %s kind: %s", createActionKey(metadata), metadata.Kind)
				for _, ep := range s.executionProviders {
					if ep.CanHandle(metadata.Kind) {
						res, err := ep.AllocateStemCells([]datastore.ActionMetaData{metadata})
						if err != nil {
							errChan <- err
							s.Unlock()
							close(reply)
							close(errChan)
							return
						}
						s.stemcells = append(s.stemcells, res[0])
						a, err = s.reserveStemCellForAction(metadata)
						newAction = &a
						if err != nil {
							errChan <- err
							s.Unlock()
							close(reply)
							close(errChan)
							return
						}
					}
				}
				if newAction == nil {
					logger.Errorf("Could not find an execution provider to handle action %s", actionMeta)
					errChan <- errors.New("could not find an execution provider to handle request")
					s.Unlock()
					close(reply)
					close(errChan)
					return
				}

			}
			s.Unlock()
			resp, err := (*newAction.ProvidedBy).DispatchRequest(&(newAction.Resource), upstreamRequest)
			go (*newAction.ProvidedBy).AllocateStemCells([]datastore.ActionMetaData{metadata})
			if err != nil {
				errChan <- err
				close(reply)
				close(errChan)
				return
			} else {
				go s.addRouterToAction(metadata)
				reply <- resp
				close(reply)
				close(errChan)
			}
		}
	}(metadata)
	return reply, errChan
}
func (s *localServicePool) RegisterExecutionProvider(provider pool.ExecutionProvider) {
	s.executionProviders[provider.GetId()] = provider
}

func NewResourcePool(storageConfig PoolConfig, routerId string) (p pool.Pool, e error) {
	var err error
	logger, err = pkg.GetLogger(true)
	if err != nil {
		panic("Can't get logging!")
	}

	var etcd *clientv3.Client
	cfg := clientv3.Config{
		Endpoints: []string{storageConfig.EtcdURL},

		// set timeout per request to fail fast when the target endpoint is unavailable
		DialTimeout: etcdDialTimeout,
	}
	etcd, err = clientv3.New(cfg)
	if err != nil {
		logger.Fatalf("Could not connect to etcd. %s", err)
	}

	logger.Debug("Created new Etcd Managed Resource pool.")
	services := &localServicePool{
		knownActions:       make(map[string]pool.Action),
		config:             storageConfig,
		etcd:               etcd,
		routerId:           routerId,
		executionProviders: make(map[int]pool.ExecutionProvider),
	}

	return services, nil
}
