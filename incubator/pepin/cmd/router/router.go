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
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/datastore"
	knativeExecutionProvider "git.corp.adobe.com/bladerunner/pepin/internal/pkg/executionprovider/knative"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/executionprovider/openwhisk"
	etcdPool "git.corp.adobe.com/bladerunner/pepin/internal/pkg/pool/etcd"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"time"

	"git.corp.adobe.com/bladerunner/pepin/internal/pkg"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/pool"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/uuid"
)

const (
	statsServerAddr = ":8080"
)

var (
	etcdURL     = flag.String("etcd", "", "The address of the etcd server.")
	port        = flag.Int("port", 8080, "Port to listen on. Default is 8080")
	routerId    = flag.String("id", string(uuid.NewUUID()), "The ID for this router, should be unique across a cluster. Default is a new UUID.")
	production  = flag.BoolP("production", "p", false, "Set if you are running in production mode")
	db          = flag.String("db", "mem", "The db impl to use. Default is \"mem\", and in-memory volatile db.")
	gcduration  = flag.String("gc", "30s", "The duration string for how frequent garbage collection should run.")
	serviceIdle = flag.String("service-idle", "1m", "How long a service may be idle before it's garbage collected.")
	epOWConfig  = flag.String("ep-ow-config", "", "Path to the OW configuration file.")
	epKNConfig  = flag.String("ep-kn-config", "", "Path to the KNative configuration file.")
)

var resourcePool pool.Pool
var logger *zap.SugaredLogger
var useGateway bool
var ds datastore.Datastore

func init() {
	flag.Parse()
	var err error
	logger, err = pkg.GetLogger(*production)
	if err != nil {
		panic("Can't get logging!")
	}
	if *epKNConfig == "" && *epOWConfig == "" {
		logger.Fatalf("You must specify at least one configured Execution provider: --ep-kn-config or --ep-ow-config")
	}
}

var downstreamTimeout time.Duration

func main() {
	var err error
	if err != nil {
		logger.Fatal("Could not create resource pool.", zap.Error(err))
	}
	downstreamTimeout = 30 * time.Second

	defer logger.Sync()
	logger.Debugf("Starting router with "+
		"etcd-url: %s, "+
		"port: %d, "+
		"db: %s", *etcdURL, *port, *db)

	var storage = pool.LocalStorage
	if etcdURL != nil {
		logger.Infof("using etcd for storage at %s", *etcdURL)
		storage = pool.EtcdStorage
	}

	switch *db {
	case "mem":
		ds = &datastore.MemoryDatastore{}
	default:
		logger.Panicf("unknown database type %s", *db)
	}
	err = ds.InitStore()
	if err != nil {
		logger.Fatalw("Error initing Datastore", zap.Error(err))
	}

	gcDuration, err := time.ParseDuration(*gcduration)
	if err != nil {
		logger.Fatalw("Could not parse Garbage Collection string: ", zap.Error(err))
	}
	serviceIdleTimeout, err := time.ParseDuration(*serviceIdle)
	if err != nil {
		logger.Fatalw("Could not service idle string: ", zap.Error(err))
	}

	resourcePool, err = etcdPool.NewResourcePool(etcdPool.PoolConfig{
		PoolStorageConfig: pool.PoolStorageConfig{
			Kind:               storage,
			ServiceIdleTimeOut: serviceIdleTimeout},
		EtcdURL:    *etcdURL,
		GCDuration: gcDuration,
	}, *routerId)
	if err != nil {
		logger.Fatalw("Error building KNative Resource Pool", zap.Error(err))
	}

	if *epKNConfig != "" {
		ep, e := knativeExecutionProvider.New(*routerId, *epKNConfig)
		if e != nil {
			logger.Fatalf("Could not create Knative Execution Provider due to error: ", e)
		}
		resourcePool.RegisterExecutionProvider(ep)
	}

	if *epOWConfig != "" {
		ep, e := openwhisk.New(*routerId, *epOWConfig)
		if e != nil {
			logger.Fatalf("Could not create OpenWhisk Execution Provider due to error: ", e)
		}
		resourcePool.RegisterExecutionProvider(ep)
	}

	resourcePool.Warm()

	if *production {
		gin.SetMode(gin.ReleaseMode)
	}
	router := gin.Default()
	runtime.SetBlockProfileRate(1)
	if !*production {
		pprof.Register(router)
	}

	router.Any("/api/v1/web/:namespace/:package/:action", func(context *gin.Context) {
		action := context.Params.ByName("action")
		namespace := context.Params.ByName("namespace")
		_, err := ds.GetAction(namespace, action)
		if err != nil {
			context.AbortWithError(404, errors.New(fmt.Sprintf("action %s/%s not found", namespace, action)))
		} else {
			context.String(200, "Going!")
		}
	})

	router.POST("/namespaces/:namespace/actions/:action", func(context *gin.Context) {
		//get action from path
		action := context.Params.ByName("action")
		namespace := context.Params.ByName("namespace")
		a, err := ds.GetAction(namespace, action)
		if err != nil {
			context.AbortWithError(404, errors.New(fmt.Sprintf("action %s/%s not found", namespace, action)))
		} else {
			responseChan, errChan := resourcePool.Activate(a, context.Request)
			select {
			case response := <-responseChan:
				logger.Debugf("Got a response %d", response.StatusCode())
				contentType := response.Header.ContentType()
				allHeaders := make(map[string]string)
				response.Header.VisitAll(func(key []byte, value []byte) {
					sKey := string(key)
					if sKey != "Content-Type" {
						allHeaders[sKey] = string(value)
					}
				})
				context.DataFromReader(response.StatusCode(), int64(response.Header.ContentLength()), string(contentType[:]), bytes.NewReader(response.Body()), allHeaders)
				return
			case e := <-errChan:
				logger.Debugf("Error reported from activation request: %s", e)
				context.String(http.StatusInternalServerError, e.Error())
				return
			case <-time.After(downstreamTimeout):
				logger.Debugf("Timed out waiting for service response.")
				context.String(http.StatusInternalServerError, "Requesting resources timed out.")
				return
			}

		}
	})
	router.GET("/healthz", func(context *gin.Context) {
		context.JSON(200, gin.H{
			"message": "pong",
		})
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: router,
	}
	go func() {
		// service connections
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("listen: %s\n", err)
		}
	}()

	//channel for shutdown signal
	quit := make(chan os.Signal)

	// Wait for interrupt signal to gracefully shutdown the server with
	// a timeout of 5 seconds.

	signal.Notify(quit, os.Interrupt)
	<-quit
	logger.Info("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Fatal("Server Shutdown:", err)
	}
	resourcePool.Close()
	logger.Info("Server exiting")
}
