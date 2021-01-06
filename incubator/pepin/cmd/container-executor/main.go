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
	"context"
	flag "github.com/spf13/pflag"
	"github.com/valyala/fasthttp"
	"go.etcd.io/etcd/client"
	"go.uber.org/zap"
	"os"
	"time"
)

var (
	etcdUrl = os.Getenv("ETCD_URL")
)

type ExecParameters struct {
	parameters map[string]string
	context    map[string]string
	code       string //TODO: include url to code instead of embedded code
}

func main() {
	flag.Parse()
	logger := zap.NewExample().Sugar()
	defer logger.Sync()

	cfg := client.Config{
		Endpoints: []string{etcdUrl},
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		logger.Fatal(err)
	}
	kapi := client.NewKeysAPI(c)
	// set "/foo" key with "bar" value
	logger.Info("Setting '/foo' key with 'bar' value")
	resp, err := kapi.Set(context.Background(), "/foo", "bar", nil)
	if err != nil {
		logger.Fatal(err)
	} else {
		// print common key info
		logger.Infof("Set is done. Metadata is %q\n", resp)
	}
	// get "/foo" key's value
	logger.Info("Getting '/foo' key value")
	resp, err = kapi.Get(context.Background(), "/foo", nil)
	if err != nil {
		logger.Fatal(err)
	} else {
		// print common key info
		logger.Infof("Get is done. Metadata is %q\n", resp)
		// print value
		logger.Infof("%q key has %q value\n", resp.Node.Key, resp.Node.Value)
	}

}

// Execute action at knative by sending:
// - action code TODO: replace with action code download URL
// - params
// to the specified knative service
func exec(svc string, exec ExecParameters) {
	//curl -X POST nodejs-helloworld.default.example.com
}

func doRequest(url string) {
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(url)
	req.Header.SetMethod("POST")
	req.SetBodyString("p=q")

	resp := fasthttp.AcquireResponse()
	client := &fasthttp.Client{}
	client.Do(req, resp)

	bodyBytes := resp.Body()
	println(string(bodyBytes))
	// User-Agent: fasthttp
	// Body: p=q
}
