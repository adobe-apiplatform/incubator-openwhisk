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
	"bytes"
	"encoding/json"
	"fmt"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/datastore"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/executionprovider"
	"git.corp.adobe.com/bladerunner/pepin/internal/pkg/pool"
	servingMeta "github.com/knative/serving/pkg/apis/serving/v1alpha1"
	servingClient "github.com/knative/serving/pkg/client/clientset/versioned"
	"github.com/valyala/fasthttp"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"html/template"
	"io/ioutil"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"net/url"
)

var logger *zap.SugaredLogger

var defaultServiceTemplate = `
{
    "apiVersion": "serving.knative.dev/v1alpha1",
    "kind": "Service",
    "metadata": {
        "name": "{{.Name}}",
        "namespace": "{{.Namespace}}",
		"labels": {
			"ow-kind": "nodejs-10",
			"stemcell": "true",
			"ow-namespace": "{{.OwNamespace}}",
			"ow-action": "{{.OwAction}}",
			"ow-router": "{{.OwRouter}}"
		}
    },
    "spec": {
        "template": {
			"metadata" : {
				"annotations": {
					"autoscaling.knative.dev/class": "hpa.autoscaling.knative.dev",
					"autoscaling.knative.dev/metric": "cpu",
					"autoscaling.knative.dev/target": "50",
					"autoscaling.knative.dev/minScale": "1",
					"autoscaling.knative.dev/maxScale": "100"
				}
			},
            "spec": {
                "containers": [
                    {
                        "env": [
                            {
                                "name": "TARGET",
                                "value": "Go Sample v1 - {{.Name}}"
                            }
                        ],
                        "image": "{{.ImageName}}",
                        "name": "user-container",
                        "resources": {}
                    }
                ]
            }
        }
    }
}
`

type templateData struct {
	Name        string
	Namespace   string
	ImageName   string
	OwNamespace string
	OwAction    string
	OwRouter    string
}
type epConfig struct {
	StemCellCount             int    `json:"stemcellcount"`
	ServiceTemplatePath       string `json:"servicetemplatepath"`
	MasterURL                 string `json:"masterurl"`
	KubeConfigPath            string `json:"kubeconfigpath"`
	ActionNamespace           string `json:"actionnamespace"`
	DefaultImageName          string `json:"defaultimage"`
	ClusterGateway            string `json:"clustergateway"`
	GarbageCollectionDuration string `json:"garbagecollectionduration"`
}

type executionProvider struct {
	config          epConfig
	serviceTemplate *template.Template
	clientSet       *servingClient.Clientset
	gcTicker        *time.Ticker
	gwClient        *fasthttp.HostClient
	httpClient      *fasthttp.Client
	routerid        string
}

func (ep *executionProvider) CanHandle(kind string) bool {
	return kind == "nodejs-10"
}

func (ep *executionProvider) DispatchRequest(resource *pool.Resource, upstreamRequest *http.Request) (*fasthttp.Response, error) {
	req, err := ep.prepareRequest(resource, upstreamRequest)
	if err != nil {
		logger.Errorf("Could not get execution provider: %d to create request object", ep.GetId())
		return nil, err
	}
	resp := fasthttp.AcquireResponse()
	if ep.config.ClusterGateway != "" {
		err = ep.gwClient.Do(req, resp)
	} else {
		err = ep.httpClient.Do(req, resp)
	}
	return resp, err
}

func (ep *executionProvider) GetId() int {
	return executionprovider.KNative
}

func New(routerid string, configuration string) (pool.ExecutionProvider, error) {
	logger, _ = pkg.GetLogger(true)
	var config epConfig
	if configuration == "" {
		return nil, errors.New("Configuration string cannot be empty.")
	}
	configBytes, configPath, err := executionprovider.CleanConfigString(configuration)

	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		return nil, err
	}
	ep := executionProvider{config: config}
	ep.routerid = routerid
	if ep.config.KubeConfigPath != "" && configPath != "" && !filepath.IsAbs(ep.config.KubeConfigPath) {
		ep.config.KubeConfigPath = filepath.Join(filepath.Dir(configPath), ep.config.KubeConfigPath)
	}

	var clientConfig *rest.Config
	if ep.config.MasterURL == "" && ep.config.KubeConfigPath == "" {
		clientConfig, err = rest.InClusterConfig()
		if err != nil {
			logger.Fatalf("Could not get KNative Clientset", err)
			return nil, err
		}
	} else {
		clientConfig, err = clientcmd.BuildConfigFromFlags(ep.config.MasterURL, ep.config.KubeConfigPath)
		if err != nil {
			logger.Fatalf("Could not get KNative Clientset", err)
			return nil, err
		}
	}
	ep.clientSet, err = servingClient.NewForConfig(clientConfig)
	if err != nil {
		logger.Errorf("Could not get KNative Clientset", err)
		return nil, err
	}

	// Configure the service template to use for services.
	if ep.config.ServiceTemplatePath == "" {
		logger.Debugf("Using Default Knative service configuration.")
		tmpl, err := template.New("service-template").Parse(defaultServiceTemplate)
		if err != nil {
			return nil, errors.Errorf("Could not parse default service template: %s", err)
		}
		ep.serviceTemplate = tmpl
	} else {
		logger.Debugf("Loading Knative service configuration: %s", ep.config.ServiceTemplatePath)
		templateData, err := ioutil.ReadFile(ep.config.ServiceTemplatePath)
		if err != nil {
			return nil, errors.Errorf("Could not read proposed service template: %s", err)
		}
		tmpl, err := template.New("service-template").Parse(string(templateData))
		if err != nil {
			return nil, errors.Errorf("Could not parsed proposed service template: %s", err)
		}
		ep.serviceTemplate = tmpl
	}
	if ep.config.ClusterGateway != "" {

		ep.config.ClusterGateway = strings.TrimSuffix(ep.config.ClusterGateway, "/")
		ep.gwClient = &fasthttp.HostClient{
			Addr: ep.config.ClusterGateway,
		}
	}
	ep.httpClient = &fasthttp.Client{}
	return &ep, nil
}

func (ep *executionProvider) Deallocate(name string) error {
	logger.Debugf("Deleting service with name: %s", name)
	var err error

	grace := int64(0)
	p := v1.DeletePropagationForeground
	options :=
		v1.DeleteOptions{
			GracePeriodSeconds: &grace,
			PropagationPolicy:  &p,
		}

	err = ep.clientSet.ServingV1alpha1().Services(ep.config.ActionNamespace).Delete(name, &options)
	if err != nil {
		logger.Errorf("Could not create service in knative.", err)
	}
	return err
}

func (ep *executionProvider) GetConfiguredStemCellCount() int {
	return ep.config.StemCellCount
}

func (ep *executionProvider) AllocateStemCells(metadata []datastore.ActionMetaData) ([]pool.Resource, error) {
	logger.Debugf("Allocating %d more stem cells.", len(metadata))

	newResources := make([]pool.Resource, len(metadata))

	for i, meta := range metadata {
		r, error := ep.buildKnativeService(meta)
		if error != nil {
			logger.Error("Could not create Knative servivce.", error)
			return nil, error
		}
		logger.Debugf("Allocated new KnativeService: %s", r.Url.String())

		newResources[i] = r
	}
	return newResources, nil
}

func (ep *executionProvider) GetResources() ([]pool.Resource, error) {
	registeredServices, err := ep.clientSet.ServingV1alpha1().Services(ep.config.ActionNamespace).List(v1.ListOptions{
		LabelSelector: "ow-namespace, ow-action, ow-router=" + ep.routerid,
	})
	if err != nil {
		logger.Warnf("Could not read existing services on start: %s", err.Error())
	}
	if len(registeredServices.Items) > 0 {
		logger.Debugf("Warming up cache from k8s.")
	} else {
		logger.Debugf("Found no existing KNative services to cache.")
		return []pool.Resource{}, nil
	}
	resources := make([]pool.Resource, len(registeredServices.Items))

	for i, aService := range registeredServices.Items {
		//var ns = aService.GetObjectMeta().GetLabels()["ow-namespace"]
		//var name = aService.GetObjectMeta().GetLabels()["ow-action"]
		var kind = aService.GetObjectMeta().GetLabels()["ow-kind"]

		resource := pool.Resource{
			Provider:    executionprovider.KNative,
			Name:        aService.Name,
			Kind:        kind,
			Url:         url.URL(*aService.Status.Address.URL),
			ExternalUrl: url.URL(*aService.Status.RouteStatusFields.URL)}
		resources[i] = resource
	}

	return resources, nil
}

func (ep *executionProvider) buildKnativeService(action datastore.ActionMetaData) (pool.Resource, error) {
	var err error
	uuid := string(uuid.NewUUID())
	serviceObj := servingMeta.Service{}

	if err != nil {
		logger.Errorf("Could not parse knative service template", err)
		return pool.Resource{}, err
	}
	var templateBuffer bytes.Buffer
	templatedata := templateData{
		Name:        fmt.Sprintf("ow-%s", uuid),
		Namespace:   ep.config.ActionNamespace,
		ImageName:   ep.config.DefaultImageName,
		OwNamespace: action.Namespace,
		OwAction:    action.Name,
		OwRouter:    ep.routerid,
	}
	err = ep.serviceTemplate.Execute(&templateBuffer, templatedata)
	if err != nil {
		logger.Errorf("Could not parse knative service template", err)
		return pool.Resource{}, err
	}

	json.Unmarshal([]byte(templateBuffer.String()), &serviceObj)
	logger.Debugf("Installing service with JSON: %s", templateBuffer.String())
	_, err = ep.clientSet.ServingV1alpha1().Services(ep.config.ActionNamespace).Create(&serviceObj)
	if err != nil {
		logger.Errorf("Could not create service in knative.", err)
		return pool.Resource{}, err
	}

	servicesWatcher, err := ep.clientSet.ServingV1alpha1().Services(ep.config.ActionNamespace).Watch(v1.ListOptions{
		FieldSelector: "metadata.name=" + templatedata.Name,
	})

	if err != nil {
		return pool.Resource{}, err
	}
	servicesResultsChan := servicesWatcher.ResultChan()
	for event := range servicesResultsChan {
		service, ok := event.Object.(*servingMeta.Service)
		if !ok {
			logger.Error("Could not get service object from event.")
		}
		switch event.Type {
		case watch.Modified:
			logger.Debugf("Modification of service: %s", service.Name)
			if service.Status.Conditions != nil {
				var allReady = true
				for _, cond := range service.Status.Conditions {
					if cond.Status != corev1.ConditionTrue {
						logger.Debugf("Condition Still not true: %s", cond.Type)
						allReady = false
					}
				}
				if allReady && len(service.Status.Conditions) > 0 {
					servicesWatcher.Stop()
					latestRev := service.Status.LatestReadyRevisionName
					internalUrl := url.URL{Scheme: "http", Host: latestRev}
					logger.Debugf("New service available at internal url %s and external url %s", internalUrl.String(), service.Status.RouteStatusFields.URL.String())
					//TODO: don't use "default", but the appropriate namespace
					//latestRev := fmt.Sprintf("%s", service.Status.LatestReadyRevisionName)
					return pool.Resource{Provider: executionprovider.KNative, Name: templatedata.Name, Kind: "nodejs-10", Url: internalUrl, ExternalUrl: url.URL(*service.Status.RouteStatusFields.URL)}, nil
				}
			}
		}
	}
	return pool.Resource{}, fmt.Errorf("Could not get service URL in time.")

}

func (ep *executionProvider) prepareRequest(action *pool.Resource, upstreamRequest *http.Request) (*fasthttp.Request, error) {
	req := fasthttp.AcquireRequest()
	query := ""
	if len(upstreamRequest.URL.Query()) > 0 {
		query = "?" + upstreamRequest.URL.RawQuery
	}
	if ep.config.ClusterGateway != "" {
		req.SetRequestURI("/run" + query)
		req.Header.SetHost(action.ExternalUrl.Host)
		logger.Debugf("Using gateway @ %s with Host header: %s", ep.config.ClusterGateway, action.ExternalUrl.Host)
	} else {
		req.SetRequestURI(action.Url.String() + "/run" + query)
	}
	if upstreamRequest.ContentLength > 0 {
		bodyReader, err := upstreamRequest.GetBody()
		if err != nil {
			return nil, err
		}
		req.SetBodyStream(bodyReader, int(upstreamRequest.ContentLength))
	}
	logger.Debugf("Request Prepared \n\turl: %s\n\tHost Header:%s", string(req.RequestURI()), string(req.Header.Host()))
	return req, nil

}
