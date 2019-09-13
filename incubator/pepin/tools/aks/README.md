
<!--
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->
# Create AKS Cluster

1. Pick a resource group name:`export RESOURCE_GROUP=<resourceGroup>`
1. Pick a cluster name: `export CLUSTER_NAME=<clusterName>`
1. Pick a container registry name: `export ACR_NAME=<acrName>`

Create the cluster:
```bash
az group create --name ${RESOURCE_GROUP} --location eastus
az aks create \
    --resource-group ${RESOURCE_GROUP} \
    --name  ${CLUSTER_NAME} \
    --node-count 3 \
    --enable-addons monitoring \
    --generate-ssh-keys
az aks get-credentials --resource-group ${RESOURCE_GROUP} --name ${CLUSTER_NAME} --admin
kubectl create clusterrolebinding kubernetes-dashboard --clusterrole=cluster-admin --serviceaccount=kube-system:kubernetes-dashboard
```

# Create a Container Registry
Note the deployment user name and secret this last step emits, you will need it later.
```bash
tools/aks/setup-acr.sh ${ACR_NAME}
export REGISTRY_SERVER=$(az acr show --resource-group=${RESOURCE_GROUP} --name=${ACR_NAME} --query 'loginServer' --output tsv)
```

Use the deployment credentials emitted above to login to the server, locally. This allows you to push the container to your ACR.
```
docker login ${REGISTRY_SERVER}
```

# Prepare cluster
Sometimes you need to run this a few times as it's doing a lot of stuff and not waiting for things to settle. It's idempotent so you can run it again and again.
This installs helm and istio in your cluster.
```bash
az aks get-credentials --resource-group ${RESOURCE_GROUP} --name ${CLUSTER_NAME} --admin
tools/aks/prepare-cluster.sh
```

# Install KNative
```bash
az aks get-credentials --resource-group ${RESOURCE_GROUP} --name ${CLUSTER_NAME} --admin
tools/aks/install-knative.sh
```

# Build Containers and Push to ACR
```bash
make container deploy-container
```

# Install Pepin
```bash
make deploy-router
```
