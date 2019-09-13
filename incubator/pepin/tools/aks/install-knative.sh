#!/bin/bash
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
set -euxo pipefail

kubectl apply --selector knative.dev/crd-install=true \
  --filename https://github.com/knative/serving/releases/download/v0.8.0/serving.yaml \
  --filename https://github.com/knative/eventing/releases/download/v0.8.0/release.yaml \
  --filename https://github.com/knative/serving/releases/download/v0.8.0/monitoring.yaml

kubectl apply --filename https://github.com/knative/serving/releases/download/v0.8.0/serving.yaml \
  --filename https://github.com/knative/eventing/releases/download/v0.8.0/release.yaml \
  --filename https://github.com/knative/serving/releases/download/v0.8.0/monitoring.yaml
