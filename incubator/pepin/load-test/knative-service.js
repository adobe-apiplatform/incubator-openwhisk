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
import http from "k6/http";
import {check, group} from "k6";

export let options = {
    stages: [
        {duration: "1m", target: 10},
        {duration: "1m", target: 20},
        {duration: "1m", target: 20},
        {duration: "1m", target: 0},
    ],
    noConnectionReuse: true,
    userAgent: "MyK6UserAgentString/1.0",
    setupTimeout: "120s",
    tags: {
        "test-kind": "kn-direct",
        "test-number": `${new Date().getTime()}`
    }
};


function getTargetService(targets) {
    return targets[Math.floor(Math.random() * targets.length)]
}

export function setup() {
    console.log("Test-number: " + options.tags.get("test-number")[0]);
    let targets = [];
    let rawTargets = __ENV.TARGETS;
    rawTargets.split(",").forEach(value => {
        targets.push(value.substr(7));
    });
    console.log("TARGETS: " + targets);

    targets.forEach(value => {
        let headers = {
            "Host" : value,
        };
        let res = http.get(__ENV.INGRESS, { headers : headers });
        check(res, {
            "Action code ready": (r) => r.status === 200
        });
        console.log("Knative Service Ready: " + value + " @ " + res.status);
    });
    return targets;
}

export default function (targets) {
    let headers = {
        "Host" : getTargetService(targets),
    };
    let res = http.get(__ENV.INGRESS, { headers: headers });
    check(res, {
        "Action code ran": (r) => r.status === 200
    });
};

