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

let urlOptions = ["test1", "test2", "test3", "test4", "test5"];
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
        "test-kind": __ENV.KNATIVE + "/" + __ENV.KNATIVE_LOOPBACK,
        "test-number": `${new Date().getTime()}`
    }
};


function getURL() {
    if (__ENV.KNATIVE === "1") {
        let url = `${__ENV.TARGET}/namespaces/bladerun_test/actions/${urlOptions[Math.floor(Math.random() * urlOptions.length)]}`;
        if (__ENV.KNATIVE_LOOPBACK === "1") {
            return url + "?loopback=true";
        } else if (__ENV.KNATIVE_LOOPBACK === "2") {
            return url + "?echo=true";
        }
        return url;
    } else {
        return `${__ENV.TARGET}/${urlOptions[Math.floor(Math.random() * urlOptions.length)]}`
    }
}

export function setup() {
    console.log("KNATIVE: " + __ENV.KNATIVE);
    console.log("KNATIVE_LOOPBACK: " + __ENV.KNATIVE_LOOPBACK);
    console.log("Test-number: " + options.tags.get("test-number")[0]);
    if (__ENV.KNATIVE === "1") {
        urlOptions.forEach(value => {
            console.log("Warming Knative Service: " + value);
            let res = http.post(`${__ENV.TARGET}/namespaces/bladerun_test/actions/${value}`);
            check(res, {
                "Action code ready": (r) => r.status === 200
            });
            console.log("Knative Service Ready: " + value + " @ " + res.status);
        });
        urlOptions.forEach(value => {
            let res = http.post(`${__ENV.TARGET}/namespaces/bladerun_test/actions/${value}`);
            check(res, {
                "Action code ready": (res) => res.status == 200
            })
        });
    }
}

export default function () {
    if (__ENV.KNATIVE === "1") {
        http.post(getURL());
    } else {
        http.get(getURL());
    }
};
