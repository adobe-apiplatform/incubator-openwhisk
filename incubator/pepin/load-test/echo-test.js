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

// track https://github.com/loadimpact/k6/issues/1081 memory explodes when sending all this data to influxdb.

export let options = {
    stages: [
        // {duration: "1m", target: 1},
        // {duration: "2m", target: 1},
        // {duration: "10m", target: 20},
        {duration: "1m", target: 10},
        {duration: "1m", target: 20},
        {duration: "1m", target: 20},
        {duration: "1m", target: 0},
    ],
    noConnectionReuse: true,
    userAgent: "MyK6UserAgentString/1.0",
    setupTimeout: "120s",
    tags: {
        "test-kind": "echo-test",
        "test-number": `${new Date().getTime()}`
    }
};


export function setup() {
    console.log("Test-number: " + options.tags.get("test-number")[0]);
}

export default function () {
    let res = http.get("http://localhost:8080");
    check(res, {
        "Action code ran": (r) => r.status === 200
    });
};

