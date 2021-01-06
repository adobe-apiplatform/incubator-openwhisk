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
	"flag"
	"fmt"
	"github.com/valyala/fasthttp"
	"log"
)

var (
	addr     = flag.String("addr", ":8080", "TCP address to listen to")
	compress = flag.Bool("compress", false, "Whether to enable transparent response compression")
)

func main() {
	flag.Parse()

	h := requestHandler
	if *compress {
		h = fasthttp.CompressHandler(h)
	}
	// Create custom server.
	s := &fasthttp.Server{
		Handler: requestHandler,

		// Every response will contain 'Server: My super server' header.
		Name: "My super server",

		// Other Server settings may be set here.
		//override const DefaultConcurrency = 256 * 1024
		Concurrency: 256 * 1024,
	}
	//if err := fasthttp.ListenAndServe(*addr, h); err != nil {
	//	log.Fatalf("Error in ListenAndServe: %s", err)
	//}
	if err := s.ListenAndServe(*addr); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}

}

func requestHandler(ctx *fasthttp.RequestCtx) {
	fmt.Println(string(ctx.Path()))

	fmt.Fprintf(ctx, "Hello, world!\n\n")

	//fmt.Fprintf(ctx, "Request method is %q\n", ctx.Method())
	//fmt.Fprintf(ctx, "RequestURI is %q\n", ctx.RequestURI())
	//fmt.Fprintf(ctx, "Requested path is %q\n", ctx.Path())
	//fmt.Fprintf(ctx, "Host is %q\n", ctx.Host())
	//fmt.Fprintf(ctx, "Query string is %q\n", ctx.QueryArgs())
	//fmt.Fprintf(ctx, "User-Agent is %q\n", ctx.UserAgent())
	//fmt.Fprintf(ctx, "Connection has been established at %s\n", ctx.ConnTime())
	//fmt.Fprintf(ctx, "Request has been started at %s\n", ctx.Time())
	//fmt.Fprintf(ctx, "Serial request number for the current connection is %d\n", ctx.ConnRequestNum())
	//fmt.Fprintf(ctx, "Your ip is %q\n\n", ctx.RemoteIP())
	//
	//fmt.Fprintf(ctx, "Raw request is:\n---CUT---\n%s\n---CUT---", &ctx.Request)

	ctx.SetContentType("text/plain; charset=utf8")

	//// Set arbitrary headers
	//ctx.Response.Header.Set("X-My-Header", "my-header-value")

	//// Set cookies
	//var c fasthttp.Cookie
	//c.SetKey("cookie-name")
	//c.SetValue("cookie-value")
	//ctx.Response.Header.SetCookie(&c)
}
