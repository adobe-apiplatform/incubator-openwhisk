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

package org.apache.openwhisk.core.database.cosmosdb

import java.util.function.Consumer

import com.azure.cosmos.implementation.ResourceResponse
import com.azure.cosmos.models.{FeedResponse, Resource}
import reactor.core.publisher.Mono
import reactor.core.publisher.Flux

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

private[cosmosdb] trait RxObservableImplicits {

  implicit class RxScalaObservableFlux[T](observable: Flux[T]) {

    /**
     * Returns the head of the [[Flux]] in a [[scala.concurrent.Future]].
     *
     * @return the head result of the [[Flux]].
     */
    def head(): Future[T] = {
      def toHandler[P](f: (P) => Unit): Consumer[P] = (t: P) => f(t)
      val promise = Promise[T]()
      observable.subscribe(toHandler(promise.success), toHandler(promise.failure))
      promise.future
    }
  }
  implicit class RxScalaObservableMono[T](observable: Mono[T]) {

    /**
     * Returns the head of the [[Flux]] in a [[scala.concurrent.Future]].
     *
     * @return the head result of the [[Flux]].
     */
    def head(): Future[T] = {
      def toHandler[P](f: (P) => Unit): Consumer[P] = (t: P) => f(t)
      val promise = Promise[T]()
      observable.subscribe(toHandler(promise.success), toHandler(promise.failure))
      promise.future
    }
  }
  implicit class RxScalaResourceObservable[T <: Resource](observable: Flux[ResourceResponse[T]]) {
    def blockingResult(): T = observable.blockFirst().getResource
  }

  implicit class RxScalaFeedObservable[T <: Resource](observable: Flux[FeedResponse[T]]) {
    def blockingOnlyResult(): Option[T] = {
      val results = observable.blockLast().getResults.asScala
      require(results.isEmpty || results.size == 1, s"More than one result found $results")
      results.headOption

//      val results = observable.blockLast().results().asScala
//      require(results.isEmpty || results.size == 1, s"More than one result found $results")
//      results.headOption
    }
  }
}
