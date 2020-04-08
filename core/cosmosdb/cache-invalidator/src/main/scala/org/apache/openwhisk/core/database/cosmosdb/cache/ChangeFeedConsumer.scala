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

package org.apache.openwhisk.core.database.cosmosdb.cache

import java.util

import akka.Done
import com.azure.cosmos.{
  ChangeFeedProcessor,
  ConnectionPolicy,
  CosmosAsyncClient,
  CosmosAsyncContainer,
  CosmosClientBuilder
}
import com.azure.cosmos.implementation.changefeed.implementation.ChangeFeedProcessorBuilderImpl
import com.azure.cosmos.implementation.changefeed.{ChangeFeedObserverCloseReason, ChangeFeedObserverContext}
import com.azure.cosmos.models.ChangeFeedProcessorOptions
import com.fasterxml.jackson.databind.JsonNode
import org.apache.openwhisk.common.Logging
import reactor.core.publisher.Mono

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}

trait ChangeFeedObserver {
  def process(context: ChangeFeedObserverContext, docs: Seq[JsonNode]): Future[Done]
}

class ChangeFeedConsumer(collName: String, config: CacheInvalidatorConfig, observer: ChangeFeedObserver)(
  implicit ec: ExecutionContext,
  log: Logging) {
  import ChangeFeedConsumer._

  log.info(this, s"Watching changes in $collName with lease managed in ${config.feedConfig.leaseCollection}")

  private val clients =
    scala.collection.mutable.Map[ConnectionInfo, CosmosAsyncClient]().withDefault(createCosmosClient)
  private val targetContainer = getContainer(collName)
  private val leaseContainer = getContainer(config.feedConfig.leaseCollection, createIfNotExist = true)
  private val (processor, startFuture) = {
    val clusterId = config.invalidatorConfig.clusterId
    val prefix = clusterId.map(id => s"$id-$collName").getOrElse(collName)

    val feedOpts = new ChangeFeedProcessorOptions
    feedOpts.setLeasePrefix(prefix)
    feedOpts.setStartFromBeginning(config.feedConfig.startFromBeginning)
    val builder = ChangeFeedProcessor
      .changeFeedProcessorBuilder()
      .setHostName(config.feedConfig.hostname)
      .setFeedContainer(targetContainer)
      .setLeaseContainer(leaseContainer)
      .setOptions(feedOpts)
      .asInstanceOf[ChangeFeedProcessorBuilderImpl] //observerFactory is not exposed hence need to cast to impl

    builder.observerFactory(() => ObserverBridge)
    val p = builder.build()
    (p, p.start().toFuture.toScala.map(_ => Done))
  }

  def isStarted: Future[Done] = startFuture

  def close(): Future[Done] = {
    val f = processor.stop().toFuture.toScala.map(_ => Done)
    f.andThen {
      case _ =>
        clients.values.foreach(c => c.close())
        Future.successful(Done)
    }
  }

  private def getContainer(name: String, createIfNotExist: Boolean = false): CosmosAsyncContainer = {
    val info = config.getCollectionInfo(name)
    val client = clients(info)
    val db = client.getDatabase(info.db)
    val container = db.getContainer(name)

    val resp = if (createIfNotExist) {
      db.createContainerIfNotExists(name, "/id", info.throughput)
    } else container.read()

    resp.block().getContainer
  }

  private object ObserverBridge extends com.azure.cosmos.implementation.changefeed.ChangeFeedObserver {
    override def open(context: ChangeFeedObserverContext): Unit = {}
    override def close(context: ChangeFeedObserverContext, reason: ChangeFeedObserverCloseReason): Unit = {}
    override def processChanges(context: ChangeFeedObserverContext, docs: util.List[JsonNode]): Mono[Void] = {
      val f = observer.process(context, docs.asScala.toList).map(_ => null).toJava.toCompletableFuture
      Mono.fromFuture(f)
    }
  }
}

object ChangeFeedConsumer {
  def createCosmosClient(conInfo: ConnectionInfo): CosmosAsyncClient = {
    val policy = ConnectionPolicy.getDefaultPolicy.setConnectionMode(conInfo.connectionMode)
    new CosmosClientBuilder()
      .setEndpoint(conInfo.endpoint)
      .setKey(conInfo.key)
      .setConnectionPolicy(policy)
      .setConsistencyLevel(conInfo.consistencyLevel)
      .buildAsyncClient()
  }
}
