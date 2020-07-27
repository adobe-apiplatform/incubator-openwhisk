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
import java.util.function.Consumer

import akka.Done
import com.azure.cosmos.{
  ChangeFeedProcessorBuilder,
  ConnectionMode,
  CosmosAsyncClient,
  CosmosAsyncContainer,
  CosmosClientBuilder
}
import com.azure.cosmos.models.{ChangeFeedProcessorOptions, ThroughputProperties}
import com.fasterxml.jackson.databind.JsonNode
import org.apache.openwhisk.common.Logging

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}

trait ChangeFeedObserver {
  def process(docs: Seq[JsonNode]): Future[Done]
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

    val processor = new ChangeFeedProcessorBuilder()
      .hostName(config.feedConfig.hostname)
      .feedContainer(targetContainer)
      .leaseContainer(leaseContainer)
      .options(feedOpts)
      .handleChanges { t: util.List[JsonNode] =>
        handleChangeFeed(t.asScala.toList)
      }
      .buildChangeFeedProcessor()
    (processor, processor.start().toFuture.toScala.map(_ => Done))
  }

  def isStarted: Future[Done] = startFuture

  def handleChangeFeed(docs: List[JsonNode]) = {
    log.info(this, s"docs ${docs}")
    observer.process(docs)
  }

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

    val throughputProperties = ThroughputProperties.createAutoscaledThroughput(info.throughput)
    if (createIfNotExist) {
      db.createContainerIfNotExists(name, "/id", throughputProperties)
    }
    db.getContainer(name)
  }

}

object ChangeFeedConsumer {
  def createCosmosClient(conInfo: ConnectionInfo): CosmosAsyncClient = {
    val clientBuilder = new CosmosClientBuilder()
      .endpoint(conInfo.endpoint)
      .key(conInfo.key)
      .consistencyLevel(conInfo.consistencyLevel)
      .contentResponseOnWriteEnabled(true)
    if (conInfo.connectionMode == ConnectionMode.GATEWAY) {
      clientBuilder.gatewayMode()
    } else {
      clientBuilder.directMode()
    }
    clientBuilder.buildAsyncClient()
  }
}
