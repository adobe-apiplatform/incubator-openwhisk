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
import reactor.core.publisher.Mono

import scala.collection.concurrent.TrieMap
import scala.concurrent.Promise
import com.azure.cosmos.models.{ChangeFeedProcessorOptions, ThroughputProperties}
import com.azure.cosmos.{
  ChangeFeedProcessor,
  ChangeFeedProcessorBuilder,
  ConnectionMode,
  CosmosAsyncClient,
  CosmosAsyncContainer,
  CosmosClientBuilder
}
import com.fasterxml.jackson.databind.JsonNode
import com.google.common.base.Throwables
import kamon.metric.MeasurementUnit
import org.apache.openwhisk.common.{LogMarkerToken, Logging, MetricEmitter}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait ChangeFeedObserver {
  def process(docs: Seq[JsonNode]): Future[Done]
}

class ChangeFeedConsumer(collName: String, config: CacheInvalidatorConfig, observer: ChangeFeedObserver)(
  implicit ec: ExecutionContext,
  log: Logging) {
  import ChangeFeedConsumer._

  log.info(this, s"Watching changes in $collName with lease managed in ${config.feedConfig.leaseCollection}")
  val clients = scala.collection.mutable.Map[ConnectionInfo, CosmosAsyncClient]().withDefault(createCosmosClient)

  var processor: Option[ChangeFeedProcessor] = None
  var lags = new TrieMap[String, LogMarkerToken]

  def start: Future[Done] = {
    def getContainer(name: String, createIfNotExist: Boolean = false): CosmosAsyncContainer = {
      val info = config.getCollectionInfo(name)
      val client = clients(info)
      val db = client.getDatabase(info.db)
      if (createIfNotExist) {
        val throughputProperties = ThroughputProperties.createManualThroughput(info.throughput)
        db.createContainerIfNotExists(name, "/id", throughputProperties).block()
      }
      db.getContainer(name)
    }

    try {
      val targetContainer = getContainer(collName)
      val leaseContainer = getContainer(config.feedConfig.leaseCollection, createIfNotExist = true)

      val clusterId = config.invalidatorConfig.clusterId
      val prefix = clusterId.map(id => s"$id-$collName").getOrElse(collName)

      val feedOpts = new ChangeFeedProcessorOptions
      feedOpts.setLeasePrefix(prefix)
      feedOpts.setStartFromBeginning(config.feedConfig.startFromBeginning)

      val p = new ChangeFeedProcessorBuilder()
        .hostName(config.feedConfig.hostname)
        .feedContainer(targetContainer)
        .leaseContainer(leaseContainer)
        .options(feedOpts)
        .handleChanges { t: util.List[JsonNode] =>
          handleChangeFeed(t.asScala.toList)
        }
        .buildChangeFeedProcessor()
      processor = Some(p)
      p.start().toFuture.toScala.map(_ => Done)
    } catch {
      case t: Throwable => Future.failed(t)
    }
  }

  def close(): Future[Done] = {

    processor
      .map { p =>
        // be careful about exceptions thrown during ChangeFeedProcessor.stop()
        // e.g. calling stop() before start() completed, etc will throw exceptions
        try {
          p.stop().toFuture.toScala.map(_ => Done)
        } catch {
          case t: Throwable =>
            log.warn(this, s"Failed to stop processor ${t}")
            Future.failed(t)
        }
      }
      .getOrElse(Future.successful(Done))
      .andThen {
        case _ =>
          log.info(this, "Closing cosmos clients.")
          clients.values.foreach(c => c.close())
          Future.successful(Done)
      }

  }

  def handleChangeFeed(docs: List[JsonNode]): Future[Done] = {
    log.info(this, s"docs ${docs}")
    val f = observer.process(docs)
    f.andThen {
      case Success(_) =>
        MetricEmitter.emitCounterMetric(feedCounter, docs.size)
        recordLag()
      case Failure(t) =>
        log.warn(this, "Error occurred while sending cache invalidation message " + Throwables.getStackTraceAsString(t))
    }
  }

  /**
   * Records the current lag on per partition basis.
   *
   */
  private def recordLag(): Unit = {
    // FeedProcessor's getEstimatedLag provides a Map of current owner (host) and
    // an approximation of the difference between the last processed item
    // and the latest change in the container for each partition (lease document).
    // Example - 'cache-invalidator_0_169011_169259=249'
    // where cache-invalidator_0_169011_169259 (is owner as (Host)_(LeaseTokenOrPartitionKey)_(CurrentLsn)_(LatestLsn)
    // 249 is estimated lag (LatestLsn - CurrentLsn + 1).
    // In case there is zero lag, map is returns as 'cache-invalidator_0=0'
    // An empty map will be returned
    // - if the processor was not started
    // - or no lease documents matching the current feed processor instance's lease prefix could be found
    val estimatedLag = processor.get.getEstimatedLag()
    estimatedLag
      .head()
      .map(lag => {
        log.info(this, s"estimated lag is ${lag}")
        lag.asScala.keys.foreach(key => {
          val pk = key.split("_")(0)
          val gaugeToken = lags.getOrElseUpdate(pk, createLagToken(pk))
          MetricEmitter.emitGaugeMetric(gaugeToken, lag.asScala(key).longValue())
        })
      })
  }

  private def createLagToken(partitionKey: String) = {
    LogMarkerToken("cosmosdb", "change_feed", "lag", tags = Map("collection" -> "whisks", "pk" -> partitionKey))(
      MeasurementUnit.none)
  }

  implicit class RxScalaObservableMono[T](observable: Mono[T]) {

    /**
     * Returns the head of the [[Mono]] in a [[scala.concurrent.Future]].
     *
     * @return the head result of the [[Mono]].
     */
    def head(): Future[T] = {
      def toHandler[P](f: (P) => Unit): Consumer[P] = (t: P) => f(t)
      val promise = Promise[T]()
      observable.subscribe(toHandler(promise.success), toHandler(promise.failure))
      promise.future
    }
  }
}

object ChangeFeedConsumer {
  private val feedCounter =
    LogMarkerToken("cosmosdb", "change_feed", "count", tags = Map("collection" -> "whisks"))(MeasurementUnit.none)

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
