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

import akka.Done
import com.azure.cosmos.implementation.Constants
import com.fasterxml.jackson.databind.JsonNode
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.database.CacheInvalidationMessage
import org.apache.openwhisk.core.database.cosmosdb.CosmosDBConstants
import org.apache.openwhisk.core.database.cosmosdb.CosmosDBUtil.unescapeId
import org.apache.openwhisk.core.entity.CacheKey

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}

class WhiskChangeEventObserver(config: InvalidatorConfig, eventProducer: EventProducer)(implicit ec: ExecutionContext,
                                                                                        log: Logging)
    extends ChangeFeedObserver {
  import WhiskChangeEventObserver._

  override def process(docs: Seq[JsonNode]): Future[Done] = {
    //Each observer is called from a pool managed by CosmosDB ChangeFeedProcessor
    //So its fine to have a blocking wait. If this fails then batch would be reread and
    //retried thus ensuring at-least-once semantics
    eventProducer.send(processDocs(docs, config))
  }
}

trait EventProducer {
  def send(msg: Seq[String]): Future[Done]
}

object WhiskChangeEventObserver {
  val instanceId = "cache-invalidator"

  def processDocs(docs: Seq[JsonNode], config: InvalidatorConfig)(implicit log: Logging): Seq[String] = {
    docs
      .filter { doc =>
        val cid = Option(getString(doc, CosmosDBConstants.clusterId))
        val currentCid = config.clusterId

        //only if current clusterId is configured do a check
        currentCid match {
          case Some(_) => cid != currentCid
          case None    => true
        }
      }
      .map { doc =>
        val id = unescapeId(getString(doc, Constants.Properties.ID))
        log.info(this, s"Changed doc [$id]")
        val event = CacheInvalidationMessage(CacheKey(id), instanceId)
        event.serialize
      }
  }

  def getString(jsonNode: JsonNode, propertyName: String): String =
    if (jsonNode.has(propertyName)) jsonNode.get(propertyName).asText
    else null

}
