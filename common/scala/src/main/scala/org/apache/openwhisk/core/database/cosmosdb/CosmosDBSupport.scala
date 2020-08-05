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

import com.azure.cosmos.{CosmosAsyncClient, CosmosAsyncContainer, CosmosAsyncDatabase}
import com.azure.cosmos.models.{
  CosmosContainerProperties,
  FeedResponse,
  SqlParameter,
  SqlQuerySpec,
  ThroughputProperties
}
import reactor.core.scala.publisher.SMono
import org.apache.openwhisk.common.Logging
import reactor.core.scala.publisher.ScalaConverters._

import scala.collection.JavaConverters._

private[cosmosdb] trait CosmosDBSupport extends CosmosDBUtil {
  protected def config: CosmosDBConfig
  protected def collName: String
  protected def client: CosmosAsyncClient
  protected def viewMapper: CosmosDBViewMapper

  def initialize()(implicit logging: Logging): (CosmosAsyncDatabase, CosmosAsyncContainer) = {
    val db = getOrCreateDatabase()
    (db, getOrCreateCollection(db))
  }

  private def getOrCreateDatabase()(implicit logging: Logging): CosmosAsyncDatabase = {
    client
      .createDatabaseIfNotExists(config.db)
      .asScala
      .flatMap(r => SMono.just(client.getDatabase(r.getProperties.getId)))
      .block()
  }

  private def getOrCreateCollection(database: CosmosAsyncDatabase)(implicit logging: Logging) = {
    val throughputProperties = ThroughputProperties.createManualThroughput(config.throughput)
    val containerProperties = new CosmosContainerProperties(collName, viewMapper.partitionKeyDefn)
    val ttl: Int = config.timeToLive.map(_.toSeconds.toInt).getOrElse(-1)
    containerProperties.setDefaultTimeToLiveInSeconds(ttl)
    containerProperties.setIndexingPolicy(viewMapper.indexingPolicy.asJava())
    val container = database
      .createContainerIfNotExists(containerProperties, throughputProperties)
      .asScala
      .flatMap { c =>
        SMono.just(database.getContainer(c.getProperties.getId))
      }
      .block()

    //validate the indexing policy matches expected
    val containerProps = database
      .queryContainers(querySpec(collName))
      .byPage()
      .asScala
      .head
      .block()
    require(containerProps.getResults.size == 1, s"container was not created ${collName}")
    val existingIndexingPolicy = IndexingPolicy(containerProps.getResults.get(0).getIndexingPolicy)
    val expectedIndexingPolicy = viewMapper.indexingPolicy
    if (!IndexingPolicy.isSame(expectedIndexingPolicy, existingIndexingPolicy)) {
      logging.warn(
        this,
        s"Indexing policy for collection [$collName] found to be different." +
          s"\nExpected - ${expectedIndexingPolicy.asJava()}" +
          s"\nExisting - ${existingIndexingPolicy}")
    }
    container
  }

  /**
   * Prepares a query for fetching any resource by id
   */
  protected def querySpec(id: String) =
    new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id", Seq(new SqlParameter("@id", id)).asJava)

  protected def asVector[T](r: FeedResponse[T]): Vector[T] = {
    r.getResults.asScala.toVector
  }
}
