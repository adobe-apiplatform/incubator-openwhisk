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

import com.azure.cosmos.implementation.Database
import java.util.ArrayList
import com.azure.cosmos.CosmosAsyncDatabase
import com.azure.cosmos.models.{SqlParameter, SqlQuerySpec}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import pureconfig._
import pureconfig.generic.auto._
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.database.test.behavior.ArtifactStoreTestUtil.storeAvailable
import reactor.core.scala.publisher.SMono
import reactor.core.scala.publisher.ScalaConverters._

import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer
import scala.util.{Random, Try}

trait CosmosDBTestSupport extends FlatSpecLike with BeforeAndAfterAll with RxObservableImplicits {
  private val dbsToDelete = ListBuffer[CosmosAsyncDatabase]()

  lazy val storeConfigTry = Try { loadConfigOrThrow[CosmosDBConfig](ConfigKeys.cosmosdb) }
  lazy val client = storeConfig.createClient()
  val useExistingDB = java.lang.Boolean.getBoolean("whisk.cosmosdb.useExistingDB")

  def storeConfig = storeConfigTry.get

  override protected def withFixture(test: NoArgTest) = {
    assume(storeAvailable(storeConfigTry), "CosmosDB not configured or available")
    super.withFixture(test)
  }

  protected def generateDBName() = {
    s"travis-${getClass.getSimpleName}-${Random.alphanumeric.take(5).mkString}"
  }

  protected def createTestDB() = {
    if (useExistingDB) {
      val db = getOrCreateDatabase()
      println(s"Using existing database ${db.getId()}")
      db
    } else {
      val dbId = generateDBName()
      val db = client
        .createDatabase(dbId)
        .asScala
        .map { r =>
          client.getDatabase(dbId)
        }
        .block(10.seconds)
      dbsToDelete += db
      println(s"Created database ${dbId}")
      db
    }
  }

  private def getOrCreateDatabase(): CosmosAsyncDatabase = {
    client
      .createDatabaseIfNotExists(storeConfig.db)
      .asScala
      .flatMap(r => SMono.just(client.getDatabase(r.getProperties.getId)))
      .block()
  }

  protected def querySpec(id: String): SqlQuerySpec = {
    val paramList = new ArrayList[SqlParameter]
    paramList.add(new SqlParameter("@id", id))
    new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id", paramList)
  }

  private def newDatabase = {
    val databaseDefinition = new Database
    databaseDefinition.setId(storeConfig.db)
    databaseDefinition
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (!useExistingDB) {
      dbsToDelete.foreach { db =>
        db.delete()
      }
    }
    client.close()
  }
}
