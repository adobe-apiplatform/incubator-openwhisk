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
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}
import com.azure.data.cosmos.{ConnectionMode, ConnectionPolicy => JConnectionPolicy}

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class CosmosDBConfigTests extends FlatSpec with Matchers {
  val globalConfig = ConfigFactory.defaultApplication()
  behavior of "CosmosDB Config"

  it should "match SDK defaults" in {
    val config = ConfigFactory.parseString(s"""
      | whisk.cosmosdb {
      |  endpoint = "http://localhost"
      |  key = foo
      |  db  = openwhisk
      | }
         """.stripMargin).withFallback(globalConfig)
    val cosmos = CosmosDBConfig(config, "WhiskAuth")

    //Cosmos SDK does not have equals defined so match them explicitly
    val policy = cosmos.connectionPolicy.asJava
    val defaultPolicy = JConnectionPolicy.defaultPolicy()
    policy.connectionMode shouldBe defaultPolicy.connectionMode
    policy.enableEndpointDiscovery shouldBe defaultPolicy.enableEndpointDiscovery
    policy.idleConnectionTimeoutInMillis shouldBe defaultPolicy.idleConnectionTimeoutInMillis
    policy.maxPoolSize shouldBe defaultPolicy.maxPoolSize
    policy.preferredLocations shouldBe defaultPolicy.preferredLocations
    policy.requestTimeoutInMillis shouldBe defaultPolicy.requestTimeoutInMillis
    policy.usingMultipleWriteLocations shouldBe defaultPolicy.usingMultipleWriteLocations

    val retryOpts = policy.retryOptions
    val defaultOpts = defaultPolicy.retryOptions

    retryOpts.maxRetryAttemptsOnThrottledRequests shouldBe defaultOpts.maxRetryAttemptsOnThrottledRequests
    retryOpts.maxRetryWaitTimeInSeconds shouldBe defaultOpts.maxRetryWaitTimeInSeconds
  }

  it should "work with generic config" in {
    val config = ConfigFactory.parseString(s"""
      | whisk.cosmosdb {
      |  endpoint = "http://localhost"
      |  key = foo
      |  db  = openwhisk
      | }
         """.stripMargin).withFallback(globalConfig)
    val cosmos = CosmosDBConfig(config, "WhiskAuth")
    cosmos.endpoint shouldBe "http://localhost"
    cosmos.key shouldBe "foo"
    cosmos.db shouldBe "openwhisk"
  }

  it should "work with extended config" in {
    val config = ConfigFactory.parseString(s"""
      | whisk.cosmosdb {
      |  endpoint = "http://localhost"
      |  key = foo
      |  db  = openwhisk
      |  connection-policy {
      |     max-pool-size = 42
      |  }
      | }
         """.stripMargin).withFallback(globalConfig)
    val cosmos = CosmosDBConfig(config, "WhiskAuth")
    cosmos.endpoint shouldBe "http://localhost"
    cosmos.key shouldBe "foo"
    cosmos.db shouldBe "openwhisk"

    cosmos.connectionPolicy.maxPoolSize shouldBe 42
    val policy = cosmos.connectionPolicy.asJava
    val defaultPolicy = JConnectionPolicy.defaultPolicy()
    policy.connectionMode shouldBe defaultPolicy.connectionMode
    policy.retryOptions.maxRetryAttemptsOnThrottledRequests shouldBe defaultPolicy.retryOptions.maxRetryAttemptsOnThrottledRequests
    policy.retryOptions.maxRetryWaitTimeInSeconds shouldBe defaultPolicy.retryOptions.maxRetryWaitTimeInSeconds
  }

  it should "work with specific extended config" in {
    val config = ConfigFactory.parseString(s"""
      | whisk.cosmosdb {
      |  endpoint = "http://localhost"
      |  key = foo
      |  db  = openwhisk
      |  connection-policy {
      |     max-pool-size = 42
      |     retry-options {
      |        max-retry-wait-time = 2 m
      |     }
      |  }
      |  collections {
      |     WhiskAuth = {
      |        connection-policy {
      |           using-multiple-write-locations = true
      |           preferred-locations = [a, b]
      |           connection-mode = DIRECT
      |        }
      |     }
      |  }
      | }
         """.stripMargin).withFallback(globalConfig)
    val cosmos = CosmosDBConfig(config, "WhiskAuth")
    cosmos.endpoint shouldBe "http://localhost"
    cosmos.key shouldBe "foo"
    cosmos.db shouldBe "openwhisk"

    val policy = cosmos.connectionPolicy.asJava
    policy.usingMultipleWriteLocations shouldBe true
    policy.maxPoolSize shouldBe 42
    policy.connectionMode shouldBe ConnectionMode.DIRECT
    policy.preferredLocations.asScala.toSeq should contain only ("a", "b")
    policy.retryOptions.maxRetryWaitTimeInSeconds shouldBe 120
  }
}
