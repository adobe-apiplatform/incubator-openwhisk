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

package whisk.core.limits

import common.TestHelpers
import common.TestUtils
import common.WskActorSystem
import common.WskProps
import common.WskTestHelpers
import common.rest.WskRestOperations
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import pureconfig.loadConfigOrThrow
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import spray.json.DefaultJsonProtocol._
import spray.json.JsObject
import spray.json._
import whisk.core.ConfigKeys
import whisk.core.entity.size._
import whisk.core.loadBalancer.ShardingContainerPoolBalancerConfig

@RunWith(classOf[JUnitRunner])
class ConcurrencyTests extends TestHelpers with WskTestHelpers with WskActorSystem {

  implicit val wskprops = WskProps()
  val wsk = new WskRestOperations

  //This action will receive concurrent activation requests, and not return results (for any of the pending activations)
  //until a specified number of activations (using param requestCount) are received at the same container.
  val concurrentAction = TestUtils.getTestActionFilename("concurrent.js")

  //NOTE: this test will only succeed if:
  // whisk.container-pool.akka-client = "true" (only the akka client properly handles concurrent requests to action containers)
  // whisk.container-factory.container-args.extra-args.env.0 = "__OW_ALLOW_CONCURRENT=true" (only action containers that tolerate concurrency can be tested - this enables concurrency in nodejs runtime)

  behavior of "Action concurrency limits"

  //This tests generates a concurrent load against the concurrent.js action with concurrency set to 5
  it should "execute activations concurrently when concurrency > 1 " in withAssetCleaner(wskprops) {
    (wp, assetHelper) =>
      val name = "TestConcurrentAction"
      assetHelper.withCleaner(wsk.action, name, confirmDelete = true) {
        val actionName = TestUtils.getTestActionFilename("concurrent.js")
        (action, _) =>
          //disable log collection since concurrent activation requires specialized log processing
          // (at action runtime and using specialized LogStore)
          action.create(name, Some(actionName), logsize = Some(0.bytes), concurrency = Some(5))
      }
      //warm the container (concurrent activations with no warmed container, will cause multiple containers to be used - so we force one to warm up)
      val run = wsk.action.invoke(name, Map("warm" -> 1.toJson), blocking = true)
      withActivation(wsk.activation, run) { response =>
        val logs = response.logs.get
        withClue(logs) { logs.size shouldBe 0 }

        response.response.status shouldBe "success"
        response.response.result shouldBe Some(JsObject("warm" -> 1.toJson))
      }

      //read configs to determine max concurrency support - currently based on busyThreshold
      val busyThreshold =
        loadConfigOrThrow[ShardingContainerPoolBalancerConfig](ConfigKeys.loadbalancer).invokerBusyThreshold

      //run maximum allowed concurrent actions via Futures
      val requestCount = busyThreshold
      println(s"executing $requestCount activations")
      val runs = (1 to requestCount).map { _ =>
        Future {
          //within the action, return (Promise.resolve) only after receiving $requestCount activations
          wsk.action.invoke(name, Map("requestCount" -> requestCount.toJson), blocking = true)
        }
      }

      //none of the actions will complete till the requestCount is reached
      Await.result(Future.sequence(runs), 30.seconds).foreach { run =>
        withActivation(wsk.activation, run) { response =>
          val logs = response.logs.get
          withClue(logs) { logs.size shouldBe 0 }
          response.response.status shouldBe "success"
          //expect exactly $requestCount activations concurrently
          response.response.result shouldBe Some(JsObject("msg" -> s"Received $requestCount activations.".toJson))
        }
      }
  }

  //This tests generates the same load against the same action as previous test, BUT with concurrency set to 1
  it should "execute activations sequentially when concurrency = 1 " in withAssetCleaner(wskprops) {
    (wp, assetHelper) =>
      val name = "TestNonConcurrentAction"
      assetHelper.withCleaner(wsk.action, name, confirmDelete = true) {
        val actionName = TestUtils.getTestActionFilename("concurrent.js")
        (action, _) =>
          //disable log collection since concurrent activation requires specialized log processing
          // (at action runtime and using specialized LogStore)
          action.create(name, Some(actionName), logsize = Some(0.bytes), concurrency = Some(1))
      }
      //warm the container (concurrent activations with no warmed container, will cause multiple containers to be used - so we force one to warm up)
      val run = wsk.action.invoke(name, Map("warm" -> 1.toJson), blocking = true)
      withActivation(wsk.activation, run) { response =>
        val logs = response.logs.get
        withClue(logs) { logs.size shouldBe 0 }

        response.response.status shouldBe "success"
        response.response.result shouldBe Some(JsObject("warm" -> 1.toJson))
      }

      //read configs to determine max concurrency support - currently based on busyThreshold
      val busyThreshold =
        loadConfigOrThrow[ShardingContainerPoolBalancerConfig](ConfigKeys.loadbalancer).invokerBusyThreshold

      //run maximum allowed concurrent actions via Futures
      val requestCount = busyThreshold
      println(s"executing $requestCount activations")
      val runs = (1 to requestCount).map { _ =>
        Future {
          //expect only 1 activation concurrently (within the 1 second timeout implemented in concurrent.js)
          wsk.action.invoke(name, Map("requestCount" -> 1.toJson), blocking = true)
        }
      }

      //none of the actions will complete till the requestCount is reached
      Await.result(Future.sequence(runs), 30.seconds).foreach { run =>
        withActivation(wsk.activation, run) { response =>
          val logs = response.logs.get
          withClue(logs) { logs.size shouldBe 0 }
          response.response.status shouldBe "success"
          //expect only 1 activation concurrently
          response.response.result shouldBe Some(JsObject("msg" -> s"Received 1 activations.".toJson))
        }
      }
  }

}
