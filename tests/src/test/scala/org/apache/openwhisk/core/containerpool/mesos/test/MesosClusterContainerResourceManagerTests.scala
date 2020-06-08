/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, "Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, "software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, "either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.containerpool.mesos.test

import akka.actor.{ActorRef, ActorSystem, Address}
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Replicator.{Update, WriteLocal}
import akka.cluster.ddata._
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.apache.openwhisk.common.AkkaLogging
import org.apache.openwhisk.core.containerpool.{
  AkkaClusterContainerResourceManager,
  ContainerResourceManagerConfig,
  Reservation
}
import org.apache.openwhisk.core.entity.InvokerInstanceId
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.utils.{NodeStats, NodeStatsUpdate}
import org.junit.runner.RunWith
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration._

object MesosClusterContainerResourceManagerTests {
  // Define your test specific configuration here
  val config =
    """
    akka {
      actor.provider = "cluster"
    }
    """
}
@RunWith(classOf[JUnitRunner])
class MesosClusterContainerResourceManagerTests
    extends TestKit(
      ActorSystem(
        "AkkaClusterContainerResourceManager",
        ConfigFactory.parseString(MesosClusterContainerResourceManagerTests.config)))
    with FlatSpecLike
    with Matchers {
  val stats = Map(
    "10.250.196.243" -> NodeStats(2244.0, 31.966, 983),
    "10.250.199.139" -> NodeStats(1732.0, 31.962, 981),
    "10.250.193.252" -> NodeStats(1348.0, 31.896, 948),
    "10.250.200.229" -> NodeStats(2244.0, 31.966, 983),
    "10.250.203.222" -> NodeStats(2244.0, 31.966, 983),
    "10.250.197.149" -> NodeStats(1988.0, 31.964, 982),
    "10.250.203.38" -> NodeStats(1988.0, 31.964, 982),
    "10.250.203.173" -> NodeStats(1988.0, 31.964, 982),
    "10.250.196.214" -> NodeStats(2500.0, 31.968, 984),
    "10.250.198.135" -> NodeStats(2500.0, 31.968, 984),
    "10.250.193.30" -> NodeStats(68.0, 31.948, 974),
    "10.250.197.61" -> NodeStats(2500.0, 31.968, 984),
    "10.250.192.161" -> NodeStats(2500.0, 31.968, 984),
    "10.250.201.242" -> NodeStats(2500.0, 31.968, 984),
    "10.250.196.98" -> NodeStats(2500.0, 31.968, 984),
    "10.250.193.51" -> NodeStats(1220.0, 31.958, 979),
    "10.250.194.45" -> NodeStats(196.0, 31.954, 977),
    "10.250.199.226" -> NodeStats(2500.0, 31.968, 984),
    "10.250.200.209" -> NodeStats(2244.0, 31.966, 983),
    "10.250.194.96" -> NodeStats(2500.0, 31.968, 984),
    "10.250.201.214" -> NodeStats(1988.0, 31.964, 982),
    "10.250.192.217" -> NodeStats(1988.0, 31.964, 982),
    "10.250.200.19" -> NodeStats(2244.0, 31.966, 983),
    "10.250.197.203" -> NodeStats(2500.0, 31.968, 984),
    "10.250.193.122" -> NodeStats(1604.0, 31.616, 808),
    "10.250.203.34" -> NodeStats(196.0, 31.92, 960),
    "10.250.194.193" -> NodeStats(2500.0, 31.968, 984),
    "10.250.192.201" -> NodeStats(68.0, 31.918, 959),
    "10.250.200.158" -> NodeStats(1732.0, 31.962, 981),
    "10.250.196.52" -> NodeStats(1732.0, 31.962, 981))
  it should "reuse a warm container" in {

    AkkaClusterContainerResourceManager.clusterHasPotentialMemoryCapacity(stats, 256, Seq(256.MB)) shouldBe true
    AkkaClusterContainerResourceManager.clusterHasPotentialMemoryCapacity(stats, 4096, Seq(256.MB)) shouldBe false

  }
  it should "update nodestats async" in {
    val pool = TestProbe().testActor
    implicit val logging = new AkkaLogging(system.log)
    val resourceManagerConfig = ContainerResourceManagerConfig(false, false, 10.seconds, 10)
    val resMgr =
      new AkkaClusterContainerResourceManager(
        system,
        InvokerInstanceId(0, userMemory = 2048.MB),
        pool,
        resourceManagerConfig)

    val replicator = DistributedData(system).replicator
    val remoteAddr = SelfUniqueAddress(UniqueAddress(Address("tcp", "remoteSys"), 123l))
    val InvokerIdsKey = ORSetKey[Int]("invokerIds")

    implicit val myAddress = DistributedData(system).selfUniqueAddress

    //setup remote id 1
    replicator ! Update(InvokerIdsKey, ORSet.empty[Int], WriteLocal)(_ :+ (1))

    //public nodestats
    awaitAssert {
      system.eventStream.publish(NodeStatsUpdate(stats))
      resMgr.clusterActionHostStats shouldBe stats
    }

    //verify Changed event
    awaitAssert {
      //replicate remote reservations from id 1 (local id is 0)
      val reservations = Map[ActorRef, Reservation](TestProbe().testActor -> Reservation(4096.MB, true)).values.toList
      replicator ! Update(
        LWWRegisterKey[List[Reservation]]("reservation1"),
        LWWRegister[List[Reservation]](remoteAddr, List.empty),
        WriteLocal)(reg => reg.withValueOf(reservations))
      resMgr.remoteReservedSize shouldBe 4096
    }
    //publish to local subscribers (ContainerPool)
    awaitAssert {
      resMgr.remoteReservedSize shouldBe 4096
      resMgr.canLaunch(256.MB, 0, false) shouldBe true
      resMgr.canLaunch(256.MB, 0, true) shouldBe false
    }
    //    resMgr.canLaunch(256.MB, 0, poolConfig, false, false) shouldBe true
  }
}
