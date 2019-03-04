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

package org.apache.openwhisk.core.containerpool
import java.time.Instant

import akka.actor.{ActorRef, ActorSystem}
import org.apache.openwhisk.common.{AkkaLogging, TransactionId}
import org.apache.openwhisk.core.containerpool.ContainerPool.memoryConsumptionOf
import org.apache.openwhisk.core.entity.ByteSize
import org.apache.openwhisk.utils.NodeStats
import scala.concurrent.duration._

import scala.collection.mutable.ListBuffer

trait PoolPolicy {

  def hasPoolSpaceFor[A](containers: Seq[ContainerData], memory: ByteSize)(implicit tid: TransactionId): Boolean

}

class MemoryPoolPolicy(poolConfig: ContainerPoolConfig) extends PoolPolicy {
  override def hasPoolSpaceFor[A](containers: Seq[ContainerData], memory: ByteSize)(
    implicit tid: TransactionId): Boolean = {
    memoryConsumptionOf(containers) + memory.toMB <= poolConfig.userMemory.toMB
  }

  private def memoryConsumptionOf(containers: Seq[ContainerData]) = containers.map(_.memoryLimit.toMB).sum
}

class ManagedClusterPolicy()(implicit system: ActorSystem) extends PoolPolicy {
  implicit val logging = new AkkaLogging(system.log)
  private var clusterReservations: Map[ActorRef, Long] = Map.empty

  var agentOfferHistory = Map.empty[String, NodeStats] //track the most recent offer stats per agent
  var lastlog = Instant.now()

  val logInterval = 2.seconds

  var logDeadline = logInterval.fromNow
  private var resourcesAvailable
    : Boolean = false //track whenever there is a switch from cluster resources being available to not being available

  override def hasPoolSpaceFor[A](containers: Seq[ContainerData], memory: ByteSize)(
    implicit tid: TransactionId): Boolean = {
    //make sure there is at least one offer > memory + buffer
    val canLaunch = hasPotentialMemoryCapacity(memory.toMB, clusterReservations.values.toList)
    //log only when changing value
    if (canLaunch != resourcesAvailable) {
      if (canLaunch) {
        logging.info(this, s"mesos can launch action with ${memory.toMB}MB reserved:${reservedSize}")
      } else {
        logging.warn(this, s"mesos cannot launch action with ${memory.toMB}MB reserved:${reservedSize}")
      }
    }
    resourcesAvailable = canLaunch
    canLaunch
  }

  /** Return true to indicate there is expectation that there is "room" to launch a task with these memory/cpu/ports specs */
  def hasPotentialMemoryCapacity(memory: Double, reserve: List[Long]): Boolean = {
    //copy AgentStats, then deduct pending tasks
    var availableOffers = agentOfferHistory.toList.sortBy(_._2.mem).toMap //sort by mem to match lowest value
    val inNeedReserved = ListBuffer.empty ++ reserve

    var unmatched = 0

    inNeedReserved.foreach { p =>
      //for each pending find an available offer that fits
      availableOffers.find(_._2.mem > p) match {
        case Some(o) =>
          availableOffers = availableOffers + (o._1 -> o._2.copy(mem = o._2.mem - p))
          inNeedReserved -= p
        case None => unmatched += 1
      }
    }
    val allowReserve = unmatched == 0 && availableOffers.exists(_._2.mem > memory)
    if (logDeadline.isOverdue() || allowReserve) {
      logDeadline = logInterval.fromNow
    }

    allowReserve
  }

  def reservedSize = clusterReservations.foldLeft[Long](0)(_ + _._2)
  def reservedStartCount = clusterReservations.count(_._2 >= 0)
  def reservedStopCount = clusterReservations.count(_._2 < 0)
}
