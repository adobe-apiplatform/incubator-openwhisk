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
import akka.Done
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.CoordinatedShutdown
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.LWWRegister
import akka.cluster.ddata.LWWRegisterKey
import akka.cluster.ddata.ORSet
import akka.cluster.ddata.ORSetKey
import akka.cluster.ddata.Replicator.Changed
import akka.cluster.ddata.Replicator.Subscribe
import akka.cluster.ddata.Replicator.Unsubscribe
import akka.cluster.ddata.Replicator.Update
import akka.cluster.ddata.Replicator.UpdateFailure
import akka.cluster.ddata.Replicator.UpdateSuccess
import akka.cluster.ddata.Replicator.WriteLocal
import akka.management.scaladsl.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import java.time.Instant
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.LoggingMarkers
import org.apache.openwhisk.common.MetricEmitter
import org.apache.openwhisk.core.entity.ByteSize
import org.apache.openwhisk.core.entity.InvokerInstanceId
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.utils.NodeStats
import org.apache.openwhisk.utils.NodeStatsUpdate
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

class AkkaClusterContainerResourceManager(
  system: ActorSystem,
  instanceId: InvokerInstanceId,
  poolActor: ActorRef,
  resourceManagerConfig: ContainerResourceManagerConfig)(implicit logging: Logging)
    extends ContainerResourceManager {

  /** cluster state tracking */
  private var localReservations: Map[ActorRef, Reservation] = Map.empty //this pool's own reservations
  private var remoteReservations: Map[Int, List[Reservation]] = Map.empty //other pool's reservations
  var clusterActionHostStats = Map.empty[String, NodeStats] //track the most recent node stats per action host (host that is able to run action containers)
  var clusterActionHostsCount = 0
  var prewarmsInitialized = false
  val clusterPoolData: ActorRef =
    system.actorOf(
      Props(new ContainerPoolClusterData(instanceId, poolActor)),
      ContainerPoolClusterData.clusterPoolActorName(instanceId.toInt))

  //invoker keys
  val InvokerIdsKey = ORSetKey[Int]("invokerIds")
  //my keys
  val myId = instanceId.toInt
  val myReservationsKey = LWWRegisterKey[List[Reservation]]("reservation" + myId)
  //remote keys
  var reservationKeys: immutable.Map[Int, LWWRegisterKey[List[Reservation]]] = Map.empty

  //cachedValues
  var idMap: immutable.Set[Int] = Set.empty

  override def activationStartLogMessage(): String =
    s"node stats ${clusterActionHostStats} reserved ${localReservations.size} (of max ${resourceManagerConfig.clusterManagedResourceMaxStarts}) containers ${reservedSize}MB"

  override def rescheduleLogMessage() = {
    s"reservations: ${localReservations.size}"
  }

  def reservedSize = localReservations.values.map(_.size.toMB).sum
  def remoteReservedSize = remoteReservations.values.map(_.map(_.size.toMB).sum).sum
  private var clusterResourcesAvailable
    : Boolean = false //track to log whenever there is a switch from cluster resources being available to not being available

  def canLaunch(memory: ByteSize, poolMemory: Long, blackbox: Boolean): Boolean = {

    if (allowMoreStarts()) {
      //only consider blackbox/nonblackbox reservations to match this invoker's usage
      val localRes = localReservations.filter(_._2.blackbox == blackbox).values.map(_.size) //active local reservations
      val remoteRes = remoteReservations.values.toList.flatten.filter(_.blackbox == blackbox).map(_.size) //remote/stale reservations

      //TODO: consider potential to fit each reservation, then required memory for this action.
      val allRes = localRes ++ remoteRes
      //make sure there is at least one node with unreserved mem > memory
      val canLaunch = AkkaClusterContainerResourceManager.clusterHasPotentialMemoryCapacity(
        clusterActionHostStats,
        memory.toMB,
        allRes) //consider all reservations blocking till they are removed during NodeStatsUpdate
      //log only when changing value
      if (canLaunch != clusterResourcesAvailable) {
        if (canLaunch) {
          logging.info(
            this,
            s"cluster can launch action with ${memory.toMB}MB local reserved:${reservedSize} remote reserved:${remoteReservedSize}")
        } else {
          logging.warn(
            this,
            s"cluster cannot launch action with ${memory.toMB}MB local reserved:${reservedSize} remote reserved:${remoteReservedSize}")
        }
      }
      clusterResourcesAvailable = canLaunch
      canLaunch
    } else {
      logging.info(this, "throttling container starts")
      false
    }
  }

  /** reservation adjustments */
  override def addReservation(ref: ActorRef, size: ByteSize, blackbox: Boolean): Unit = {
    localReservations = localReservations + (ref -> Reservation(size, blackbox))
  }
  override def releaseReservation(ref: ActorRef): Unit = {
    localReservations = localReservations - ref
  }
  def allowMoreStarts(): Boolean =
    localReservations.size < resourceManagerConfig.clusterManagedResourceMaxStarts //only positive reservations affect ability to start

  class ContainerPoolClusterData(instanceId: InvokerInstanceId, containerPool: ActorRef) extends Actor {
    //it is possible to use cluster managed resources, but not run the invoker in the cluster
    //when not using cluster boostrapping, you need to set akka seed node configs
    if (resourceManagerConfig.useClusterBootstrap) {
      AkkaManagement(context.system).start()
      ClusterBootstrap(context.system).start()
    }
    implicit val cluster = Cluster(system)
    implicit val ec = context.dispatcher
    val replicator = DistributedData(system).replicator
    implicit val myAddress = DistributedData(system).selfUniqueAddress

    //subscribe to invoker ids changes (need to setup additional keys based on each invoker arriving)
    replicator ! Subscribe(InvokerIdsKey, self)
    //add this invoker to ids list
    replicator ! Update(InvokerIdsKey, ORSet.empty[Int], WriteLocal)(_ :+ (myId))

    logging.info(this, "subscribing to NodeStats updates")
    system.eventStream.subscribe(self, classOf[NodeStatsUpdate])

    //track the recent updates, so that we only send updates after changes (since this is done periodically, not on each data change)
    var lastReservations: List[Reservation] = List.empty
    var lastStats: Map[String, NodeStats] = Map.empty

    //schedule updates
    context.system.scheduler.schedule(0.seconds, 1.seconds, self, UpdateData)

    CoordinatedShutdown(context.system)
      .addTask(CoordinatedShutdown.PhaseBeforeClusterShutdown, "akkaClusterContainerResourceManagerCleanup") { () =>
        cleanup()
        Future.successful(Done)
      }
    private def cleanup() = {
      //remove this invoker from ids list
      logging.info(this, s"stopping invoker ${myId}")
      replicator ! Update(InvokerIdsKey, ORSet.empty[Int], WriteLocal)(_.remove(myId))

    }
    override def receive: Receive = {
      case UpdateData =>
        //update this invokers reservations seen by other invokers
        val reservations = localReservations.values.toList
        if (lastReservations != reservations) {
          lastReservations = reservations
          logging.info(
            this,
            s"invoker ${myId} (self) has ${reservations.size} reservations (${reservations.map(_.size.toMB).sum}MB)")
          replicator ! Update(myReservationsKey, LWWRegister[List[Reservation]](myAddress, List.empty), WriteLocal)(
            reg => reg.withValueOf(reservations))
        }
      case UpdateSuccess => //nothing (normal behavior)
      case f: UpdateFailure[_] => //log the failure
        logging.error(this, s"failed to update replicated data: $f")
      case NodeStatsUpdate(stats) =>
        logging.info(
          this,
          s"received node stats ${stats} local reservations:${localReservations.size} (${reservedSize}MB) remote reservations: ${remoteReservations
            .map(_._2.size)
            .sum} (${remoteReservedSize}MB)")
        clusterActionHostStats = stats
        if (stats.nonEmpty) {
          MetricEmitter.emitGaugeMetric(LoggingMarkers.CLUSTER_RESOURCES_TOTAL_MEM, stats.values.map(_.mem).sum.toLong)
          MetricEmitter.emitGaugeMetric(LoggingMarkers.CLUSTER_RESOURCES_MAX_MEM, stats.values.maxBy(_.mem).mem.toLong)
        }
        MetricEmitter.emitGaugeMetric(LoggingMarkers.CLUSTER_RESOURCES_NODE_COUNT, stats.size)
        logging.info(this, s"metrics invoker ${myId} (self) has ${localReservations.size} reserved (${reservedSize}MB)")
        MetricEmitter.emitGaugeMetric(LoggingMarkers.CLUSTER_RESOURCES_RESERVED_COUNT, localReservations.size)
        MetricEmitter.emitGaugeMetric(
          LoggingMarkers.CLUSTER_RESOURCES_RESERVED_SIZE,
          localReservations.map(_._2.size.toMB).sum)

        if (stats.nonEmpty && !prewarmsInitialized) { //we assume that when stats are received, we should startup prewarm containers
          prewarmsInitialized = true
          logging.info(this, "initializing prewarmpool after stats recevied")
          //        initPrewarms()
          containerPool ! InitPrewarms
        }

        //only signal updates (to pool or replicator) in case things have changed
        if (lastStats != stats) {
          lastStats = stats
          containerPool ! ContainerRemoved //this is not actually removing a container, just signaling that we should process a buffered item
        }

      case c @ Changed(LWWRegisterKey(idStr)) =>
        val resRegex = "reservation(\\d+)".r
        val (id, res) = idStr match {
          case resRegex(remoteId) => remoteId.toInt -> true
        }
        if (id != myId) {

          if (res) {
            val idKey = LWWRegisterKey[List[Reservation]]("reservation" + id)
            val newValue = c.get(idKey).value
            logging.info(this, s"invoker ${id} has ${newValue.size} reservations (${newValue.map(_.size.toMB).sum}MB)")
            remoteReservations = remoteReservations + (id -> newValue)
          }
        }
      case c @ Changed(InvokerIdsKey) =>
        val newValue = c.get(InvokerIdsKey).elements
        val deleted = reservationKeys.keySet.diff(newValue)
        val added = newValue.diff(reservationKeys.keySet)
        added.foreach { id =>
          if (id != myId) { //skip my own id
            if (!reservationKeys.keySet.contains(id)) {
              val idKey = LWWRegisterKey[List[Reservation]]("reservation" + id)
              logging.info(this, s"adding invoker ${id} to resource tracking")
              reservationKeys = reservationKeys + (id -> idKey)
              replicator ! Subscribe(idKey, self)
            } else {
              logging.warn(this, s"invoker ${id} already tracked, will not add")
            }
          }
        }
        deleted.foreach { id =>
          if (reservationKeys.keySet.contains(id)) {
            val idKey = LWWRegisterKey[List[Reservation]]("reservation" + id)
            reservationKeys = reservationKeys - id
            replicator ! Unsubscribe(idKey, self)
            remoteReservations = remoteReservations + (id -> List.empty)
          } else {
            logging.warn(this, s"invoker ${id} not tracked, will not remove")
          }
        }
    }
  }
  def remove[A](pool: ListBuffer[(A, RemoteContainerRef)], memory: ByteSize): List[(A, RemoteContainerRef)] = {

    if (pool.isEmpty) {
      List.empty
    } else {

      val oldest = pool.minBy(_._2.lastUsed)
      if (memory > 0.B) { //&& memoryConsumptionOf(pool) >= memory.toMB //remove any amount possible
        // Remove the oldest container if:
        // - there is more memory required
        // - there are still containers that can be removed
        // - there are enough free containers that can be removed
        //val (ref, data) = freeContainers.minBy(_._2.lastUsed)
        // Catch exception if remaining memory will be negative
        val remainingMemory = Try(memory - oldest._2.size).getOrElse(0.B)
        List(oldest._1 -> oldest._2) ++ remove(pool - oldest, remainingMemory)
      } else {
        // If this is the first call: All containers are in use currently, or there is more memory needed than
        // containers can be removed.
        // Or, if this is one of the recursions: Enough containers are found to get the memory, that is
        // necessary. -> Abort recursion
        List.empty
      }
    }
  }
}
object AkkaClusterContainerResourceManager {

  /** Return true to indicate there is expectation that there is "room" to launch a task with these memory/cpu/ports specs */
  def clusterHasPotentialMemoryCapacity(stats: Map[String, NodeStats],
                                        memory: Double,
                                        reserve: Iterable[ByteSize]): Boolean = {
    //copy AgentStats, then deduct pending tasks
    var availableResources = stats.toList.sortBy(_._2.mem).toMap //sort by mem to match lowest value
    val inNeedReserved = ListBuffer.empty ++ reserve

    var unmatched = 0

    inNeedReserved.foreach { p =>
      //for each pending find an available offer that fits
      availableResources.find(_._2.mem > p.toMB) match {
        case Some(o) =>
          availableResources = availableResources + (o._1 -> o._2.copy(mem = o._2.mem - p.toMB))
          inNeedReserved -= p
        case None => unmatched += 1
      }
    }
    unmatched == 0 && availableResources.exists(_._2.mem > memory)
  }
}

/**
 * Reservation indicates resources allocated to container, but possibly not launched yet. May be negative for container stop.
 * Pending: Allocated from this point of view, but not yet started/stopped by cluster manager.
 * Scheduled: Started/Stopped by cluster manager, but not yet reflected in NodeStats, so must still be considered when allocating resources.
 * */
case class Reservation(size: ByteSize, blackbox: Boolean)
case class RemoteContainerRef(size: ByteSize, lastUsed: Instant, containerAddress: ContainerAddress)
case object UpdateData

object ContainerPoolClusterData {
  def clusterPoolActorName(instanceId: Int): String = "containerPoolCluster" + instanceId.toInt
}
