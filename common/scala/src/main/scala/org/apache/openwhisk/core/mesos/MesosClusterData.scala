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

package org.apache.openwhisk.core.mesos
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.MemberStatus
import akka.cluster.UniqueAddress
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.LWWRegister
import akka.cluster.ddata.LWWRegisterKey
import akka.cluster.ddata.ORSet
import akka.cluster.ddata.ORSetKey
import akka.cluster.ddata.Replicator.Changed
import akka.cluster.ddata.Replicator.Get
import akka.cluster.ddata.Replicator.GetFailure
import akka.cluster.ddata.Replicator.GetSuccess
import akka.cluster.ddata.Replicator.NotFound
import akka.cluster.ddata.Replicator.ReadLocal
import akka.cluster.ddata.Replicator.Subscribe
import akka.cluster.ddata.Replicator.Update
import akka.cluster.ddata.Replicator.WriteLocal
import akka.cluster.singleton.ClusterSingletonManager
import akka.cluster.singleton.ClusterSingletonManagerSettings
import akka.cluster.singleton.ClusterSingletonProxy
import akka.cluster.singleton.ClusterSingletonProxySettings
import akka.management.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.pattern.ask
import akka.util.Timeout
import com.adobe.api.platform.runtime.mesos.DistributedDataTaskStore
import com.adobe.api.platform.runtime.mesos.MesosAgentStats
import com.adobe.api.platform.runtime.mesos.MesosClient
import com.adobe.api.platform.runtime.mesos.SubscribeComplete
import java.time.Instant
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.entity.UUID
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.util.Try

//events
case class SetClusterFrameworkId(frameworkId: String)
case class SetClusterStats(stats: MesosAgentStats)
case class AddTask(taskId: String)
case class RemoveTask(taskId: String)
case class GetTasks(address: UniqueAddress)
case class CleanTasks(address: UniqueAddress)

//data
case class MemberTask(address: UniqueAddress, taskId: String)

class MesosClusterData(actorSystem: ActorSystem, mesosConfig: MesosConfig, logging: Logging) extends MesosData {
  override val autoSubscribe: Boolean = true
  var frameworkId: Option[String] = None
  implicit val cluster = Cluster(actorSystem)
  implicit val ec = actorSystem.dispatcher
  implicit val log = logging
  val statsTopic = "agentStats"
  val dataManager = actorSystem.actorOf(Props(new MesosClusterListener(this)))
  var mesosClientActor: Option[ActorRef] = None
  implicit val tid = TransactionId("mesoscluster")
  private val initPromise: Promise[ActorRef] = Promise()

  if (mesosConfig.useClusterBootstrap) {
    AkkaManagement(actorSystem).start()
    ClusterBootstrap(actorSystem).start()
  }

  if (cluster.selfMember.status == MemberStatus.Up) {
    completeInit()
  } else {
    logging.info(this, "waiting for cluster join...")
  }

  //singleton for cleaning tasks
  actorSystem.actorOf(
    ClusterSingletonManager
      .props(
        Props(new Actor {
          var cleanedAddresses
            : Map[UniqueAddress, Instant] = Map.empty //track addresses and expiration time (will prune periodically to avoid growing the map)
          case object PruneCleaned
          //periodically prune cleaned addresses
          context.system.scheduler.schedule(10.seconds, 300.seconds, self, PruneCleaned)

          override def receive: Receive = {
            case CleanTasks(address) =>
              //only clean once; but all nodes must notify, so we track which ones have been cleaned
              if (!cleanedAddresses.contains(address)) {
                cleanedAddresses = cleanedAddresses + (address -> Instant.now().plusSeconds(300))
                removeTasks(address)
              }
            case PruneCleaned =>
              val now = Instant.now()
              cleanedAddresses = cleanedAddresses.filter(_._2.isBefore(now))
          }
        }),
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(actorSystem)),
    name = "invokerCleanup")
  val cleaner = actorSystem.actorOf(
    ClusterSingletonProxy
      .props(singletonManagerPath = "/user/invokerCleanup", settings = ClusterSingletonProxySettings(actorSystem)),
    name = "invokerCleanupProxy")

  //every member listens for cluster events and sends cleanup messages to the singleton cleaner (delete only once)
  actorSystem.actorOf(Props(new Actor {
    override def preStart(): Unit = {
      cluster.subscribe(self, classOf[MemberEvent])
    }
    override def receive: Receive = {
      case MemberUp(member) =>
        if (cluster.selfUniqueAddress == member.uniqueAddress) {
          completeInit()
        }
      case MemberRemoved(member, _) =>
        cleaner ! CleanTasks(member.uniqueAddress)
      case UnreachableMember(member) =>
        cleaner ! CleanTasks(member.uniqueAddress)
      case m =>
        logging.debug(this, s"unknown message $m")
    }
  }))

  private def completeInit() = {
    val client = createClient()
    mesosClientActor = Some(client)
    initPromise.success(client)
  }

  private def removeTasks(addr: UniqueAddress) = {
    //if client was not initialized, we will not even try to delete tasks
    mesosClientActor.foreach { mesosClient =>
      dataManager
        .ask(GetTasks(addr))(Timeout(5.minutes))
        .mapTo[Set[MemberTask]]
        .map { tasks =>
          if (tasks.nonEmpty) {
            logging.info(this, s"deleting ${tasks.size} orphaned meesos tasks abandoned by invoker at ${addr}")
            tasks.foreach { t =>
              MesosContainerFactory
                .destroy(mesosClient, mesosConfig, this, t.taskId)(tid, logging, ec)
                .map(_ => dataManager ! RemoveTask(t.taskId))
            }
          }
        }
    }

  }
  def createClient(): ActorRef = {
    //this listener handles propagating MesosAgentState to NodeStatsUpdateEvent
    val statsListener = actorSystem.actorOf(Props(new MesosClusterStatsListener()))

    val clientProps = MesosClient.props(
      getOrInitFrameworkId _,
      "whisk-containerfactory-framework",
      mesosConfig.masterUrl,
      mesosConfig.role,
      mesosConfig.timeouts.failover,
      taskStore = new DistributedDataTaskStore(actorSystem), //must use DistributedTaskStore for cluster failover reconciliation
      refuseSeconds = mesosConfig.offerRefuseDuration.toSeconds.toDouble,
      heartbeatMaxFailures = mesosConfig.heartbeatMaxFailures,
      autoSubscribe = true, //must be true for cluster usage so when singleton client fails, new one will re-subscribe automatically
      listener = Some(statsListener))

    //create the MesosClient actor as a cluster singleton
    actorSystem.actorOf(
      ClusterSingletonManager
        .props(clientProps, terminationMessage = PoisonPill, settings = ClusterSingletonManagerSettings(actorSystem)),
      name = "mesosClientMaster")
    actorSystem.actorOf(
      ClusterSingletonProxy
        .props(singletonManagerPath = "/user/mesosClientMaster", settings = ClusterSingletonProxySettings(actorSystem)),
      name = "mesosClientProxy")
  }

  private def getOrInitFrameworkId(): String = {
    frameworkId.getOrElse {
      //create the framework id
      val newFrameworkId = "whisk-containerfactory-" + UUID()
      //save the framework id
      frameworkId = Some(newFrameworkId)
      logging.info(this, s"created new framework id $newFrameworkId")
      //replicate the frameworkd id
      dataManager ! SetClusterFrameworkId(newFrameworkId)
      newFrameworkId
    }
  }
  override def addTask(taskId: String): Unit = {
    super.addTask(taskId)
    dataManager ! AddTask(taskId)
  }
  override def removeTask(taskId: String): Unit = {
    super.removeTask(taskId)
    dataManager ! RemoveTask(taskId)
  }

  class MesosClusterStatsListener extends Actor {
    override def receive: Receive = {
      case a: MesosAgentStats =>
        //publish to cluster
        dataManager ! SetClusterStats(a)
      case SubscribeComplete(f) =>
        logging.info(this, s"received framework id $f")
    }
  }
  override def getMesosClient(): ActorRef = {
    mesosClientActor.getOrElse {
      val c = Try(Await.result(initPromise.future, mesosConfig.timeouts.subscribe))
        .map(client => client)
        .recover {
          case _: TimeoutException =>
            logging.error(this, "Mesos akka cluster init took too long. Will retry...")
            getMesosClient()
        }
      c.get
    }
  }
}

class MesosClusterListener(clusterData: MesosClusterData)(implicit logging: Logging) extends Actor {

  implicit val node = Cluster(context.system)
  val addr = node.selfUniqueAddress
  val replicator = DistributedData(context.system).replicator
  implicit val selfAddress = DistributedData(context.system).selfUniqueAddress

  val FrameworkIdKey = LWWRegisterKey[String]("mesosFrameworkId")
  val MemberTasksKey = ORSetKey[MemberTask]("mesosTasks")
  //cluster stats are only published by the singleton MesosClient, so we could use DistributedPubSub,
  //but we are already using DistributedData, so we will just use that
  val ClusterStatsKey = LWWRegisterKey[MesosAgentStats]("mesosNodeStats")

  //subscribe to changes on frameworkid and cluster stats
  replicator ! Subscribe(FrameworkIdKey, self)
  replicator ! Subscribe(ClusterStatsKey, self)
  override def receive: Receive = {
    //Replication events
    case SetClusterFrameworkId(frameworkId) =>
      replicator ! Update(FrameworkIdKey, LWWRegister[String](selfAddress, null), WriteLocal)(reg =>
        reg.withValueOf(frameworkId))
    case SetClusterStats(stats) =>
      replicator ! Update(
        ClusterStatsKey,
        LWWRegister[MesosAgentStats](selfAddress, MesosAgentStats(Map.empty)),
        WriteLocal)(reg => reg.withValueOf(stats))
    case AddTask(task) =>
      replicator ! Update(MemberTasksKey, ORSet.empty[MemberTask], WriteLocal)(_ :+ MemberTask(addr, task))
    case RemoveTask(task) =>
      replicator ! Update(MemberTasksKey, ORSet.empty[MemberTask], WriteLocal)(_.remove(MemberTask(addr, task)))
    //Get Handlers
    case GetTasks(address: UniqueAddress) =>
      replicator ! Get(MemberTasksKey, ReadLocal, Some((sender(), address)))
    case g @ GetSuccess(MemberTasksKey, Some((replyTo: ActorRef, address: UniqueAddress))) =>
      val value = g.get(MemberTasksKey).elements.filter(_.address == address)
      replyTo ! value
    case NotFound(key, _) =>
      logging.warn(this, s"$key not found")
    case f @ GetFailure(key, _) =>
      logging.warn(this, s"$key failure $f")
    //Change Handlers
    case c @ Changed(FrameworkIdKey) =>
      clusterData.frameworkId = Some(c.get(FrameworkIdKey).value)
    case c @ Changed(ClusterStatsKey) =>
      clusterData.publishStats(context.system, c.get(ClusterStatsKey).value)
  }
}
