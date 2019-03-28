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

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Address
import akka.event.Logging.ErrorLevel
import akka.event.Logging.InfoLevel
import akka.pattern.AskTimeoutException
import akka.pattern.ask
import akka.util.Timeout
import com.adobe.api.platform.runtime.mesos.Constraint
import com.adobe.api.platform.runtime.mesos.DeleteTask
import com.adobe.api.platform.runtime.mesos.LIKE
import com.adobe.api.platform.runtime.mesos.Subscribe
import com.adobe.api.platform.runtime.mesos.SubscribeComplete
import com.adobe.api.platform.runtime.mesos.Teardown
import com.adobe.api.platform.runtime.mesos.UNLIKE
import java.time.Instant

import pureconfig.loadConfigOrThrow

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import org.apache.openwhisk.common.Counter
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.LoggingMarkers
import org.apache.openwhisk.common.MetricEmitter
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.entity.ByteSize
import org.apache.openwhisk.core.entity.ExecManifest
import org.apache.openwhisk.core.entity.InvokerInstanceId

/**
 * Configuration for mesos timeouts
 */
case class MesosTimeoutConfig(failover: FiniteDuration,
                              taskLaunch: FiniteDuration,
                              taskDelete: FiniteDuration,
                              subscribe: FiniteDuration,
                              teardown: FiniteDuration)

/**
 * Configuration for mesos action container health checks
 */
case class MesosContainerHealthCheckConfig(portIndex: Int,
                                           delay: FiniteDuration,
                                           interval: FiniteDuration,
                                           timeout: FiniteDuration,
                                           gracePeriod: FiniteDuration,
                                           maxConsecutiveFailures: Int)

/**
 * Configuration for MesosClient
 */
case class MesosConfig(masterUrl: String,
                       masterPublicUrl: Option[String],
                       role: String,
                       mesosLinkLogMessage: Boolean,
                       constraints: Seq[String],
                       constraintDelimiter: String,
                       blackboxConstraints: Seq[String],
                       teardownOnExit: Boolean,
                       healthCheck: Option[MesosContainerHealthCheckConfig],
                       offerRefuseDuration: FiniteDuration,
                       heartbeatMaxFailures: Int,
                       timeouts: MesosTimeoutConfig,
                       useClusterBootstrap: Boolean) {}

class MesosContainerFactory(config: WhiskConfig,
                            actorSystem: ActorSystem,
                            logging: Logging,
                            instance: InvokerInstanceId,
                            parameters: Map[String, Set[String]],
                            containerArgs: ContainerArgsConfig =
                              loadConfigOrThrow[ContainerArgsConfig](ConfigKeys.containerArgs),
                            runtimesRegistryConfig: RuntimesRegistryConfig =
                              loadConfigOrThrow[RuntimesRegistryConfig](ConfigKeys.runtimesRegistry),
                            mesosConfig: MesosConfig = loadConfigOrThrow[MesosConfig](ConfigKeys.mesos),
                            testMesosData: Option[MesosData] = None,
                            taskIdGenerator: (InvokerInstanceId) => String = MesosContainerFactory.taskIdGenerator _)
    extends ContainerFactory {

  implicit val as: ActorSystem = actorSystem
  implicit val ec: ExecutionContext = actorSystem.dispatcher
  implicit val tid: TransactionId = TransactionId("mesos")
  val mesosData = testMesosData.getOrElse {
    if (actorSystem.settings.config.getString("akka.actor.provider") == "cluster") {
      new MesosClusterData(actorSystem, mesosConfig, logging)
    } else {
      new MesosLocalData(actorSystem, mesosConfig, logging)
    }
  }

  private val mesosClientActor = mesosData.getMesosClient()

  /** Inits Mesos framework, in case we do not autosubscribe. */
  if (!mesosData.autoSubscribe) {
    subscribe()
  }

  /** Subscribes Mesos actor to mesos event stream; retry on timeout (which should be unusual). */
  private def subscribe(): Future[Unit] = {
    logging.info(this, s"subscribing to Mesos master at ${mesosConfig.masterUrl}")
    mesosClientActor
      .ask(Subscribe)(mesosConfig.timeouts.subscribe)
      .mapTo[SubscribeComplete]
      .map(complete => logging.info(this, s"subscribe completed successfully... $complete"))
      .recoverWith {
        case e =>
          logging.error(this, s"subscribe failed... $e}")
          subscribe()
      }
  }

  override def createContainer(tid: TransactionId,
                               name: String,
                               actionImage: ExecManifest.ImageName,
                               userProvidedImage: Boolean,
                               memory: ByteSize,
                               cpuShares: Int)(implicit config: WhiskConfig, logging: Logging): Future[Container] = {
    implicit val transid = tid
    val image = if (userProvidedImage) {
      actionImage.publicImageName
    } else {
      actionImage.localImageName(runtimesRegistryConfig.url)
    }
    val constraintStrings = if (userProvidedImage) {
      mesosConfig.blackboxConstraints
    } else {
      mesosConfig.constraints
    }
    val taskId = taskIdGenerator(instance)

    MesosTask.create(
      mesosClientActor,
      mesosConfig,
      mesosData,
      taskId,
      tid,
      image = image,
      userProvidedImage = userProvidedImage,
      memory = memory,
      cpuShares = cpuShares,
      environment = Map("__OW_API_HOST" -> config.wskApiHost),
      network = containerArgs.network,
      dnsServers = containerArgs.dnsServers,
      name = Some(name),
      //strip any "--" prefixes on parameters (should make this consistent everywhere else)
      parameters
        .map({ case (k, v) => if (k.startsWith("--")) (k.replaceFirst("--", ""), v) else (k, v) })
        ++ containerArgs.extraArgs,
      parseConstraints(constraintStrings))
  }

  /**
   * Validate that constraint strings are well formed, and ignore constraints with unknown operators
   * @param constraintStrings
   * @param logging
   * @return
   */
  def parseConstraints(constraintStrings: Seq[String])(implicit logging: Logging): Seq[Constraint] =
    constraintStrings.flatMap(cs => {
      val parts = cs.split(mesosConfig.constraintDelimiter)
      require(parts.length == 3, "constraint must be in the form <attribute><delimiter><operator><delimiter><value>")
      Seq(LIKE, UNLIKE).find(_.toString == parts(1)) match {
        case Some(o) => Some(Constraint(parts(0), o, parts(2)))
        case _ =>
          logging.warn(this, s"ignoring unsupported constraint operator ${parts(1)}")
          None
      }
    })

  override def init(): Unit = Unit

  /** Cleanups any remaining Containers; should block until complete; should ONLY be run at shutdown. */
  override def cleanup(): Unit = {
    if (mesosConfig.teardownOnExit) {
      val complete: Future[Any] = mesosClientActor.ask(Teardown)(mesosConfig.timeouts.teardown)
      Try(Await.result(complete, mesosConfig.timeouts.teardown))
        .map(_ => logging.info(this, "Mesos framework teardown completed."))
        .recover {
          case _: TimeoutException => logging.error(this, "Mesos framework teardown took too long.")
          case t: Throwable =>
            logging.error(this, s"Mesos framework teardown failed : $t}")
        }
    } else {
      val deleting = mesosData.tasks.map { taskId =>
        MesosContainerFactory.destroy(mesosClientActor, mesosConfig, mesosData, taskId._1)(
          TransactionId("mesos"),
          logging,
          ec)
      }
      logging.info(this, s"waiting on cleanup of ${deleting.size} mesos tasks")
      Try(Await.result(Future.sequence(deleting), 60.seconds))
        .map(_ => logging.info(this, "Mesos task cleanup completed."))
        .recover {
          case _: TimeoutException => logging.error(this, "Mesos task cleanup took too long.")
          case t: Throwable =>
            logging.error(this, s"Mesos task cleanup failed : $t}")
        }

    }
  }

  case class CleanTasks(address: Address)

}
object MesosContainerFactory {
  val counter = new Counter()
  val startTime = Instant.now.getEpochSecond
  private def taskIdGenerator(instance: InvokerInstanceId): String = {
    s"whisk-${instance.toInt}-${startTime}-${counter.next()}"
  }
  protected[mesos] def destroy(
    mesosClientActor: ActorRef,
    mesosConfig: MesosConfig,
    mesosData: MesosData,
    taskId: String)(implicit transid: TransactionId, logging: Logging, ec: ExecutionContext): Future[Unit] = {
    val taskDeleteTimeout = Timeout(mesosConfig.timeouts.taskDelete)

    val start = transid.started(
      this,
      LoggingMarkers.INVOKER_MESOS_CMD(MesosTask.KILL_CMD),
      s"killing mesos taskid $taskId (timeout: ${taskDeleteTimeout})",
      logLevel = InfoLevel)
    mesosData.removeTask(taskId)
    mesosClientActor
      .ask(DeleteTask(taskId))(taskDeleteTimeout)
      .andThen {
        case Success(_) => transid.finished(this, start, logLevel = InfoLevel)
        case Failure(ate: AskTimeoutException) =>
          transid.failed(this, start, s"task destroy timed out ${ate.getMessage}", ErrorLevel)
          MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_MESOS_CMD_TIMEOUT(MesosTask.KILL_CMD))
        case Failure(t) => transid.failed(this, start, s"task destroy failed ${t.getMessage}", ErrorLevel)
      }
      .map(_ => {})
  }
}

object MesosContainerFactoryProvider extends ContainerFactoryProvider {
  override def instance(actorSystem: ActorSystem,
                        logging: Logging,
                        config: WhiskConfig,
                        instance: InvokerInstanceId,
                        parameters: Map[String, Set[String]]): ContainerFactory =
    new MesosContainerFactory(config, actorSystem, logging, instance, parameters)
}
