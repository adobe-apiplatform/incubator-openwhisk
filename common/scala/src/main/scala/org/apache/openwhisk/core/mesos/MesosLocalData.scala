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
import akka.actor.Props
import com.adobe.api.platform.runtime.mesos.LocalTaskStore
import com.adobe.api.platform.runtime.mesos.MesosAgentStats
import com.adobe.api.platform.runtime.mesos.MesosClient
import com.adobe.api.platform.runtime.mesos.SubscribeComplete
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.entity.UUID

class MesosLocalData(actorSystem: ActorSystem, mesosConfig: MesosConfig, logging: Logging) extends MesosData {
  override val autoSubscribe: Boolean = false

  val statsListener = actorSystem.actorOf(Props(new MesosLocalStatsListener))

  val mesosClientActor = actorSystem.actorOf(
    MesosClient.props(
      () => "whisk-containerfactory-" + UUID(),
      "whisk-containerfactory-framework",
      mesosConfig.masterUrl,
      mesosConfig.role,
      mesosConfig.timeouts.failover,
      taskStore = new LocalTaskStore,
      refuseSeconds = mesosConfig.offerRefuseDuration.toSeconds.toDouble,
      heartbeatMaxFailures = mesosConfig.heartbeatMaxFailures,
      autoSubscribe = false,
      listener = Some(statsListener)))

  class MesosLocalStatsListener extends Actor {
    override def receive: Receive = {
      case a: MesosAgentStats =>
        publishStats(context.system, a)
      case SubscribeComplete(f) =>
        logging.info(this, s"received framework id $f")
    }
  }
  override def getMesosClient(): ActorRef = mesosClientActor
}
