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
import akka.actor.ActorLogging
import akka.actor.ActorRef
import com.adobe.api.platform.runtime.mesos.MesosAgentStats
import org.apache.openwhisk.utils.NodeStatsUpdate
import scala.collection.concurrent.TrieMap

/**
 * Accounting basis for MesosContainerFactory primarily to cleanup mesos tasks during cleanup
 */
trait MesosData {
  //track tasks locally, so we can kill them during shutdown
  //tasks set needs thread safety since this factory is called directly by different actors
  val tasks: TrieMap[String, String] = TrieMap.empty
  val autoSubscribe: Boolean
  def getMesosClient(): ActorRef
  def addTask(taskId: String): Unit = { tasks.put(taskId, taskId) }
  def removeTask(taskId: String): Unit = { tasks.remove(taskId) }
}

class MesosClientSubscriber(isCluster: Boolean = true) extends Actor with ActorLogging {

  def receive = {
    case a: MesosAgentStats =>
      //map the mesos type to OW type
      val owStats = NodeStatsUpdate(a.stats.mapValues(a => org.apache.openwhisk.utils.NodeStats(a.mem, a.cpu, a.ports)))
      //publish to local subscribers (ContainerPool)
      context.system.eventStream.publish(owStats)
  }
}
