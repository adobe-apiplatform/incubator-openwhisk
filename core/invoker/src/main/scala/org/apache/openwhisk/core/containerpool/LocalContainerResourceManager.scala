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
import akka.actor.ActorRef
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.entity.ByteSize

class LocalContainerResourceManager(implicit logging: Logging, tid: TransactionId) extends ContainerResourceManager {

  override def activationStartLogMessage(): String = ""
  override def rescheduleLogMessage(): String = ""
  override def updateUnused(unused: Map[ActorRef, ContainerData]): Unit = {}
  override def allowMoreStarts(config: ContainerPoolConfig): Boolean = true
  override def addReservation(ref: ActorRef, byteSize: ByteSize): Unit = {}
  override def releaseReservation(ref: ActorRef): Unit = {}
  override def requestSpace(size: ByteSize): Unit = {}
  override def canLaunch(size: ByteSize, poolMemory: Long, poolConfig: ContainerPoolConfig): Boolean =
    poolMemory + size.toMB <= poolConfig.userMemory.toMB
}
