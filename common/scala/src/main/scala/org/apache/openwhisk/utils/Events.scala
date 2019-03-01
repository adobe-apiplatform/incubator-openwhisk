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

package org.apache.openwhisk.utils

import akka.actor.ActorRef
import akka.event.EventBus
import akka.event.LookupClassification
import java.time.Instant

case class NodeStats(mem: Double, cpu: Double, ports: Int, lastSeen: Instant)
abstract sealed trait OWEvent
case class NodeStatsUpdate(stats: Map[String, NodeStats])
final case class MsgEnvelope(topic: OWEvent, payload: Any)

case object NodeStatsUpdateEvent extends OWEvent

/**
 * Publishes the payload of the MsgEnvelope when the topic of the
 * MsgEnvelope equals the OWEvent specified when subscribing.
 */
object Events extends EventBus with LookupClassification {
  type Event = MsgEnvelope
  type Classifier = OWEvent
  type Subscriber = ActorRef

  // is used for extracting the classifier from the incoming events
  override protected def classify(event: Event): Classifier = event.topic

  // will be invoked for each event for all subscribers which registered themselves
  // for the event’s classifier
  override protected def publish(event: Event, subscriber: Subscriber): Unit = {
    subscriber ! event.payload
  }

  // must define a full order over the subscribers, expressed as expected from
  // `java.lang.Comparable.compare`
  override protected def compareSubscribers(a: Subscriber, b: Subscriber): Int =
    a.compareTo(b)

  // determines the initial size of the index data structure
  // used internally (i.e. the expected number of different classifiers)
  override protected def mapSize: Int = 8

}
