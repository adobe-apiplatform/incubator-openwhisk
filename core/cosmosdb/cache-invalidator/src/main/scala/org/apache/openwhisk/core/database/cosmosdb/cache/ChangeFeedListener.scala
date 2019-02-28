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
package org.apache.openwhisk.core.database.cosmosdb.cache

import java.io.Closeable
import java.util

import com.microsoft.azure.documentdb.changefeedprocessor.{
  ChangeFeedEventHost,
  ChangeFeedHostOptions,
  ChangeFeedObserverCloseReason,
  ChangeFeedObserverContext,
  IChangeFeedObserver
}
import com.microsoft.azure.documentdb.{ChangeFeedOptions, Document}
import com.typesafe.config.Config
import org.apache.openwhisk.common.ExecutorCloser

import scala.collection.JavaConverters._

class ChangeFeedManager[A <: BaseObserver](collName: String, observerClazz: Class[A])(implicit config: Config)
    extends Closeable {
  private val listener = {
    val feedConfig = CacheInvalidatorConfig.getFeedConfig()
    val collInfo = CacheInvalidatorConfig.getCollectionInfo(collName)
    val leaseCollInfo = CacheInvalidatorConfig.getCollectionInfo(feedConfig.leaseCollection)
    new ChangeFeedListener(collInfo, leaseCollInfo, feedConfig, observerClazz)
  }

  override def close(): Unit = listener.close()
}

class ChangeFeedListener[A <: BaseObserver](collInfo: DocumentCollectionInfo,
                                            leaseCollInfo: DocumentCollectionInfo,
                                            feedConfig: FeedConfig,
                                            observerClazz: Class[A])
    extends Closeable {
  private val host = {
    val feedOpts = new ChangeFeedOptions
    feedOpts.setPageSize(100)

    val hostOpts = new ChangeFeedHostOptions
    //Using same lease collection across collection. To avoid collision
    //set prefix to coll name
    hostOpts.setLeasePrefix(collInfo.collectionName)

    //TODO Track the lag
    val host = new ChangeFeedEventHost(feedConfig.hostname, collInfo.asJava, leaseCollInfo.asJava, feedOpts, hostOpts)
    host.registerObserver(observerClazz)
    host
  }

  override def close(): Unit = ExecutorCloser(host.getExecutorService).close()
}

abstract class BaseObserver extends IChangeFeedObserver {
  override def open(context: ChangeFeedObserverContext): Unit = Unit
  override def close(context: ChangeFeedObserverContext, reason: ChangeFeedObserverCloseReason): Unit = Unit
  override def processChanges(context: ChangeFeedObserverContext, docs: util.List[Document]): Unit =
    docs.asScala.foreach(process)
  def process(doc: Document)
}
