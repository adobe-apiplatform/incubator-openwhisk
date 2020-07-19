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

package org.apache.openwhisk.core.database.cosmosdb
import com.azure.cosmos.implementation.apachecommons.lang.StringUtils
import com.azure.cosmos.implementation.{Constants, DataType, HashIndex, IndexKind, RangeIndex, Index => JIndex}
import com.azure.cosmos.models.{
  ExcludedPath => JExcludedPath,
  IncludedPath => JIncludedPath,
  IndexingPolicy => JIndexingPolicy
}

import scala.collection.JavaConverters._

/**
 * Scala based IndexingPolicy type which maps to java based IndexingPolicy. This is done for 2 reasons
 *
 *  - Simplify constructing policy instance
 *  - Enable custom comparison between existing policy and desired policy as policy instances
 *    obtained from CosmosDB have extra index type configured per included path. Hence the comparison
 *    needs to be customized
 *
 */
case class IndexingPolicy(includedPaths: Set[IncludedPath],
                          excludedPaths: Set[ExcludedPath] = Set(ExcludedPath("/*"))) {

  def asJava(): JIndexingPolicy = {
    val policy = new JIndexingPolicy()
    policy.setIncludedPaths(includedPaths.toList.map(_.asJava()).asJava)
    policy.setExcludedPaths(excludedPaths.toList.map(_.asJava()).asJava)
    policy
  }
}

object IndexingPolicy {
  def apply(policy: JIndexingPolicy): IndexingPolicy =
    IndexingPolicy(
      policy.getIncludedPaths.asScala.map(IncludedPath(_)).toSet,
      policy.getExcludedPaths.asScala.map(ExcludedPath(_)).toSet)

  /**
   * IndexingPolicy fetched from CosmosDB contains extra entries. So need to check
   * that at least what we expect is present
   */
  def isSame(expected: IndexingPolicy, current: IndexingPolicy): Boolean = {
    epaths(expected.excludedPaths) == epaths(current.excludedPaths) &&
    ipaths(expected.includedPaths) == ipaths(current.includedPaths)
  }

  private def ipaths(included: Set[IncludedPath]) = included.map(_.path)

  //CosmosDB seems to add _etag by default in excluded path. So explicitly ignore that in comparison
  private def epaths(excluded: Set[ExcludedPath]) = excluded.map(_.path).filterNot(_.contains("_etag"))
}

case class IncludedPath(path: String) {
  def asJava(): JIncludedPath = {
    val includedPath = new JIncludedPath(path)
    // IncludePath.setIndexes() is no longer available. Reference - https://github.com/Azure/azure-sdk-for-java/pull/11654
    includedPath.setPath(path)
    includedPath
  }
}

object IncludedPath {
  def apply(ip: JIncludedPath): IncludedPath = IncludedPath(ip.getPath)
}

case class ExcludedPath(path: String) {
  def asJava(): JExcludedPath = {
    val excludedPath = new JExcludedPath(path)
    excludedPath
  }
}

object ExcludedPath {
  def apply(ep: JExcludedPath): ExcludedPath = ExcludedPath(ep.getPath)
}

case class Index(kind: IndexKind, dataType: DataType, precision: Int) {
  def asJava(): JIndex = kind match {
    case IndexKind.HASH  => JIndex.hash(dataType, precision)
    case IndexKind.RANGE => JIndex.range(dataType, precision)
    case _               => throw new RuntimeException(s"Unsupported kind $kind")
  }
}

object Index {
  def apply(index: JIndex): Index = index match {
    case i: HashIndex =>
      Index(
        IndexKind.valueOf(StringUtils.upperCase(i.getString(Constants.Properties.INDEX_KIND))),
        i.getDataType,
        i.getPrecision)
    case i: RangeIndex =>
      Index(
        IndexKind.valueOf(StringUtils.upperCase(i.getString(Constants.Properties.INDEX_KIND))),
        i.getDataType,
        i.getPrecision)
    case _ => throw new RuntimeException(s"Unsupported kind $index")
  }
}
