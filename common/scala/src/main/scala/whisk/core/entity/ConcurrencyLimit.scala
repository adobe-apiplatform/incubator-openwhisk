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

package whisk.core.entity

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import spray.json._
import whisk.core.ConfigKeys
import pureconfig._

case class ConcurrencyLimitConfig(min: Int, max: Int, std: Int)

/**
 * ConcurrencyLimit encapsulates allowed concurrency in a single container for an action. The limit must be within a
 * permissible range (by default [1, 500]).
 *
 * It is a value type (hence == is .equals, immutable and cannot be assigned null).
 * The constructor is private so that argument requirements are checked and normalized
 * before creating a new instance.
 *
 * @param maxConcurrent the max number of concurrent activations in a single container
 */
protected[entity] class ConcurrencyLimit private (val maxConcurrent: Int) extends AnyVal

protected[core] object ConcurrencyLimit extends ArgNormalizer[ConcurrencyLimit] {
  private val concurrencyConfig = loadConfigOrThrow[ConcurrencyLimitConfig](ConfigKeys.concurrencyLimit)

  println(s"concurrency config ${concurrencyConfig}")
  protected[core] val minConcurrent: Int = concurrencyConfig.min
  protected[core] val maxConcurrent: Int = concurrencyConfig.max
  protected[core] val stdConcurrent: Int = concurrencyConfig.std

  /** Gets ConcurrencyLimit with default value */
  protected[core] def apply(): ConcurrencyLimit = ConcurrencyLimit(stdConcurrent)

  /**
   * Creates ConcurrencyLimit for limit, iff limit is within permissible range.
   *
   * @param concurrency the limit, must be within permissible range
   * @return ConcurrencyLimit with limit set
   * @throws IllegalArgumentException if limit does not conform to requirements
   */
  @throws[IllegalArgumentException]
  protected[core] def apply(concurrency: Int): ConcurrencyLimit = {
    require(concurrency >= minConcurrent, s"concurrency $concurrency below allowed threshold of $minConcurrent")
    require(concurrency <= maxConcurrent, s"concurrency $concurrency exceeds allowed threshold of $maxConcurrent")
    new ConcurrencyLimit(concurrency)
  }

  override protected[core] implicit val serdes = new RootJsonFormat[ConcurrencyLimit] {
    def write(m: ConcurrencyLimit) = JsNumber(m.maxConcurrent)

    def read(value: JsValue) = {
      Try {
        val JsNumber(c) = value
        require(c.isWhole(), "concurrency limit must be whole number")

        ConcurrencyLimit(c.toInt)
      } match {
        case Success(limit)                       => limit
        case Failure(e: IllegalArgumentException) => deserializationError(e.getMessage, e)
        case Failure(e: Throwable)                => deserializationError("concurrency limit malformed", e)
      }
    }
  }
}
