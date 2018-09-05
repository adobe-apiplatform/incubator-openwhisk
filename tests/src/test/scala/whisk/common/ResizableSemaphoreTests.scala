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

package whisk.common

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.Matchers

@RunWith(classOf[JUnitRunner])
class ResizableSemaphoreTests extends FlatSpec with Matchers {
  behavior of "ResizableSemaphore"

  it should "not allow to acquire, force or release negative amounts of permits" in {
    val s = new ResizableSemaphore(2, 5)
    an[IllegalArgumentException] should be thrownBy s.tryAcquire(0)
    an[IllegalArgumentException] should be thrownBy s.tryAcquire(-1)

    an[IllegalArgumentException] should be thrownBy s.release(0)
    an[IllegalArgumentException] should be thrownBy s.release(-1)
  }

  it should "allow to acquire the defined amount of permits only" in {
    val s = new ResizableSemaphore(2, 5)
    s.tryAcquire() shouldBe true // 1 permit left
    s.tryAcquire() shouldBe true // 0 permits left
    s.tryAcquire() shouldBe false

    val s2 = new ForcibleSemaphore(4)
    s2.tryAcquire(5) shouldBe false // only 4 permits available
    s2.tryAcquire(3) shouldBe true // 1 permit left
    s2.tryAcquire(2) shouldBe false // only 1 permit available
    s2.tryAcquire() shouldBe true
  }

  it should "allow to release permits again" in {
    val s = new ForcibleSemaphore(2)
    s.tryAcquire() shouldBe true // 1 permit left
    s.tryAcquire() shouldBe true // 0 permits left
    s.tryAcquire() shouldBe false
    s.release() // 1 permit left
    s.tryAcquire() shouldBe true
    s.release(2) // 1 permit left
    s.tryAcquire(2) shouldBe true
  }

  it should "allow to resize permits - when factor of reductionSize is reached during release with reducing=true" in {
    val s = new ResizableSemaphore(2, 5)
    s.tryAcquire() shouldBe true // 1 permit left
    s.tryAcquire() shouldBe true // 0 permits left
    s.tryAcquire() shouldBe false
    s.release(4, reducing = true) shouldBe false // 4 permits left
    s.tryAcquire(4) shouldBe true
    s.tryAcquire() shouldBe false
    s.release(5, reducing = true) shouldBe true // 0 permits left (5 permits reduced to 0)
    s.tryAcquire() shouldBe false
    s.release(5, reducing = false) shouldBe false // 5 permits left
    s.tryAcquire() shouldBe true // 4 permits left
    s.tryAcquire() shouldBe true // 3 permits left
    s.tryAcquire() shouldBe true // 2 permits left
    s.tryAcquire() shouldBe true // 1 permits left
    s.tryAcquire() shouldBe true // 0 permits left
    s.tryAcquire() shouldBe false
    s.release(10, reducing = true) shouldBe true // 5 permits left (10 permits reduced to 5)
    s.tryAcquire() shouldBe true
    s.availablePermits shouldBe 4
  }

  it should "not give away more permits even under concurrent load" in {
    // 100 iterations of this test
    (0 until 100).foreach { _ =>
      val s = new ResizableSemaphore(32, 35)
      // try to acquire more permits than allowed in parallel
      val acquires = (0 until 64).par.map(_ => s.tryAcquire()).seq

      val result = Seq.fill(32)(true) ++ Seq.fill(32)(false)
      acquires should contain theSameElementsAs result
    }
  }
}
