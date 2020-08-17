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

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

// Enable these tests to detect any leak errors.
@RunWith(classOf[JUnitRunner])
class CosmosDBArtifactStoreLeakTests extends CosmosDBArtifactStoreTests {

  //NOTE: for some reason the leak checking is not failing the test when run in afterAll()!
  // This test is the same as CosmosDBArtifactStoreTests, except it will fail due to leaks.
  override def afterEach(): Unit = {
    withClue("Recorded leak count should be zero") {
      RecordingLeakDetectorFactory.counter.cur shouldBe 0
    }
    //leak checks only fail properly if done first
    super.afterEach()
  }
}
