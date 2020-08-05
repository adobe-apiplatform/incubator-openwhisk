package org.apache.openwhisk.core.database.cosmosdb

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
