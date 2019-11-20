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

package org.apache.openwhisk.core.containerpool.mesos.test

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Status.Failure
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import akka.testkit.TestProbe
import com.adobe.api.platform.runtime.mesos.Bridge
import com.adobe.api.platform.runtime.mesos.CapacityFailure
import com.adobe.api.platform.runtime.mesos.CommandDef
import com.adobe.api.platform.runtime.mesos.Constraint
import com.adobe.api.platform.runtime.mesos.DeleteTask
import com.adobe.api.platform.runtime.mesos.DockerPullFailure
import com.adobe.api.platform.runtime.mesos.DockerRunFailure
import com.adobe.api.platform.runtime.mesos.LIKE
import com.adobe.api.platform.runtime.mesos.Running
import com.adobe.api.platform.runtime.mesos.SubmitTask
import com.adobe.api.platform.runtime.mesos.Subscribe
import com.adobe.api.platform.runtime.mesos.SubscribeComplete
import com.adobe.api.platform.runtime.mesos.TaskDef
import com.adobe.api.platform.runtime.mesos.UNLIKE
import com.adobe.api.platform.runtime.mesos.User
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskState
import org.apache.mesos.v1.Protos.TaskStatus
import org.apache.openwhisk.common.AkkaLogging
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.containerpool.ClusterResourceError
import org.apache.openwhisk.core.containerpool.ContainerArgsConfig
import org.apache.openwhisk.core.containerpool.ContainerPoolConfig
import org.apache.openwhisk.core.containerpool.logging.DockerToActivationLogStore
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.core.entity.InvokerInstanceId
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.mesos.MesosConfig
import org.apache.openwhisk.core.mesos.MesosContainerFactory
import org.apache.openwhisk.core.mesos.MesosData
import org.apache.openwhisk.core.mesos.MesosTimeoutConfig
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FlatSpecLike
import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import scala.collection.immutable.Map
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import org.apache.openwhisk.utils.retry
import org.scalatest.BeforeAndAfterAll
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class MesosContainerFactoryTest
    extends TestKit(ActorSystem("MesosActorSystem"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with ScalaFutures
    with MockFactory {

  implicit val logging = new AkkaLogging(system.log)
  implicit val testConfig = PatienceConfig(1.minute)

  /** Awaits the given future, throws the exception enclosed in Failure. */
  def await[A](f: Future[A], timeout: FiniteDuration = 500.milliseconds) = Await.result[A](f, timeout)

  implicit val wskConfig =
    new WhiskConfig(Map(wskApiHostname -> "apihost") ++ wskApiHost)
  var count = 0
  var lastTaskId: String = null
  def testTaskId(instanceId: InvokerInstanceId) = {
    count += 1
    lastTaskId = "testTaskId" + count
    lastTaskId
  }

  // 80 slots, each 265MB
  val poolConfig =
    ContainerPoolConfig(21200.MB, 0.5, false, false, false, 10, 10.seconds)
  val actionMemory = 265.MB
  val mesosCpus = poolConfig.cpuShare(actionMemory) / 1024.0
  def mesosTestData(ref: Option[ActorRef] = None) = new MesosData {
    override val autoSubscribe: Boolean = false
    override def getMesosClient(): ActorRef = ref.getOrElse(testActor)
    override def addTask(taskId: String): Unit = {}
    override def removeTask(taskId: String): Unit = {}
  }

  val containerArgsConfig =
    new ContainerArgsConfig(
      "net1",
      Seq("dns1", "dns2"),
      Seq.empty,
      Seq.empty,
      Seq.empty,
      Map("extra1" -> Set("e1", "e2"), "extra2" -> Set("e3", "e4")))

  private var factory: MesosContainerFactory = _
  override def beforeEach() = {
    count = 0
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    Option(factory).foreach(_.close())
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
    retry({
      val threadNames = Thread.getAllStackTraces.asScala.keySet.map(_.getName)
      withClue(s"Threads related to  MesosActorSystem found to be active $threadNames") {
        assert(!threadNames.exists(_.startsWith("MesosActorSystem")))
      }
    }, 10, Some(1.second))
    super.afterAll()
  }

  val timeouts = MesosTimeoutConfig(1.seconds, 10.seconds, 10.seconds, 1.seconds, 1.seconds)
  val instanceId = InvokerInstanceId(123, Some("test"), Some("test"), 0.B)
  val mesosConfig =
    MesosConfig(
      "http://master:5050",
      None,
      "*",
      true,
      Seq.empty,
      " ",
      Seq.empty,
      true,
      None,
      1.seconds,
      2,
      timeouts,
      true,
      3)

  behavior of "MesosContainerFactory"

  it should "send Subscribe on init" in {
    val wskConfig = new WhiskConfig(Map.empty)
    val probe = TestProbe()
    factory = new MesosContainerFactory(
      wskConfig,
      system,
      logging,
      instanceId,
      Map("--arg1" -> Set("v1", "v2")),
      containerArgsConfig,
      mesosConfig = mesosConfig,
      testMesosData = Some(mesosTestData(Some(probe.ref))))

    probe.expectMsg(Subscribe)
    //emulate successful subscribe
    probe.reply(new SubscribeComplete("testid"))
  }

  it should "send SubmitTask (with constraints) on create" in {
    val mesosConfig = MesosConfig(
      "http://master:5050",
      None,
      "*",
      true,
      Seq("att1 LIKE v1", "att2 UNLIKE v2"),
      " ",
      Seq("bbatt1 LIKE v1", "bbatt2 UNLIKE v2"),
      true,
      None,
      1.seconds,
      2,
      timeouts,
      false,
      3)
    val probe = TestProbe()
    factory = new MesosContainerFactory(
      wskConfig,
      system,
      logging,
      instanceId,
      Map("--arg1" -> Set("v1", "v2"), "--arg2" -> Set("v3", "v4"), "other" -> Set("v5", "v6")),
      containerArgsConfig,
      mesosConfig = mesosConfig,
      testMesosData = Some(mesosTestData(Some(probe.ref))),
      taskIdGenerator = testTaskId _)

    probe.expectMsg(Subscribe)
    //emulate successful subscribe
    probe.reply(new SubscribeComplete("testid"))

    factory.createContainer(
      TransactionId.testing,
      "mesosContainer",
      ImageName("fakeImage"),
      false,
      actionMemory,
      poolConfig.cpuShare(actionMemory))

    probe.expectMsg(
      SubmitTask(TaskDef(
        lastTaskId,
        "mesosContainer",
        "fakeImage",
        mesosCpus,
        actionMemory.toMB.toInt,
        List(8080),
        None,
        false,
        User("net1"),
        Map(
          "arg1" -> Set("v1", "v2"),
          "arg2" -> Set("v3", "v4"),
          "other" -> Set("v5", "v6"),
          "dns" -> Set("dns1", "dns2"),
          "extra1" -> Set("e1", "e2"),
          "extra2" -> Set("e3", "e4")),
        Some(CommandDef(Map("__OW_API_HOST" -> wskConfig.wskApiHost))),
        Seq(Constraint("att1", LIKE, "v1"), Constraint("att2", UNLIKE, "v2")).toSet)))
  }

  it should "send DeleteTask on destroy" in {
    val probe = TestProbe()
    factory = new MesosContainerFactory(
      wskConfig,
      system,
      logging,
      instanceId,
      Map("--arg1" -> Set("v1", "v2"), "--arg2" -> Set("v3", "v4"), "other" -> Set("v5", "v6")),
      containerArgsConfig,
      mesosConfig = mesosConfig,
      testMesosData = Some(mesosTestData(Some(probe.ref))),
      taskIdGenerator = testTaskId _)

    probe.expectMsg(Subscribe)
    //emulate successful subscribe
    probe.reply(new SubscribeComplete("testid"))

    //create the container
    val c = factory.createContainer(
      TransactionId.testing,
      "mesosContainer",
      ImageName("fakeImage"),
      false,
      actionMemory,
      poolConfig.cpuShare(actionMemory))
    probe.expectMsg(
      SubmitTask(TaskDef(
        lastTaskId,
        "mesosContainer",
        "fakeImage",
        mesosCpus,
        actionMemory.toMB.toInt,
        List(8080),
        None,
        false,
        User("net1"),
        Map(
          "arg1" -> Set("v1", "v2"),
          "arg2" -> Set("v3", "v4"),
          "other" -> Set("v5", "v6"),
          "dns" -> Set("dns1", "dns2"),
          "extra1" -> Set("e1", "e2"),
          "extra2" -> Set("e3", "e4")),
        Some(CommandDef(Map("__OW_API_HOST" -> wskConfig.wskApiHost))))))

    //emulate successful task launch
    val taskId = TaskID.newBuilder().setValue(lastTaskId)

    probe.reply(
      Running(
        taskId.getValue,
        "testAgentID",
        TaskStatus.newBuilder().setTaskId(taskId).setState(TaskState.TASK_RUNNING).build(),
        "agenthost",
        Seq(30000)))
    //wait for container
    val container = await(c)

    //destroy the container
    implicit val tid = TransactionId.testing
    val deleted = container.destroy()
    probe.expectMsg(DeleteTask(lastTaskId))

    probe.reply(TaskStatus.newBuilder().setTaskId(taskId).setState(TaskState.TASK_KILLED).build())

    await(deleted)

  }

  it should "return static message for logs" in {
    val probe = TestProbe()
    factory = new MesosContainerFactory(
      wskConfig,
      system,
      logging,
      instanceId,
      Map("--arg1" -> Set("v1", "v2"), "--arg2" -> Set("v3", "v4"), "other" -> Set("v5", "v6")),
      new ContainerArgsConfig(
        "bridge",
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Map("extra1" -> Set("e1", "e2"), "extra2" -> Set("e3", "e4"))),
      mesosConfig = mesosConfig,
      testMesosData = Some(mesosTestData(Some(probe.ref))),
      taskIdGenerator = testTaskId _)

    probe.expectMsg(Subscribe)
    //emulate successful subscribe
    probe.reply(new SubscribeComplete("testid"))

    //create the container
    val c = factory.createContainer(
      TransactionId.testing,
      "mesosContainer",
      ImageName("fakeImage"),
      false,
      actionMemory,
      poolConfig.cpuShare(actionMemory))

    probe.expectMsg(
      SubmitTask(TaskDef(
        lastTaskId,
        "mesosContainer",
        "fakeImage",
        mesosCpus,
        actionMemory.toMB.toInt,
        List(8080),
        None,
        false,
        Bridge,
        Map(
          "arg1" -> Set("v1", "v2"),
          "arg2" -> Set("v3", "v4"),
          "other" -> Set("v5", "v6"),
          "extra1" -> Set("e1", "e2"),
          "extra2" -> Set("e3", "e4")),
        Some(CommandDef(Map("__OW_API_HOST" -> wskConfig.wskApiHost))))))

    //emulate successful task launch
    val taskId = TaskID.newBuilder().setValue(lastTaskId)

    probe.reply(
      Running(
        taskId.getValue,
        "testAgentID",
        TaskStatus.newBuilder().setTaskId(taskId).setState(TaskState.TASK_RUNNING).build(),
        "agenthost",
        Seq(30000)))
    //wait for container
    val container = await(c)

    implicit val tid = TransactionId.testing
    implicit val m = ActorMaterializer()
    val logs = container
      .logs(actionMemory, false)
      .via(DockerToActivationLogStore.toFormattedString)
      .runWith(Sink.seq)
    await(logs)(0) should endWith
    " stdout: Logs are not collected from Mesos containers currently. You can browse the logs for Mesos Task ID testTaskId using the mesos UI at http://master:5050"

  }

  it should "generate a ClusterResourceError in case of mesos resource failure" in {
    val probe = TestProbe()
    val factory =
      new MesosContainerFactory(
        wskConfig,
        system,
        logging,
        instanceId,
        Map("--arg1" -> Set("v1", "v2"), "--arg2" -> Set("v3", "v4"), "other" -> Set("v5", "v6")),
        containerArgsConfig,
        mesosConfig = mesosConfig,
        testMesosData = Some(mesosTestData(Some(probe.ref))),
        taskIdGenerator = testTaskId _)

    probe.expectMsg(Subscribe)
    //emulate successful subscribe
    probe.reply(new SubscribeComplete("testid"))

    //create the container
    val c = factory.createContainer(
      TransactionId.testing,
      "mesosContainer",
      ImageName("fakeImage"),
      false,
      actionMemory,
      poolConfig.cpuShare(actionMemory))
    probe.expectMsg(
      SubmitTask(TaskDef(
        lastTaskId,
        "mesosContainer",
        "fakeImage",
        mesosCpus,
        actionMemory.toMB.toInt,
        List(8080),
        None,
        false,
        User("net1"),
        Map(
          "arg1" -> Set("v1", "v2"),
          "arg2" -> Set("v3", "v4"),
          "other" -> Set("v5", "v6"),
          "dns" -> Set("dns1", "dns2"),
          "extra1" -> Set("e1", "e2"),
          "extra2" -> Set("e3", "e4")),
        Some(CommandDef(Map("__OW_API_HOST" -> wskConfig.wskApiHost))))))

    //emulate successful task launch
    val taskId = TaskID.newBuilder().setValue(lastTaskId)

    probe.reply(Failure(CapacityFailure(actionMemory.toMB, 1.0f, 1, List.empty)))

    ScalaFutures.whenReady(c.failed) { cr =>
      cr shouldBe ClusterResourceError(actionMemory)
    }
  }

  it should "retry on DockerRunFailure or DockerPullFailure" in {
    val probe = TestProbe()
    val mesosData = mock[MesosData]
    (() => mesosData.getMesosClient()).expects().returning(probe.ref)
    (mesosData.addTask(_: String)).expects("testTaskId1")
    (mesosData.removeTask(_: String)).expects("testTaskId1")
    (mesosData.addTask(_: String)).expects("testTaskId2")
    (mesosData.removeTask(_: String)).expects("testTaskId2")
    (mesosData.addTask(_: String)).expects("testTaskId3")
    (mesosData.removeTask(_: String)).expects("testTaskId3")
    (mesosData.addTask(_: String)).expects("testTaskId4")
    (mesosData.removeTask(_: String)).expects("testTaskId4")

    //    val mesosData = mesosTestData()
    val factory =
      new MesosContainerFactory(
        wskConfig,
        system,
        logging,
        instanceId,
        Map("--arg1" -> Set("v1", "v2"), "--arg2" -> Set("v3", "v4"), "other" -> Set("v5", "v6")),
        containerArgsConfig,
        mesosConfig = mesosConfig,
        //testMesosData = Some(mesosTestData(Some(probe.ref))),
        testMesosData = Some(mesosData),
        taskIdGenerator = testTaskId _)

    probe.expectMsg(Subscribe)
    //emulate successful subscribe
    probe.reply(new SubscribeComplete("testid"))

    //create the container
    val c = factory.createContainer(
      TransactionId.testing,
      "mesosContainer",
      ImageName("fakeImage"),
      false,
      actionMemory,
      poolConfig.cpuShare(actionMemory))
    probe.expectMsg(
      SubmitTask(TaskDef(
        "testTaskId1",
        "mesosContainer",
        "fakeImage",
        mesosCpus,
        actionMemory.toMB.toInt,
        List(8080),
        None,
        false,
        User("net1"),
        Map(
          "arg1" -> Set("v1", "v2"),
          "arg2" -> Set("v3", "v4"),
          "other" -> Set("v5", "v6"),
          "dns" -> Set("dns1", "dns2"),
          "extra1" -> Set("e1", "e2"),
          "extra2" -> Set("e3", "e4")),
        Some(CommandDef(Map("__OW_API_HOST" -> wskConfig.wskApiHost))))))

    //emulate successful task launch
    val taskId = TaskID.newBuilder().setValue(lastTaskId)

    probe.reply(Failure(DockerRunFailure("FAILING IN TEST1")))

    probe.expectMsg(
      SubmitTask(TaskDef(
        "testTaskId2",
        "mesosContainer",
        "fakeImage",
        mesosCpus,
        actionMemory.toMB.toInt,
        List(8080),
        None,
        false,
        User("net1"),
        Map(
          "arg1" -> Set("v1", "v2"),
          "arg2" -> Set("v3", "v4"),
          "other" -> Set("v5", "v6"),
          "dns" -> Set("dns1", "dns2"),
          "extra1" -> Set("e1", "e2"),
          "extra2" -> Set("e3", "e4")),
        Some(CommandDef(Map("__OW_API_HOST" -> wskConfig.wskApiHost))))))

    probe.reply(Failure(DockerRunFailure("FAILING IN TEST2")))

    probe.expectMsg(
      SubmitTask(TaskDef(
        "testTaskId3",
        "mesosContainer",
        "fakeImage",
        mesosCpus,
        actionMemory.toMB.toInt,
        List(8080),
        None,
        false,
        User("net1"),
        Map(
          "arg1" -> Set("v1", "v2"),
          "arg2" -> Set("v3", "v4"),
          "other" -> Set("v5", "v6"),
          "dns" -> Set("dns1", "dns2"),
          "extra1" -> Set("e1", "e2"),
          "extra2" -> Set("e3", "e4")),
        Some(CommandDef(Map("__OW_API_HOST" -> wskConfig.wskApiHost))))))

    probe.reply(Failure(DockerPullFailure("FAILING IN TEST3")))

    probe.expectMsg(
      SubmitTask(TaskDef(
        "testTaskId4",
        "mesosContainer",
        "fakeImage",
        mesosCpus,
        actionMemory.toMB.toInt,
        List(8080),
        None,
        false,
        User("net1"),
        Map(
          "arg1" -> Set("v1", "v2"),
          "arg2" -> Set("v3", "v4"),
          "other" -> Set("v5", "v6"),
          "dns" -> Set("dns1", "dns2"),
          "extra1" -> Set("e1", "e2"),
          "extra2" -> Set("e3", "e4")),
        Some(CommandDef(Map("__OW_API_HOST" -> wskConfig.wskApiHost))))))

    probe.reply(Failure(DockerRunFailure("FAILING IN TEST4")))

    whenReady(c.failed) { cr =>
      cr shouldBe DockerRunFailure("FAILING IN TEST4")
    }
  }
}
