package org.apache.openwhisk.core.containerpool.mesos.test

import akka.actor.ActorRef
import akka.testkit.{TestActorRef, TestProbe}
import org.apache.openwhisk.core.containerpool.test.ContainerPoolTests
import org.apache.openwhisk.core.containerpool.{ContainerPoolConfig, _}
import org.apache.openwhisk.core.entity.{ByteSize, MemoryLimit}
import org.junit.runner.RunWith
import org.scalatest.OneInstancePerTest
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class MesosContainerPoolTests extends ContainerPoolTests with OneInstancePerTest { //for some reason the mocks are reused without OneInstancePerTest???
  it should "init prewarms only when InitPrewarms message is sent, when ContainerResourceManager.autoStartPrewarming is false" in {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()
    val resMgr = new ContainerResourceManager {
      override def canLaunch(size: ByteSize, poolMemory: Long, blackbox: Boolean): Boolean = true
    }

    val pool = system.actorOf(
      ContainerPool
        .props(
          factory,
          poolConfig(MemoryLimit.STD_MEMORY),
          feed.ref,
          List(PrewarmingConfig(1, exec, memoryLimit)),
          resMgr = Some(resMgr)))
    //prewarms are not started immediately
    containers(0).expectNoMessage(100.milliseconds)
    //prewarms must be started explicitly (e.g. by the ContainerResourceManager)
    pool ! InitPrewarms

    containers(0).expectMsg(Start(exec, memoryLimit)) // container0 was prewarmed
    containers(0).send(pool, NeedWork(preWarmedData(exec.kind)))
    pool ! runMessage
    containers(0).expectMsg(runMessage)

  }

  it should "limit the number of container prewarm starts" in {
    val (containers, factory) = testContainers(3)
    val feed = TestProbe()
    var reservations = 0;
    val resMgr = new ContainerResourceManager {
      override def addReservation(ref: ActorRef, byteSize: ByteSize, blackbox: Boolean): Unit = {
        reservations += 1
      }

      //limit reservations to 2 containers
      override def canLaunch(size: ByteSize, poolMemory: Long, blackbox: Boolean): Boolean = {

        if (reservations >= 2) {
          false
        } else {
          true
        }
      }
    }

    val pool = system.actorOf(
      ContainerPool
        .props(
          factory,
          poolConfig(MemoryLimit.STD_MEMORY),
          feed.ref,
          List(PrewarmingConfig(3, exec, memoryLimit)), //configure 3 prewarms, but only allow 2 to start
          resMgr = Some(resMgr)))
    //prewarms are not started immediately
    containers(0).expectNoMessage(100.milliseconds)

    //prewarms must be started explicitly (e.g. by the ContainerResourceManager)
    pool ! InitPrewarms

    containers(0).expectMsg(Start(exec, memoryLimit)) // container0 was prewarmed

    //second container should start
    containers(1).expectMsg(Start(exec, memoryLimit)) // container1 was prewarmed

    //third container should not start
    containers(2).expectNoMessage(100.milliseconds)

    //verify that resMgr.addReservation is called exactly twice
    reservations shouldBe 2
  }

  it should "release reservation on ContainerStarted" in {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()
    val resMgr = mock[ContainerResourceManager] //mock to capture invocations
    val pool = TestActorRef(
      ContainerPool
        .props(
          factory,
          poolConfig(MemoryLimit.STD_MEMORY),
          feed.ref,
          List(PrewarmingConfig(1, exec, memoryLimit)),
          resMgr = Some(resMgr)))
    (resMgr
      .canLaunch(_: ByteSize, _: Long, _: Boolean))
      .expects(memoryLimit, 0, false)
      .returning(true)
      .repeat(1)

    (resMgr.addReservation(_: ActorRef, _: ByteSize, _: Boolean)).expects(*, memoryLimit, *)

    (() => resMgr.activationStartLogMessage()).expects().returning("").repeat(2)

    //expect the container to become unused after second NeedWork
    (resMgr.releaseReservation(_: ActorRef)).expects(containers(0).ref).atLeastOnce()

    pool ! runMessageConcurrent
    pool ! runMessageConcurrent

    containers(0).expectMsg(runMessageConcurrent)
    containers(0).send(pool, NeedWork(warmedData(runMessageConcurrent)))

    containers(0).expectMsg(runMessageConcurrent)

    //ContainerStarted will cause resMgr.releaseReservation call
    containers(0).send(pool, ContainerStarted)
  }

  it should "release reservation on ContainerRemoved" in {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()
    val resMgr = mock[ContainerResourceManager] //mock to capture invocations
    val pool = TestActorRef(
      ContainerPool
        .props(
          factory,
          poolConfig(MemoryLimit.STD_MEMORY),
          feed.ref,
          List(PrewarmingConfig(1, exec, memoryLimit)),
          resMgr = Some(resMgr)))
    (resMgr
      .canLaunch(_: ByteSize, _: Long, _: Boolean))
      .expects(memoryLimit, 0, false)
      .returning(true)
      .repeat(1)

    (resMgr.addReservation(_: ActorRef, _: ByteSize, _: Boolean)).expects(*, memoryLimit, *)

    //expect releaseReservation after the failure
    (resMgr.releaseReservation(_: ActorRef)).expects(containers(0).ref).atLeastOnce()

    pool ! InitPrewarms //ContainerResourceManager would normally do this

    //fail the container
    containers(0).send(pool, ContainerRemoved(false))
  }

  it should "not block warm usage if cluster has no capacity" in {
    //stream.reset()
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()
    val resMgr = mock[ContainerResourceManager] //mock to capture invocations

    //NOTE: this test relies on ContainerPool counting prewarm pool against cluster usage
    val pool = TestActorRef(
      ContainerPool
        .props(
          factory,
          ContainerPoolConfig(MemoryLimit.STD_MEMORY * 2, 0.5, false, 5.minute, None, 100),
          feed.ref,
          List(PrewarmingConfig(2, exec, memoryLimit)),
          resMgr = Some(resMgr)))
    (resMgr
      .canLaunch(_: ByteSize, _: Long, _: Boolean))
      .expects(memoryLimit, 0, false)
      .returning(true)
      .twice()
    (resMgr
      .canLaunch(_: ByteSize, _: Long, _: Boolean))
      .expects(memoryLimit, 512, false)
      .returning(false)
      .repeat(3)

    (resMgr.addReservation(_: ActorRef, _: ByteSize, _: Boolean)).expects(*, memoryLimit, *).twice()

    (() => resMgr.activationStartLogMessage()).expects().returning("").repeat(3)

    (resMgr.releaseReservation(_: ActorRef)).expects(containers(0).ref).twice()
    (resMgr.releaseReservation(_: ActorRef)).expects(containers(1).ref).twice()

    pool ! InitPrewarms //ContainerResourceManager would normally do this

    //wait for prewarm to finish startup:
    containers(0).expectMsg(Start(exec, memoryLimit))
    containers(1).expectMsg(Start(exec, memoryLimit))
    //ContainerStarted will cause resMgr.releaseReservation call
    containers(0).send(pool, ContainerStarted)
    containers(1).send(pool, ContainerStarted)
    containers(0).send(pool, NeedWork(preWarmedData(exec.kind, memoryLimit)))
    containers(1).send(pool, NeedWork(preWarmedData(exec.kind, memoryLimit)))

    //running first and second activaiton MUST NOT cause resMgr.canLaunch() to be called again
    pool ! runMessage
    pool ! runMessage
    //running third activaiton MUST cause resMgr.canLaunch() to be called again, will be buffered since we cannot launch another
    pool ! runMessage
    containers(0).expectMsg(runMessage)
    containers(1).expectMsg(runMessage)

    containers(0).expectNoMessage(100.milliseconds)
    containers(1).expectNoMessage(100.milliseconds)

    //complete the first to cause the second to execute
    containers(0).send(pool, NeedWork(warmedData(runMessage)))
    containers(0).expectMsgPF() {
      // The `Some` assures, that it has been retried while the first action was still blocking the invoker.
      case Run(runMessage.action, runMessage.msg, Some(_)) => true
    }

  }
  it should "not block warm usage if cluster has no capacity with single prewarm" in {
    val (containers, factory) = testContainers(2)
    val feed = TestProbe()
    val resMgr2 = mock[ContainerResourceManager] //mock to capture invocations
    //NOTE: this test relies on ContainerPool counting prewarm pool against cluster usage
    val pool = TestActorRef(
      ContainerPool
        .props(
          factory,
          ContainerPoolConfig(MemoryLimit.STD_MEMORY, 0.5, false, 5.minute, None, 100),
          feed.ref,
          List(PrewarmingConfig(1, exec, memoryLimit)),
          resMgr = Some(resMgr2)))

    //the first prewarm launch
    (resMgr2
      .canLaunch(_: ByteSize, _: Long, _: Boolean))
      .expects(memoryLimit, 0, false)
      .returning(true)
      .once()
    //the replacement prewarm attempt
    //the cold start attempt
    (resMgr2
      .canLaunch(_: ByteSize, _: Long, _: Boolean))
      .expects(memoryLimit, 256, false)
      .returning(false)
      .repeat(2)

    (resMgr2.addReservation(_: ActorRef, _: ByteSize, _: Boolean)).expects(*, memoryLimit, *).once()

    (() => resMgr2.activationStartLogMessage()).expects().returning("").repeat(1)

    (resMgr2.releaseReservation(_: ActorRef)).expects(containers(0).ref).twice()

    pool ! InitPrewarms //ContainerResourceManager would normally do this

    //wait for prewarm to finish startup:
    containers(0).expectMsg(Start(exec, memoryLimit))
    //ContainerStarted will cause resMgr.releaseReservation call
    containers(0).send(pool, ContainerStarted)
    containers(0).send(pool, NeedWork(preWarmedData(exec.kind, memoryLimit)))

    //running first activaiton MUST NOT cause resMgr.canLaunch() to be called again
    pool ! runMessage
    //running second activaiton MUST cause resMgr.canLaunch() to be called again, will be buffered since we cannot launch another
    containers(0).expectMsg(runMessage)
    pool ! runMessage
    containers(0).expectNoMessage(100.milliseconds)

    containers(1).expectNoMessage(100.milliseconds)
  }
}
