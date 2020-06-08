package org.apache.openwhisk.core.containerpool.mesos.test

import akka.actor.ActorRef
import akka.testkit.{TestActorRef, TestProbe}
import org.apache.openwhisk.core.containerpool.test.ContainerPoolTests
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.entity.{ByteSize, MemoryLimit}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class MesosContainerPoolTests extends ContainerPoolTests {
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
      .repeat(3)

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

}
