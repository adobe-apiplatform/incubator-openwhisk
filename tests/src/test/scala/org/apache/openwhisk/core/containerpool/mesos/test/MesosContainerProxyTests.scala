package org.apache.openwhisk.core.containerpool.mesos.test

import akka.actor.FSM.Transition
import akka.testkit.TestProbe
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.containerpool.test.ContainerProxyTests
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.entity.{ActivationResponse, InvokerInstanceId}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import spray.json.JsObject

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

@RunWith(classOf[JUnitRunner])
class MesosContainerProxyTests extends ContainerProxyTests {

  it should "send resend on ClusterResourceError" in {
    val container = new TestContainer {
      override def run(
        parameters: JsObject,
        environment: JsObject,
        timeout: FiniteDuration,
        concurrent: Int,
        reschedule: Boolean = false)(implicit transid: TransactionId): Future[(Interval, ActivationResponse)] = {

        throw ClusterResourceError(256.MB)
      }
    }
    val factory = createFactory(Future.successful(container))
    val acker = createAcker()
    val store = createStore
    val parentProbe = TestProbe()
    val machine =
      childActorOf(
        ContainerProxy
          .props(
            factory,
            acker,
            store,
            createCollector(),
            InvokerInstanceId(0, userMemory = defaultUserMemory),
            poolConfig,
            healthchecksConfig(),
            pauseGrace = pauseGrace))
    registerCallback(machine)
    //run(machine, Uninitialized) // first run an activation
    val runMsg = Run(action, message)
    machine ! runMsg
    expectMsg(Transition(machine, Uninitialized, Running))
    expectMsg(ContainerStarted)
    expectMsg(ContainerRemoved(false))

    expectMsg(runMsg)

    awaitAssert {
      factory.calls should have size 1
      container.runCount shouldBe 0
      container.suspendCount shouldBe 0
      container.resumeCount shouldBe 0
      container.destroyCount shouldBe 0
    }
  }
}
