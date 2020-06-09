package org.apache.openwhisk.core.containerpool.mesos.test

import akka.actor.FSM.Transition
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.containerpool.test.ContainerProxyTests
import org.apache.openwhisk.core.entity.InvokerInstanceId
import org.apache.openwhisk.core.entity.size._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
class MesosContainerProxyTests extends ContainerProxyTests {

  it should "send resend on ClusterResourceError" in {
    val factory = createFactory(Future.failed(ClusterResourceError(256.MB)))
    val acker = createAcker()
    val store = createStore
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
    // first run an activation
    val runMsg = Run(action, message)
    machine ! runMsg
    expectMsg(Transition(machine, Uninitialized, Running))
    expectMsg(ContainerRemoved(false))

    //run msg must be resent
    expectMsg(runMsg)

    awaitAssert {
      factory.calls should have size 1
    }
  }
}
