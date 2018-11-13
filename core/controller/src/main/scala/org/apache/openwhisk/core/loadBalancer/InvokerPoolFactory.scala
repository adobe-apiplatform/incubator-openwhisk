package org.apache.openwhisk.core.loadBalancer
import akka.actor.ActorRef
import akka.actor.ActorRefFactory
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.connector.MessageProducer
import org.apache.openwhisk.core.connector.MessagingProvider
import org.apache.openwhisk.core.entity.InvokerInstanceId
import scala.concurrent.Future

trait InvokerPoolFactory {
  def createInvokerPool(
    actorRefFactory: ActorRefFactory,
    messagingProvider: MessagingProvider,
    messagingProducer: MessageProducer,
    sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[RecordMetadata],
    monitor: Option[ActorRef]): ActorRef
}
