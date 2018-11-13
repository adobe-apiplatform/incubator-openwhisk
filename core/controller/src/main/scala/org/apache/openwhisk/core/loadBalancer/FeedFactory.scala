package org.apache.openwhisk.core.loadBalancer
import akka.actor.ActorRef
import akka.actor.ActorRefFactory
import org.apache.openwhisk.core.connector.MessagingProvider
import scala.concurrent.Future

trait FeedFactory {
  def createFeed(actorRefFactory: ActorRefFactory,
                 messagingProvider: MessagingProvider,
                 messageHandler: Array[Byte] => Future[Unit]): ActorRef
}
