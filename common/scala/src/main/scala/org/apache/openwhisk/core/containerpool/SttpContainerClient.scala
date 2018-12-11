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

package org.apache.openwhisk.core.containerpool
import akka.actor.ActorSystem
import com.softwaremill.sttp._
import com.softwaremill.sttp.asynchttpclient.future.AsyncHttpClientFutureBackend
import java.net.ConnectException
import java.nio.charset.StandardCharsets
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.entity.ActivationResponse.ConnectionError
import org.apache.openwhisk.core.entity.ActivationResponse.ContainerHttpError
import org.apache.openwhisk.core.entity.ActivationResponse.ContainerResponse
import org.apache.openwhisk.core.entity.ActivationResponse.NoResponseReceived
import org.apache.openwhisk.core.entity.ActivationResponse.Timeout
import org.apache.openwhisk.core.entity.ByteSize
import org.apache.openwhisk.core.entity.size.SizeLong
import org.asynchttpclient.DefaultAsyncHttpClientConfig
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.language.higherKinds
import scala.util.Try
import scala.util.control.NonFatal
import spray.json.JsObject
import spray.json.JsValue
import spray.json._

/**
 * This HTTP client is used only in the invoker to communicate with the action container.
 * It allows to POST a JSON object and receive JSON object back; that is the
 * content type and the accept headers are both 'application/json.
 * This implementation uses the akka http host-level client API.
 * NOTE: Keepalive is disabled to prevent issues with paused containers
 *
 * @param hostname the host name
 * @param port the port
 * @param timeout the timeout in msecs to wait for a response
 * @param maxResponse the maximum size in bytes the connection will accept
 * @param queueSize once all connections are used, how big of queue to allow for additional requests
 * @param retryInterval duration between retries for TCP connection errors
 */
protected class SttpContainerClient(
  hostname: String,
  port: Int,
  timeout: FiniteDuration,
  maxResponse: ByteSize,
  retryInterval: FiniteDuration = 100.milliseconds)(implicit logging: Logging, as: ActorSystem)
    extends ContainerClient {

  val config = new DefaultAsyncHttpClientConfig.Builder()
    .setMaxConnections(100)
    .setMaxConnectionsPerHost(100)
    .setPooledConnectionIdleTimeout(100)
    //.setConnectionTtl(500)
    .setConnectTimeout(1000) //default 5000
    .setRequestTimeout(100) //default 60000
    .setPooledConnectionIdleTimeout(60000) //default 60000
    //.setIoThreadsCount()
    .build()

  //val ahc = Async
  implicit val executionContext = as.dispatcher
  implicit val backend = new RetryingBackend(AsyncHttpClientFutureBackend.usingConfig(config), timeout, retryInterval)
  val asTruncatedString: ResponseAs[String, Nothing] = asByteArray.map { b =>
    if (b.length <= maxResponse.toBytes) {
      new String(b, StandardCharsets.UTF_8)
    } else {
      //truncate the response
      new String(b.take(maxResponse.toBytes.toInt), StandardCharsets.UTF_8)
    }
  }

  implicit val jsValueSerializer: BodySerializer[JsObject] = { j: JsObject =>
    val serialized = j.toString()
    StringBody(serialized, "UTF-8", Some("application/json"))
  }
  def close() = {
    logging.info(this, "closing sttp")
    Future.successful(backend.close())
  }

  /**
   * Posts to hostname/endpoint the given JSON object.
   * Waits up to timeout before aborting on a good connection.
   * If the endpoint is not ready, retry up to timeout.
   * Every retry reduces the available timeout so that this method should not
   * wait longer than the total timeout (within a small slack allowance).
   *
   * @param endpoint the path the api call relative to hostname
   * @param body the JSON value to post (this is usually a JSON objecT)
   * @param retry whether or not to retry on connection failure
   * @return Left(Error Message) or Right(Status Code, Response as UTF-8 String)
   */
  def post(endpoint: String, body: JsValue, retry: Boolean)(
    implicit tid: TransactionId): Future[Either[ContainerHttpError, ContainerResponse]] = {

    val activationid = if (endpoint == "/run") body.asJsObject.fields("activation_id") else "init"

    logging.info(this, s"posting for ${activationid}")
    val start = System.currentTimeMillis()

    val uriStr = s"http://$hostname:$port$endpoint"

    sttp
      .post(uri"$uriStr") //TODO: should be able to use uri"http://$hostname:$port$endpoint" but sttp doesn't handle the tokens correctly in that case...
      .body(body.asJsObject)
      .readTimeout(timeout) //this sets both request timeout and read timeout, see https://github.com/softwaremill/sttp/pull/124
      .response(asTruncatedString)
      .send()
      .map { response =>
        logging.info(this, s"posting complete in ${System.currentTimeMillis() - start}ms for ${activationid}")
        response.contentLength match {
          case Some(contentLength) if response.code != StatusCodes.NoContent =>
            if (contentLength <= maxResponse.toBytes) {
              //take either right or left
              val body: String = response.body.fold[String](identity, identity)
              Right(ContainerResponse(response.code.intValue, body, None))
            } else {
              //in case response.body is Left(), it was not parsed so we will need to truncate it
              val body: String = response.body.fold[String](_.take(maxResponse.toBytes.toInt), identity)
              Right(ContainerResponse(response.code.intValue, body, Some(contentLength.B, maxResponse)))
            }
          case _ =>
            //handle missing Content-Length as NoResponseReceived
            Left(NoResponseReceived())
        }
      }
      .recover {
        case t: TimeoutException => Left(Timeout(t))
        case NonFatal(t)         => Left(ConnectionError(t))
        case t =>
          logging.error(this, s"failed request ${t}")
          Left(ConnectionError(t))
      }
  }
}

//see https://github.com/softwaremill/sttp/blob/4727232f2338e275b735d20b7d0598940690255c/docs/backends/custom.rst#example-retrying-backend-wrapper
class RetryingBackend[R[_], S](delegate: SttpBackend[Future, Nothing],
                               timeout: FiniteDuration,
                               retryInterval: FiniteDuration)(implicit as: ActorSystem, ec: ExecutionContext)
    extends SttpBackend[Future, Nothing] {
  def shouldRetry(request: Request[_, _], r: Either[Throwable, Response[_]], newTimeout: FiniteDuration): Boolean = {

    r match {
      case Left(_: TimeoutException) =>
        false //requestTimeout/readTimeout already elapsed
      case Left(_: ConnectException) => //java.net.ConnectException is thrown immediately, allow retries if within the limit
        newTimeout > Duration.Zero
      case _ =>
        false
    }

  }

  override def send[T](r: Request[T, Nothing]): Future[Response[T]] = {
    sendWithRetry(r, timeout, 0)
  }
  private def sendWithRetry[T](request: Request[T, Nothing],
                               timeout: FiniteDuration,
                               retries: Int): Future[Response[T]] = {
    val newTimeout = timeout - retryInterval
    val r = responseMonad.handleError(delegate.send(request)) {
      case t if shouldRetry(request, Left(t), newTimeout) =>
        akka.pattern.after(retryInterval, as.scheduler) {
          sendWithRetry(request, newTimeout, retries + 1)
        }
      case t =>
        Future.failed(new TimeoutException(t.getMessage))
    }
    responseMonad.flatMap(r) { resp =>
      responseMonad.unit(resp)
    }
  }
  override def close(): Unit = delegate.close()
  override def responseMonad: MonadError[Future] = delegate.responseMonad
}
object SttpContainerClient {

  /** A helper method to post one single request to a connection. Used for container tests. */
  def post(host: String, port: Int, endPoint: String, content: JsValue, timeout: FiniteDuration)(
    implicit logging: Logging,
    as: ActorSystem,
    ec: ExecutionContext,
    tid: TransactionId): (Int, Option[JsObject]) = {
    val connection = new AkkaContainerClient(host, port, timeout, 1.MB, 1)
    val response = executeRequest(connection, endPoint, content)
    val result = Await.result(response, timeout + 10.seconds) //additional timeout to complete futures
    connection.close()
    result
  }

  /** A helper method to post multiple concurrent requests to a single connection. Used for container tests. */
  def concurrentPost(host: String, port: Int, endPoint: String, contents: Seq[JsValue], timeout: FiniteDuration)(
    implicit logging: Logging,
    tid: TransactionId,
    as: ActorSystem,
    ec: ExecutionContext): Seq[(Int, Option[JsObject])] = {
    val connection = new AkkaContainerClient(host, port, timeout, 1.MB, 1)
    val futureResults = contents.map { executeRequest(connection, endPoint, _) }
    val results = Await.result(Future.sequence(futureResults), timeout + 10.seconds) //additional timeout to complete futures
    connection.close()
    results
  }

  private def executeRequest(connection: AkkaContainerClient, endpoint: String, content: JsValue)(
    implicit logging: Logging,
    as: ActorSystem,
    ec: ExecutionContext,
    tid: TransactionId): Future[(Int, Option[JsObject])] = {

    val res = connection
      .post(endpoint, content, true)
      .map({
        case Right(r)                   => (r.statusCode, Try(r.entity.parseJson.asJsObject).toOption)
        case Left(NoResponseReceived()) => throw new IllegalStateException("no response from container")
        case Left(Timeout(_))           => throw new java.util.concurrent.TimeoutException()
        case Left(ConnectionError(t: java.net.SocketTimeoutException)) =>
          throw new java.util.concurrent.TimeoutException()
        case Left(ConnectionError(t)) => throw new IllegalStateException(t.getMessage)
      })

    res
  }
}
