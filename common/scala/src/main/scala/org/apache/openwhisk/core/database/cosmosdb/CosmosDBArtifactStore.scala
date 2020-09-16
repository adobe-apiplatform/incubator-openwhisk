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

import akka.actor.ActorSystem
import akka.event.Logging.InfoLevel
import akka.http.scaladsl.model.{ContentType, StatusCodes, Uri}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.azure.cosmos.{CosmosAsyncClient, CosmosException}
import com.azure.cosmos.implementation.Constants.Properties
import com.azure.cosmos.implementation.{Constants, QueryMetrics, RequestOptions}
import com.azure.cosmos.models.{
  CosmosItemRequestOptions,
  CosmosItemResponse,
  CosmosQueryRequestOptions,
  FeedResponse,
  ModelBridgeInternal,
  PartitionKey
}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import kamon.metric.MeasurementUnit
import org.apache.openwhisk.common.{LogMarkerToken, Logging, LoggingMarkers, MetricEmitter, Scheduler, TransactionId}
import org.apache.openwhisk.core.database.StoreUtils._
import org.apache.openwhisk.core.database._
import org.apache.openwhisk.core.database.cosmosdb.CosmosDBArtifactStoreProvider.DocumentClientRef
import org.apache.openwhisk.core.database.cosmosdb.CosmosDBConstants._
import org.apache.openwhisk.core.entity.Attachments.Attached
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.http.Messages
import reactor.core.scala.publisher.ScalaConverters._
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Success

class CosmosDBArtifactStore[DocumentAbstraction <: DocumentSerializer](protected val collName: String,
                                                                       protected val config: CosmosDBConfig,
                                                                       clientRef: DocumentClientRef,
                                                                       documentHandler: DocumentHandler,
                                                                       protected val viewMapper: CosmosDBViewMapper,
                                                                       val inliningConfig: InliningConfig,
                                                                       val attachmentStore: Option[AttachmentStore])(
  implicit system: ActorSystem,
  val logging: Logging,
  jsonFormat: RootJsonFormat[DocumentAbstraction],
  val materializer: ActorMaterializer,
  docReader: DocumentReader)
    extends ArtifactStore[DocumentAbstraction]
    with DefaultJsonProtocol
    with DocumentProvider
    with CosmosDBSupport
    with AttachmentSupport[DocumentAbstraction] {

  private val cosmosScheme = "cosmos"
  val attachmentScheme: String = attachmentStore.map(_.scheme).getOrElse(cosmosScheme)

  protected val client: CosmosAsyncClient = clientRef.get.client
  private[cosmosdb] val (database, collection) = initialize()

  private val putToken = createToken("put", read = false)
  private val delToken = createToken("del", read = false)
  private val getToken = createToken("get")
  private val queryToken = createToken("query")
  private val countToken = createToken("count")
  private val docSizeToken = createDocSizeToken()

  private val documentsSizeToken = createUsageToken("documentsSize", MeasurementUnit.information.kilobytes)
  private val indexSizeToken = createUsageToken("indexSize", MeasurementUnit.information.kilobytes)
  private val documentCountToken = createUsageToken("documentCount")

  private val softDeleteTTL = config.softDeleteTTL.map(_.toSeconds.toInt)

  private val clusterIdValue = config.clusterId.map(JsString(_))

  private val usageMetricRecorder = config.recordUsageFrequency.map { f =>
    Scheduler.scheduleWaitAtLeast(f, 10.seconds)(() => recordResourceUsage())
  }
  private val objectMapper = new ObjectMapper();

  def cosmosContainer() = collection

  override protected[core] implicit val executionContext: ExecutionContext = system.dispatcher

  override protected[database] def put(d: DocumentAbstraction)(implicit transid: TransactionId): Future[DocInfo] = {
    val asJson = d.toDocumentRecord
    val (doc, docSize) = toCosmosDoc(asJson)
    val id = doc.get(cid).asText()
    val et = if (doc.has(etag)) { doc.get(CosmosDBConstants.etag).asText() } else null
    val docinfoStr = s"id: $id, rev: ${et}"
    val start = transid.started(this, LoggingMarkers.DATABASE_SAVE, s"[PUT] '$collName' saving document: '$docinfoStr'")

    val o = if (isNewDocument(doc)) {
      collection.createItem(doc, new PartitionKey(id), newRequestOption(id))
    } else {
      collection.replaceItem(doc, id, new PartitionKey(id), matchRevOption(id, et))
    }

    val f = o.asScala.toFuture
      .recoverWith {
        case e: CosmosException if isConflict(e) && isNewDocument(doc) =>
          val docId = DocId(asJson.fields(_id).convertTo[String])
          //Fetch existing document and check if its deleted
          getRaw(docId).flatMap {
            case Some(js) =>
              if (isSoftDeleted(js)) {
                //Existing document is soft deleted. So can be replaced. Use the etag of document
                //and replace it with document we are trying to add
                val etag = js.fields(Properties.E_TAG).convertTo[String]
                collection
                  .replaceItem(doc, doc.get("id").asText(), new PartitionKey(id), matchRevOption(id, etag))
                  .asScala
                  .toFuture
              } else {
                //Trying to create a new document and found an existing
                //Document which is valid (not soft delete) then conflict is a valid outcome
                throw e
              }
            case None =>
              //Document not found. Should not happen unless someone else removed
              //Propagate existing exception
              throw e
          }
      }
      .transform(
        { r =>
          docSizeToken.histogram.record(docSize)
          transid.finished(
            this,
            start,
            s"[PUT] '$collName' completed document: '$docinfoStr', size=$docSize, ru=${r.getRequestCharge}${extraLogs(r)}",
            InfoLevel)
          collectMetrics(putToken, r.getRequestCharge)
          toDocInfo(doc, r.getETag)
        }, {
          case e: CosmosException if isConflict(e) =>
            transid.finished(this, start, s"[PUT] '$collName', document: '$docinfoStr'; conflict.")
            DocumentConflictException("conflict on 'put'")
          case e => e
        })

    reportFailure(f, start, failure => s"[PUT] '$collName' internal error, failure: '${failure.getMessage}'")
  }

  override protected[database] def del(doc: DocInfo)(implicit transid: TransactionId): Future[Boolean] = {
    checkDocHasRevision(doc)
    val start = transid.started(this, LoggingMarkers.DATABASE_DELETE, s"[DEL] '$collName' deleting document: '$doc'")
    val f = softDeleteTTL match {
      case Some(_) => softDelete(doc)
      case None    => hardDelete(doc)
    }
    val g = f
      .transform(
        { r =>
          transid.finished(this, start, s"[DEL] '$collName' completed document: '$doc'${extraLogs(r)}", InfoLevel)
          true
        }, {
          case e: CosmosException if isNotFound(e) =>
            transid.finished(this, start, s"[DEL] '$collName', document: '$doc'; not found.")
            NoDocumentException("not found on 'delete'")
          case e: CosmosException if isConflict(e) =>
            transid.finished(this, start, s"[DEL] '$collName', document: '$doc'; conflict.")
            DocumentConflictException("conflict on 'delete'")
          case e => e
        })

    reportFailure(
      g,
      start,
      failure => s"[DEL] '$collName' internal error, doc: '$doc', failure: '${failure.getMessage}'")
  }

  private def hardDelete(doc: DocInfo) = {
    val f = collection.deleteItem(escapeId(doc.id.id), partitionKey(doc), matchRevOption(doc)).asScala.toFuture
    f.foreach(r => collectMetrics(delToken, r.getRequestCharge))
    f
  }

  private def softDelete(doc: DocInfo)(implicit transid: TransactionId) = {
    for {
      js <- getAsWhiskJson(doc.id)
      r <- softDeletePut(doc, js)
    } yield r
  }

  private def softDeletePut(docInfo: DocInfo, js: JsObject)(implicit transid: TransactionId) = {
    val deletedJs = transform(js, Seq((deleted, Some(JsTrue))))
    val (doc, _) = toCosmosDoc(deletedJs)
    softDeleteTTL.foreach(doc.asInstanceOf[ObjectNode].put(Constants.Properties.TTL, _))
    val id = doc.get(cid).asText()
    val f =
      collection.replaceItem(doc, id, new PartitionKey(id), matchRevOption(docInfo)).asScala.toFuture
    f.foreach(r => collectMetrics(putToken, r.getRequestCharge))
    f
  }

  override protected[database] def get[A <: DocumentAbstraction](doc: DocInfo,
                                                                 attachmentHandler: Option[(A, Attached) => A] = None)(
    implicit transid: TransactionId,
    ma: Manifest[A]): Future[A] = {
    val start = transid.started(this, LoggingMarkers.DATABASE_GET, s"[GET] '$collName' finding document: '$doc'")

    require(doc != null, "doc undefined")
    val f =
      collection
        .readItem(escapeId(doc.id.id), partitionKey(doc), newRequestOption(doc.id), classOf[JsonNode])
        .asScala
        .toFuture
        .transform(
          { rr =>
            collectMetrics(getToken, rr.getRequestCharge)
            if (isSoftDeleted(rr.getItem)) {
              transid.finished(this, start, s"[GET] '$collName', document: '$doc'; not found.")
              // for compatibility
              throw NoDocumentException("not found on 'get'")
            } else {
              val (js, docSize) = getResultToWhiskJsonDoc(rr.getItem)
              transid
                .finished(
                  this,
                  start,
                  s"[GET] '$collName' completed: found document '$doc',size=$docSize, ru=${rr.getRequestCharge}${extraLogs(rr)}",
                  InfoLevel)
              deserialize[A, DocumentAbstraction](doc, js)
            }
          }, {
            case e: CosmosException if isNotFound(e) =>
              transid.finished(this, start, s"[GET] '$collName', document: '$doc'; not found.")
              // for compatibility
              throw NoDocumentException("not found on 'get'")
            case e => e
          })
        .recoverWith {
          case _: DeserializationException => throw DocumentUnreadable(Messages.corruptedEntity)
        }

    reportFailure(
      f,
      start,
      failure => s"[GET] '$collName' internal error, doc: '$doc', failure: '${failure.getMessage}'")

  }

  override protected[database] def get(id: DocId)(implicit transid: TransactionId): Future[Option[JsObject]] = {
    val start = transid.started(this, LoggingMarkers.DATABASE_GET, s"[GET_BY_ID] '$collName' finding document: '$id'")

    val f = collection
      .readItem(escapeId(id.id), partitionKey(id), newRequestOption(id), classOf[JsonNode])
      .asScala
      .toFuture
      .map { rr =>
        collectMetrics(getToken, rr.getRequestCharge)
        if (isSoftDeleted(rr.getItem)) {
          transid.finished(this, start, s"[GET_BY_ID] '$collName' completed: '$id' not found")
          None
        } else {
          val (js, _) = getResultToWhiskJsonDoc(rr.getItem)
          transid.finished(this, start, s"[GET_BY_ID] '$collName' completed: found document '$id'")
          Some(js)
        }
      }
      .recoverWith {
        case e: CosmosException if isNotFound(e) =>
          transid.finished(this, start, s"[GET_BY_ID] '$collName' completed: '$id' not found")
          Future.successful(None)
      }

    reportFailure(
      f,
      start,
      failure => s"[GET_BY_ID] '$collName' internal error, doc: '$id', failure: '${failure.getMessage}'")
  }

  /**
   * Method exposed for test cases to access the raw json returned by CosmosDB
   */
  private[cosmosdb] def getRaw(id: DocId): Future[Option[JsObject]] = {
    collection
      .readItem(escapeId(id.id), partitionKey(id), newRequestOption(id), classOf[JsonNode])
      .asScala
      .toFuture
      .map { rr =>
        val js = objectMapper.writeValueAsString(rr.getItem).parseJson.asJsObject
        Some(js)
      }
      .recoverWith {
        case e: CosmosException if isNotFound(e) => Future.successful(None)
      }
  }

  private def getAsWhiskJson(id: DocId): Future[JsObject] = {
    collection
      .readItem(escapeId(id.id), partitionKey(id), newRequestOption(id), classOf[JsonNode])
      .asScala
      .toFuture
      .map { rr =>
        val (js, _) = getResultToWhiskJsonDoc(rr.getItem)
        collectMetrics(getToken, rr.getRequestCharge)
        js
      }
  }

  override protected[core] def query(table: String,
                                     startKey: List[Any],
                                     endKey: List[Any],
                                     skip: Int,
                                     limit: Int,
                                     includeDocs: Boolean,
                                     descending: Boolean,
                                     reduce: Boolean,
                                     stale: StaleParameter)(implicit transid: TransactionId): Future[List[JsObject]] = {
    require(!(reduce && includeDocs), "reduce and includeDocs cannot both be true")
    require(!reduce, "Reduce scenario not supported") //TODO Investigate reduce
    require(skip >= 0, "skip should be non negative")
    require(limit >= 0, "limit should be non negative")
    documentHandler.checkIfTableSupported(table)

    val Array(ddoc, viewName) = table.split("/")

    val start = transid.started(this, LoggingMarkers.DATABASE_QUERY, s"[QUERY] '$collName' searching '$table'")
    val realIncludeDocs = includeDocs | documentHandler.shouldAlwaysIncludeDocs(ddoc, viewName)
    val realLimit = if (limit > 0) skip + limit else limit

    val querySpec = viewMapper.prepareQuery(ddoc, viewName, startKey, endKey, realLimit, realIncludeDocs, descending)

    val options = newQueryOptions()
    val queryMetrics = scala.collection.mutable.Buffer[QueryMetrics]()
    if (transid.meta.extraLogging) {
      options.setQueryMetricsEnabled(true)
      //unavailable: see https://github.com/Azure/azure-sdk-for-java/issues/12108
      //options.setEmitVerboseTracesInQuery(true)
    }

    def collectQueryMetrics(r: FeedResponse[JsonNode]): Unit = {
      collectMetrics(queryToken, r.getRequestCharge())
      queryMetrics.appendAll(ModelBridgeInternal.queryMetrics(r).values().asScala)
    }

    val f = Source
      .fromPublisher(collection.queryItems(querySpec, options, classOf[JsonNode]).byPage())
      .wireTap(collectQueryMetrics(_))
      .mapConcat(asVector)
      .drop(skip)
      .map(queryResultToWhiskJsonDoc)
      .map(js =>
        documentHandler
          .transformViewResult(ddoc, viewName, startKey, endKey, realIncludeDocs, js, CosmosDBArtifactStore.this))
      .mapAsync(1)(identity)
      .mapConcat(identity)
      .runWith(Sink.seq)
      .map(_.toList)
      .map(l => if (limit > 0) l.take(limit) else l)

    val g = f.andThen {
      case Success(queryResult) =>
        if (queryMetrics.nonEmpty) {
          val combinedMetrics = QueryMetrics.ZERO.add(queryMetrics.toSeq: _*)
          logging.debug(
            this,
            s"[QueryMetricsEnabled] Collection [$collName] - Query [${querySpec.getQueryText}].\nQueryMetrics\n[$combinedMetrics]")
        }
        val stats =
          viewMapper.recordQueryStats(ddoc, viewName, descending, querySpec.getParameters.asScala.toSeq, queryResult)
        val statsToLog = stats.map(s => " " + s).getOrElse("")
        transid.finished(
          this,
          start,
          s"[QUERY] '$collName' completed: matched ${queryResult.size}$statsToLog",
          InfoLevel)
    }
    reportFailure(g, start, failure => s"[QUERY] '$collName' internal error, failure: '${failure.getMessage}'")
  }

  override protected[core] def count(table: String,
                                     startKey: List[Any],
                                     endKey: List[Any],
                                     skip: Int,
                                     stale: StaleParameter)(implicit transid: TransactionId): Future[Long] = {
    require(skip >= 0, "skip should be non negative")
    val Array(ddoc, viewName) = table.split("/")

    val start = transid.started(this, LoggingMarkers.DATABASE_QUERY, s"[COUNT] '$collName' searching '$table")
    val querySpec = viewMapper.prepareCountQuery(ddoc, viewName, startKey, endKey)

    //For aggregates the value is in _aggregates fields
    val f = collection
      .queryItems(querySpec, newQueryOptions(), classOf[JsonNode])
      .byPage(1)
      .asScala
      .map { r =>
        val res = r.getResults.asScala
        val count = res.head.asInt()
        transid.finished(this, start, s"[COUNT] '$collName' completed: count $count")
        collectMetrics(countToken, r.getRequestCharge)
        if (count > skip) count - skip else 0L
      }
      .head
      .toFuture

    reportFailure(f, start, failure => s"[COUNT] '$collName' internal error, failure: '${failure.getMessage}'")
  }

  override protected[database] def putAndAttach[A <: DocumentAbstraction](
    doc: A,
    update: (A, Attached) => A,
    contentType: ContentType,
    docStream: Source[ByteString, _],
    oldAttachment: Option[Attached])(implicit transid: TransactionId): Future[(DocInfo, Attached)] = {
    attachmentStore match {
      case Some(as) =>
        attachToExternalStore(doc, update, contentType, docStream, oldAttachment, as)
      case None =>
        Future.failed(new IllegalArgumentException(
          s" '$cosmosScheme' is now not supported. You must configure an external AttachmentStore for storing attachments"))
    }
  }

  override protected[core] def readAttachment[T](doc: DocInfo, attached: Attached, sink: Sink[ByteString, Future[T]])(
    implicit transid: TransactionId): Future[T] = {
    val name = attached.attachmentName
    val attachmentUri = Uri(name)
    attachmentUri.scheme match {
      case AttachmentSupport.MemScheme =>
        memorySource(attachmentUri).runWith(sink)
      case s if s == cosmosScheme || attachmentUri.isRelative =>
        //relative case is for compatibility with earlier naming approach where attachment name would be like 'jarfile'
        //Compared to current approach of '<scheme>:<name>'
        Future.failed(new IllegalArgumentException(
          s" '$cosmosScheme' is now not supported. You must configure an external AttachmentStore for storing attachments"))
      case s if attachmentStore.isDefined && attachmentStore.get.scheme == s =>
        attachmentStore.get.readAttachment(doc.id, attachmentUri.path.toString, sink)
      case _ =>
        throw new IllegalArgumentException(s"Unknown attachment scheme in attachment uri $attachmentUri")
    }
  }

  override protected[core] def deleteAttachments[T](doc: DocInfo)(implicit transid: TransactionId): Future[Boolean] =
    attachmentStore
      .map(as => as.deleteAttachments(doc.id))
      .getOrElse(Future.successful(true)) // For CosmosDB it is expected that the entire document is deleted.

  override def shutdown(): Unit = {
    //Its async so a chance exist for next scheduled job to still trigger
    usageMetricRecorder.foreach(system.stop)
    attachmentStore.foreach(_.shutdown())
    clientRef.close()
  }

  def getResourceUsage(): Future[Option[CollectionResourceUsage]] = {
    val opts = new RequestOptions
    opts.setQuotaInfoEnabled(true)
    database
      .queryContainers(querySpec(collName))
      .byPage()
      .asScala
      .head
      .toFuture
      .map { rr =>
        CollectionResourceUsage(rr.getResponseHeaders.asScala.toMap)
      }
  }

  private def recordResourceUsage() = {
    getResourceUsage().map { o =>
      o.foreach { u =>
        u.documentsCount.foreach(documentCountToken.gauge.update(_))
        u.documentsSize.foreach(ds => documentsSizeToken.gauge.update(ds.toKB))
        u.indexSize.foreach(is => indexSizeToken.gauge.update(is.toKB))
        logging.info(this, s"Collection usage stats for [$collName] are ${u.asString}")
        u.indexingProgress.foreach { i =>
          if (i < 100) logging.info(this, s"Indexing for collection [$collName] is at $i%")
        }
      }
      o
    }
  }

  private def isNotFound[A <: DocumentAbstraction](e: CosmosException) =
    e.getStatusCode == StatusCodes.NotFound.intValue

  private def isConflict(e: CosmosException) = {
    e.getStatusCode == StatusCodes.Conflict.intValue || e.getStatusCode == StatusCodes.PreconditionFailed.intValue
  }

  private def toCosmosDoc(json: JsObject): (JsonNode, Int) = {
    val computedJs = documentHandler.computedFields(json)
    val computedOpt = if (computedJs.fields.nonEmpty) Some(computedJs) else None
    val docId = escapeId(json.fields(_id).convertTo[String])
    val fieldsToAdd =
      Seq(
        (cid, Some(JsString(docId))),
        (etag, json.fields.get(_rev)),
        (computed, computedOpt),
        (clusterId, clusterIdValue))
    val fieldsToRemove = Seq(_id, _rev)
    val mapped = transform(json, fieldsToAdd, fieldsToRemove)
    val jsonString = mapped.compactPrint
    val doc = objectMapper.readTree(jsonString)
    doc.asInstanceOf[ObjectNode].remove(Constants.Properties.TTL)
    (doc, jsonString.length)
  }

  private def queryResultToWhiskJsonDoc(doc: JsonNode): JsObject = {
    val docJson = objectMapper.writeValueAsString(doc).parseJson.asJsObject
    //If includeDocs is true then document json is to be used
    val js = if (doc.has(alias)) docJson.fields(alias).asJsObject else docJson
    val id = js.fields(cid).convertTo[String]
    toWhiskJsonDoc(js, id, None)
  }

  private def getResultToWhiskJsonDoc(doc: JsonNode): (JsObject, Int) = {
    checkDoc(doc)
    val jsString = objectMapper.writeValueAsString(doc) //doc.toJson
    val js = jsString.parseJson.asJsObject
    val whiskDoc = toWhiskJsonDoc(js, getId(doc), Some(JsString(getEtag(doc))))
    (whiskDoc, jsString.length)
  }

  private def getId(doc: JsonNode) = if (doc.has(cid)) doc.get(cid).asText() else null
  private def getEtag(doc: JsonNode) = if (doc.has(etag)) doc.get(etag).asText() else null
  private def toDocInfo[T <: JsonNode](doc: T, etag: String) = {
    DocInfo(DocId(unescapeId(doc.get(cid).asText())), DocRevision(etag))
  }

  private def matchRevOption(info: DocInfo): CosmosItemRequestOptions =
    matchRevOption(escapeId(info.id.id), info.rev.rev)

  private def matchRevOption(id: String, etag: String): CosmosItemRequestOptions = {
    val options = new CosmosItemRequestOptions()
    options.setIfMatchETag(etag)
    options
  }

  private def partitionKey(docInfo: DocInfo) = new PartitionKey(escapeId(docInfo.id.id))
  private def partitionKey(id: DocId) = new PartitionKey(escapeId(id.id))
  //Using DummyImplicit to allow overloading work with type erasure of DocId AnyVal
  private def newRequestOption(id: DocId)(implicit i: DummyImplicit): CosmosItemRequestOptions =
    newRequestOption(escapeId(id.id))

  private def newRequestOption(id: String) = {
    val options = new CosmosItemRequestOptions
    options
  }

  private def newQueryOptions() = {
    val options = new CosmosQueryRequestOptions()
    //default for queryMetricsEnabled as of azure 4.4 sdk is true, reset to false:
    options.setQueryMetricsEnabled(false)
    options
  }

  private def checkDoc[T <: JsonNode](doc: T): Unit = {
    require(getId(doc) != null, s"$doc does not have id field set")
    require(getEtag(doc) != null, s"$doc does not have etag field set")
  }

  private def collectMetrics(token: LogMarkerToken, charge: Double): Unit = {
    MetricEmitter.emitCounterMetric(token, Math.round(charge))
  }

  private def createToken(action: String, read: Boolean = true): LogMarkerToken = {
    val mode = if (read) "read" else "write"
    val tags = Map("action" -> action, "mode" -> mode, "collection" -> collName)
    if (TransactionId.metricsKamonTags) LogMarkerToken("cosmosdb", "ru", "used", tags = tags)(MeasurementUnit.none)
    else LogMarkerToken("cosmosdb", "ru", collName, Some(action))(MeasurementUnit.none)
  }

  private def createUsageToken(name: String, unit: MeasurementUnit = MeasurementUnit.none): LogMarkerToken = {
    val tags = Map("collection" -> collName)
    if (TransactionId.metricsKamonTags) LogMarkerToken("cosmosdb", name, "used", tags = tags)(unit)
    else LogMarkerToken("cosmosdb", name, collName)(unit)
  }

  private def createDocSizeToken(): LogMarkerToken = {
    val unit = MeasurementUnit.information.bytes
    val name = "doc"
    val tags = Map("collection" -> collName)
    if (TransactionId.metricsKamonTags) LogMarkerToken("cosmosdb", name, "size", tags = tags)(unit)
    else LogMarkerToken("cosmosdb", name, collName)(unit)
  }

  private def isSoftDeleted(doc: JsonNode) =
    doc.has(deleted) && doc.get(deleted).asBoolean()

  private def isSoftDeleted(js: JsObject) = js.fields.get(deleted).contains(JsTrue)

  private def isNewDocument(doc: JsonNode) = doc.get(etag) == null

  private def extraLogs(r: CosmosItemResponse[_])(implicit tid: TransactionId): String = {
    if (tid.meta.extraLogging) {
      " " + r.getDiagnostics.toString
    } else ""
  }
}
