package com.ruimo.recoeng

import javax.inject._
import play.api.Logger
import play.api.libs.json._
import play.api.libs.functional.syntax._
import org.joda.time.DateTime
import java.util.concurrent.atomic.AtomicLong
import com.ruimo.recoeng.json.Desc
import com.ruimo.recoeng.json.SortOrder
import com.ruimo.recoeng.json.ScoredItem
import com.ruimo.recoeng.json.JsonRequestPaging
import com.ruimo.recoeng.json.OnSalesJsonRequest
import com.ruimo.recoeng.json.RecommendByItemJsonRequest
import com.ruimo.recoeng.json.OnSalesJsonResponse
import com.ruimo.recoeng.json.JsonRequestHeader
import com.ruimo.recoeng.json.TransactionMode
import com.ruimo.recoeng.json.TransactionSalesMode
import com.ruimo.recoeng.json.SalesItem
import com.ruimo.recoeng.json.JsonResponseHeader
import com.ruimo.recoeng.json.RecommendByItemJsonResponse
import play.api._
import play.api.inject.Module
import play.api.inject.Binding

object SequenceNumber {
  private val seed = new AtomicLong
  def apply(): Long = seed.incrementAndGet
}

trait RecoEngApi {
  def onSales(
    requestTime: Long = System.currentTimeMillis,
    sequenceNumber: Long = SequenceNumber(),
    transactionMode: TransactionMode,
    transactionTime: Long,
    userCode: String,
    itemTable: Seq[SalesItem]
  ): JsResult[OnSalesJsonResponse]

  def recommendByItem(
    requestTime: Long = System.currentTimeMillis,
    sequenceNumber: Long = SequenceNumber(),
    salesItems: Seq[SalesItem],
    sort: SortOrder = Desc("score"),
    paging: JsonRequestPaging
  ): JsResult[RecommendByItemJsonResponse]
}

class RecoEngApiImpl @Inject() (
  jsonServer: JsonServer
) extends RecoEngApi {
  val logger = Logger(getClass)

  implicit val pagingWrites = Writes[JsonRequestPaging] { p =>
    Json.obj(
      "offset" -> Json.toJson(p.offset),
      "limit" -> Json.toJson(p.limit)
    )
  }

  implicit val requestHeaderWrites = Writes[JsonRequestHeader] { req =>
    Json.obj(
      "dateTime" -> Json.toJson(req.dateTimeInYyyyMmDdHhMmSs),
      "sequenceNumber" -> Json.toJson(req.sequenceNumber)
    )
  }

  implicit val salesItemWrites = Writes[SalesItem] { it =>
    Json.obj(
      "storeCode" -> Json.toJson(it.storeCode),
      "itemCode" -> Json.toJson(it.itemCode),
      "quantity" -> Json.toJson(it.quantity)
    )
  }

  implicit val onSalesJsonRequestWrites = Writes[OnSalesJsonRequest] { req =>
    Json.obj(
      "header" -> Json.toJson(req.header),
      "transactionMode" -> Json.toJson(req.mode),
      "dateTime" -> Json.toJson(req.tranDateInYyyyMmDdHhMmSs),
      "userCode" -> Json.toJson(req.userCode),
      "salesItems" -> Json.toJson(req.salesItems)
    )
  }

  implicit val recommendByItemJsonRequestWrites = Writes[RecommendByItemJsonRequest] { req =>
    Json.obj(
      "header" -> Json.toJson(req.header),
      "salesItems" -> Json.toJson(req.salesItems),
      "sort" -> Json.toJson(req.sort),
      "paging" -> Json.toJson(req.paging)
    )
  }

  implicit val responseHeaderWrites: Writes[JsonResponseHeader] = (
    (__ \ "sequenceNumber").write[String] and
    (__ \ "statusCode").write[String] and
    (__ \ "message").write[String]
  )(unlift(JsonResponseHeader.unapply))
  
  implicit val onSalesResponseWrites = Writes[OnSalesJsonResponse] { resp =>
    Json.obj("header" -> Json.toJson(resp.header))
  }

  implicit val responseHeaderReads: Reads[JsonResponseHeader] = (
    (JsPath \ "sequenceNumber").read[String] and
    (JsPath \ "statusCode").read[String] and
    (JsPath \ "message").read[String]
  )(JsonResponseHeader.apply _)

  implicit val onSalesJsonResponseReads: Reads[OnSalesJsonResponse] =
    (JsPath \ "header").read[JsonResponseHeader] map OnSalesJsonResponse.apply

  implicit val scoredItemReads: Reads[ScoredItem] = (
    (JsPath \ "storeCode").read[String] and
    (JsPath \ "itemCode").read[String] and
    (JsPath \ "score").read[Double]
  )(ScoredItem.apply _)

  implicit val jsonRequestPagingReads: Reads[JsonRequestPaging] = (
    (JsPath \ "offset").read[Int] and
    (JsPath \ "limit").read[Int]
  )(JsonRequestPaging.apply _)

  implicit val recommendByItemJsonResponseReads: Reads[RecommendByItemJsonResponse] = (
    (JsPath \ "header").read[JsonResponseHeader] and
    (JsPath \ "salesItems").read[Seq[ScoredItem]] and
    (JsPath \ "sort").read[String] and
    (JsPath \ "paging").read[JsonRequestPaging]
  )(RecommendByItemJsonResponse.apply _)

  def onSales(
    requestTime: Long,
    sequenceNumber: Long,
    transactionMode: TransactionMode,
    transactionTime: Long,
    userCode: String,
    itemTable: Seq[SalesItem]
  ): JsResult[OnSalesJsonResponse] = {
    val req = OnSalesJsonRequest(
      header = JsonRequestHeader(
        dateTime = new DateTime(requestTime),
        sequenceNumber = sequenceNumber.toString
      ),
      mode = TransactionSalesMode.asString,
      dateTime = new DateTime(transactionTime),
      userCode = userCode,
      salesItems = itemTable
    )

    sendJsonRequest("/onSales", "onSales", Json.toJson(req), _.validate[OnSalesJsonResponse])
  }

  def recommendByItem(
    requestTime: Long,
    sequenceNumber: Long,
    salesItems: Seq[SalesItem],
    sort: SortOrder,
    paging: JsonRequestPaging
  ): JsResult[RecommendByItemJsonResponse] = {
    val req = RecommendByItemJsonRequest(
      header = JsonRequestHeader(
        dateTime = new DateTime(requestTime),
        sequenceNumber = sequenceNumber.toString
      ),
      salesItems = salesItems,
      sort = sort.toString,
      paging = paging
    )

    sendJsonRequest(
      "/recommendByItem",
      "recommendByItem", Json.toJson(req), _.validate[RecommendByItemJsonResponse]
    )
  }

  def sendJsonRequest[T](
    contextPath: String, apiName: String, jsonRequest: JsValue, resultValidator: JsValue => JsResult[T]
  ): JsResult[T] = {
    val jsonResponse: JsValue = jsonServer.request(contextPath, jsonRequest)
    val result: JsResult[T] = resultValidator(jsonResponse)

    result match {
      case JsError(error) =>
        logger.error(
          "Sending recommend " + apiName + " request. error: " + error +
          ", req: " + jsonRequest + ", resp: " + jsonResponse
        )
      case _ =>
    }

    result
  }
}

class RecoEngModule extends Module {
  def bindings(environment: Environment, configuration: Configuration): Seq[Binding[_]] = {
    Seq(
      bind[JsonServer].to[JsonServerImpl],
      bind[RecoEngApi].to[RecoEngApiImpl]
    )
  }
}

