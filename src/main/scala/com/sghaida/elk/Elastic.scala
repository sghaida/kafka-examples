package com.sghaida.elk

import com.sksamuel.elastic4s.http.{ElasticClient, ElasticError, ElasticProperties}
import com.sksamuel.elastic4s.{ElasticApi, RefreshPolicy}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.indexes.IndexRequest
import com.sksamuel.elastic4s.mappings.MappingDefinition
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClientBuilder.{HttpClientConfigCallback, RequestConfigCallback}
import org.slf4j.LoggerFactory

case class Elastic (private val uri: String, private val username: Option[String], private val password: Option[String]) {

  private def createClientWithPass() = {
    val provider = {
      val provider = new BasicCredentialsProvider
      val credentials = new UsernamePasswordCredentials(username.get, password.get)
      provider.setCredentials(AuthScope.ANY, credentials)
      provider
    }

    ElasticClient(
      ElasticProperties(s"$uri"),
      new RequestConfigCallback {
        override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder =
          requestConfigBuilder
      } ,
      new HttpClientConfigCallback {
        override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
          httpClientBuilder.setDefaultCredentialsProvider(provider)
        }
    }
    )
  }

  private val client = {
    if (username.isDefined && password.isDefined) createClientWithPass()
    else ElasticClient(ElasticProperties(s"$uri"))
  }

  def createIndex(indexName: String, mappingDef: Option[MappingDefinition], mappingName: String): Either[ElasticError, Boolean] = {

    val res = client.execute {
      mappingDef match {
          case None => ElasticApi.createIndex (indexName)
          case Some(_) => ElasticApi.createIndex(indexName).mappings(mappingDef)
        }
      }.await

    if (res.isSuccess) {
      Elastic.logger.info(s"index: $indexName has been created: ${res.isSuccess}")
      Right(res.isSuccess)
    } else {
      Left(res.error)
    }
  }

  def insertDoc(indexName: String, docType: String, fields: Map[String, Any]): Either[ElasticError, String] = {
    val res = client.execute{
      indexInto(indexName / docType).fields(fields).refresh(RefreshPolicy.Immediate)
    }.await

    if (res.isSuccess){
      Elastic.logger.info(
        s"doc: {${fields.map(item => item._1 + ":" + item._2.toString ).mkString(", ")}} has been inserted into index: $indexName with Id: ${res.result.id}"
      )

      Right(res.result.id)
    } else {
      Elastic.logger.error(
        s"doc: {doc hasn't been inserted due to some error: ${res.error.reason}"
      )
      Left(res.error)
    }
  }

  def genInsertDoc(indexName: String, docType: String, jsonStr: String, id: Option[String]): IndexRequest = {

    val res = id match {
      case None => indexInto(indexName / docType).doc(jsonStr).refresh(RefreshPolicy.Immediate)
      case Some(_) => indexInto(indexName / docType).doc(jsonStr).withId(id.get).refresh(RefreshPolicy.Immediate)
    }

    res
  }

  def bulkInsert (bulkRequest: List[IndexRequest]):  Either[ElasticError, String] ={
    val res = client.execute(bulk(bulkRequest)).await

    if (res.isSuccess){
      //Elastic.logger.info(s"doc: $jsonStr has been inserted into index: $indexName with Id: ${res.result.id}")
      Elastic.logger.info(s"docs has been")

      Right(res.result.items.map(item => item.id).mkString(", "))
    } else {
      Elastic.logger.error(
        s"doc: {doc hasn't been inserted due to some error: ${res.error.reason}"
      )
      Left(res.error)
    }

  }

  def insertDoc(indexName: String, docType: String, jsonStr: String, id: Option[String]): Either[ElasticError, String] = {

    val res = id match {
      case None =>
        client.execute{
          indexInto(indexName / docType).doc(jsonStr).refresh(RefreshPolicy.Immediate)
        }.await

      case Some(_) =>
        client.execute{
          indexInto(indexName / docType).doc(jsonStr).withId(id.get).refresh(RefreshPolicy.Immediate)
        }.await
    }


    if (res.isSuccess){
      //Elastic.logger.info(s"doc: $jsonStr has been inserted into index: $indexName with Id: ${res.result.id}")
      Elastic.logger.info(s"doc has been inserted into index: $indexName with Id: ${res.result}")
      Right(res.result.id)
    } else {
      Elastic.logger.error(
        s"doc: {doc hasn't been inserted due to some error: ${res.error.reason}"
      )
      Left(res.error)
    }
  }


  def deleteIndex(indexName: String): Boolean = {
    val res = client.execute{
      ElasticApi.deleteIndex(indexName)
    }.await

    Elastic.logger.info(s"index: $indexName has been deleted: ${res.isSuccess}")

    res.isSuccess
  }

}

object Elastic {

  private final val logger = LoggerFactory.getLogger(Elastic.getClass.getName)

  def apply(uri: String, username: Option[String], password: Option[String]): Elastic =
    new Elastic(uri, username, password)
}

