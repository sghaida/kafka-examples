package com.sghaida.elk

import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.ElasticApi
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.mappings.MappingDefinition
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClientBuilder.{HttpClientConfigCallback, RequestConfigCallback}

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

  def createIndex(indexName: String, mappingDef: MappingDefinition): Boolean = {
    val res = client.execute {
      ElasticApi.createIndex(indexName).mappings(mappingDef)
    }.await

    res.isSuccess
  }

  def deleteIndex(indexName: String): Boolean = {
    val res = client.execute{
      ElasticApi.deleteIndex(indexName)
    }.await

    res.isSuccess
  }

}

object Elastic {
  def apply(uri: String, username: Option[String], password: Option[String]): Elastic =
    new Elastic(uri, username, password)
}
