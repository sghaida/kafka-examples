package com.sghaida.twitter

import twitter4j.{Query, TwitterFactory}
import twitter4j.conf.ConfigurationBuilder

import scala.collection.JavaConverters.asScalaBufferConverter

case class Twitter(
      key: String, secret: String, accessToken: String, tokenSecret: String,
      retryIntervalSeconds: Int = 60, retryCount: Int=10
) {

  private val buildConfig: ConfigurationBuilder = {
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(key)
      .setOAuthConsumerSecret(secret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(tokenSecret)
      .setHttpRetryIntervalSeconds(retryIntervalSeconds)
      .setHttpRetryCount(retryCount)

    cb
  }

  private val getConnection: twitter4j.Twitter = {
    val tf:TwitterFactory = new TwitterFactory(buildConfig.build())
    tf.getInstance()
  }

  def search(topic: String): List[String] = {
    val query = new Query(s"$topic")
    try{
      val result = getConnection.search(query)
      result.getTweets.asScala.toList.map(item => item.getText)
    }catch {
      case ex:Exception => throw ex
    }
  }
}



