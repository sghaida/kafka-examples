package com.sghaida.twitter

import java.{lang, util}

import com.twitter.hbc
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.{Constants, HttpHosts}
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.httpclient.auth.OAuth1
import com.twitter.hbc.core.event.Event
import java.util.concurrent.LinkedBlockingQueue


case class OAuth(consumerApiKey: String, consumerApiSecret: String, accessToken: String, accessTokenSecret: String)

abstract class ClientBuilder {
  var oAuthInfo: OAuth
  var eventQueueLength: Option[Int]
  var messageQueue: LinkedBlockingQueue[String]
  var terms: util.ArrayList[String]
  var followings: util.ArrayList[Long]

  def withOauth(oAuthInfo: OAuth): ClientBuilder
  def withMessageQueue(msgQueue: LinkedBlockingQueue[String]): ClientBuilder
  def withEventQueue(eventQueueLength: Option[Int]): ClientBuilder
  def withHosebirdConfig(terms: Option[util.ArrayList[String]], followings: Option[util.ArrayList[Long]]): ClientBuilder

  def buildClient: BasicClient
}

class TwitterClient(builder: ClientBuilder)extends hbc.ClientBuilder {
  val oAuthInfo: OAuth = builder.oAuthInfo
  val eventQueueLength: Option[Int] = builder.eventQueueLength
  val messageQueue: LinkedBlockingQueue[String] = builder.messageQueue
  val terms: util.ArrayList[String] = builder.terms
  val followings: util.ArrayList[Long] = builder.followings
}

class Twitter extends ClientBuilder{

  override var oAuthInfo: OAuth = OAuth("","","", "")
  override var eventQueueLength: Option[Int] = Some(1000)
  override var messageQueue: LinkedBlockingQueue[String] = new LinkedBlockingQueue[String](100000)
  override var terms: util.ArrayList[String] = new util.ArrayList[String]()
  override var followings: util.ArrayList[Long] = new util.ArrayList[Long]()

  override def withOauth(oAuthInfo: OAuth): ClientBuilder = {
    this.oAuthInfo = oAuthInfo
    this
  }

  override def withMessageQueue(msgQueue: LinkedBlockingQueue[String]): ClientBuilder = {
    this.messageQueue = msgQueue
    this
  }

  override def withEventQueue(eventQueueLength: Option[Int]): ClientBuilder = {
    eventQueueLength match {
      case length =>
        this.eventQueueLength = length
        this
      case None =>
        this.eventQueueLength = None
        this
    }

  }

  override def withHosebirdConfig(terms: Option[util.ArrayList[String]], followings: Option[util.ArrayList[Long]]): ClientBuilder = {
    terms match {
      case Some(termList) =>
        this.terms = termList
        this
      case None => this
    }

    followings match {
      case Some(followingList) =>
        this.followings = followingList
        this
      case None => this
    }
  }

  override def buildClient: BasicClient = {

    val client = new TwitterClient(this)

    val hosebirdHosts = new HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint = new StatusesFilterEndpoint

    if (terms.size() > 1)
      hosebirdEndpoint.trackTerms(terms)
    else if (followings.size() > 1)
      hosebirdEndpoint.followings(followings.asInstanceOf[util.List[lang.Long]])

    val auth = new OAuth1(
      client.oAuthInfo.consumerApiKey,
      client.oAuthInfo.consumerApiSecret,
      client.oAuthInfo.accessToken,
      client.oAuthInfo.accessTokenSecret
    )

    // val msgQueue = new LinkedBlockingQueue[String](client.messageQueueLength)


    import com.twitter.hbc.core.processor.StringDelimitedProcessor
    val builder = new hbc.ClientBuilder()
      .name("Hosebird-Client-01")
      .hosts(hosebirdHosts)
      .authentication(auth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(client.messageQueue))

      if (eventQueueLength.isDefined){
        val eventQueue = new LinkedBlockingQueue[Event](client.eventQueueLength.get)
        builder.eventMessageQueue(eventQueue)
      }

    val hosebirdClient = builder.build()
    hosebirdClient
  }
}
