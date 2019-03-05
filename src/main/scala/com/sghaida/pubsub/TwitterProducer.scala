package com.sghaida.pubsub

import java.util.Properties
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.sghaida.twitter.{OAuth, Twitter => tw}
import com.google.common.collect.Lists
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory


object TwitterProducer {

  private val logger = LoggerFactory.getLogger(TwitterProducer.getClass.getName)

  private val consumerApiKey = sys.env.getOrElse(
    "TWITTER_CONSUMER_API_KEY", throw new Exception("TWITTER_CONSUMER_API_KEY is not defined in env")
  )

  private val consumerApiSecret = sys.env.getOrElse(
    "TWITTER_CONSUMER_API_SECRET", throw new Exception("TWITTER_CONSUMER_API_SECRET is not defined in env")
  )

  private val accessToken = sys.env.getOrElse(
    "TWITTER_ACCESS_TOKEN", throw new Exception("TWITTER_ACCESS_TOKEN is not defined in env")
  )

  private val accessTokenSecret = sys.env.getOrElse(
    "TWITTER_ACCESS_TOKEN_SECRET", throw new Exception("TWITTER_ACCESS_TOKEN_SECRET is not defined in env")
  )

  private val auth = OAuth(
    consumerApiKey = consumerApiKey,
    consumerApiSecret = consumerApiSecret,
    accessToken = accessToken,
    accessTokenSecret = accessTokenSecret
  )

  private val config = {

    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, Int.MaxValue.toString)
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5") // kafka version < 2.0.0 use one instead

    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy") // add compression
    props.put(ProducerConfig.LINGER_MS_CONFIG, "10") // wait 1 sec before send the batch
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, (32*1024).toString) // if the batch size is meet before the linger.ms expire


    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    props
  }

  private def createProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](config)

  def run():Unit = {

    val producer = createProducer

    // val followings = Lists.newArrayList(1234L, 566788L)
    val terms = Lists.newArrayList("kafka", "restful", "news")
    val messageQueue: LinkedBlockingQueue[String] = new LinkedBlockingQueue[String](1000)

    val client = new tw()
      .withOauth(auth)
      //.withEventQueue(Some(1000))
      .withMessageQueue(messageQueue)
      .withHosebirdConfig(followings = None, terms=Some(terms)).buildClient

    client.connect()

    scala.sys.runtime.addShutdownHook(new Thread(() => {
      logger.info("shutting down")
      client.stop()
      producer.close()
    }))

    while (!client.isDone) {
      val msg = try {
        Some(messageQueue.poll(5,TimeUnit.SECONDS))
      } catch {
        case ex: InterruptedException => {
          logger.error(ex.getMessage)
          client.stop()
          None
        }
      }
      msg match {
        case Some(tweet) =>
          logger.info(tweet)
          producer.send(
            new ProducerRecord( "twitter-tweets",null, tweet),
            new Callback {
              def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
                if (exception != null){
                  logger.error(exception.getMessage)
                }
              }
            })
        case None =>
      }
    }
  }

  def main(args: Array[String]): Unit = run()
}
