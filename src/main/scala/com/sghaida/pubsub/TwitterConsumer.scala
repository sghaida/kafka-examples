package com.sghaida.pubsub

import java.time
import java.util.Properties

import com.sghaida.elk.Elastic
import com.sksamuel.elastic4s.indexes.IndexRequest
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.json4s.JsonAST.{JInt, JNumber, JObject, JString}
import org.json4s.jackson.JsonMethods
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.{asJavaCollectionConverter, iterableAsScalaIterableConverter}
import scala.concurrent.duration.{Duration, DurationInt}


object TwitterConsumer {

  private final val logger = LoggerFactory.getLogger("TwitterConsumer")
  implicit final def fromFiniteDuration(d: Duration): time.Duration = time.Duration.ofMillis(d.toMillis)
  implicit def toOption[T](a: T): Some[T] = Some(a)

  def main(args: Array[String]): Unit = {

    /*create es index*/
    val es = Elastic("https://host:443","username", "password")

    val res = es.createIndex("twitter", None,  "tweets")
    if (res.isLeft) throw new Exception("es index couldn't be created")

    /* 1. create consumer properties */
    val config = {

      val props = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      /* disable auto-commit offset and commit it manually*/
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50")
      props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,"10000")

      props.put(ConsumerConfig.GROUP_ID_CONFIG, "twitter-elastic")
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "latest")

      props
    }

    /* 2. create consumer */
    val consumer = new KafkaConsumer[String, String](config)

    /* 3. subscribe consumer to a topic
      for one topic Collections singleton "some-topic"
      for multiple topics List("some-topic").asJavaCollection
     */
    consumer.subscribe(List("twitter-tweets").asJavaCollection)

    /* poll data */
    while (true){
      val records = consumer.poll(100 milliseconds)
      logger.info(s"received records count: ${records.count}")

      var bulkRequest = List[IndexRequest]()

      for {rec <- records.asScala}{
        val id = extractId(
          rec.value(),
          s"${rec.topic()}_${rec.partition()}_${rec.offset()}",
          "id_str"
        )

        if (getFollowersCount(rec.value()) > 1000)
          /* just save the tweets that has more than 1000 follower for that specific user*/
          bulkRequest :+=  es.genInsertDoc("twitter", "tweets", rec.value(), id)
      }

      if (bulkRequest.nonEmpty) {
        es.bulkInsert(bulkRequest)
        logger.info("tweet has been inserted")
        logger.info("committing offsets")
        consumer.commitSync()
        logger.info("offsets has been committed")
        bulkRequest = List[IndexRequest]()
      }
    }
    //consumer.close()
  }

  def extractId(jsonStr: String, defaultId: String, idFieldName: String): String = {

    val res = JsonMethods.parse(jsonStr).asInstanceOf[JObject]

    res \ idFieldName match {
      case x: JString => x.s
      case _ => defaultId
    }
  }

  def getFollowersCount(jsonStr: String): Int = {
    val res = JsonMethods.parse(jsonStr).asInstanceOf[JObject]

    ((res \ "user").asInstanceOf[JObject] \ "followers_count").asInstanceOf[JNumber] match {
      case JInt(x) if x.isDefined=> x.value.toInt
      case _ => 0
    }
  }
}
