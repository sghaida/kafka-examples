package com.sghaida.pubsub

import java.time
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, DurationInt}


object ConsumerAssignSeek {

  private final val logger = LoggerFactory.getLogger("ConsumerAssignSeek")
  implicit final def fromFiniteDuration(d: Duration): time.Duration = time.Duration.ofMillis(d.toMillis)


  def main(args: Array[String]): Unit = {

    /* 1. create consumer properties */
    val config = {

      val props = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "latest")

      props
    }

    /* 2. create consumer */
    val consumer = new KafkaConsumer[String, String](config)

    /* 3. create topic partitions*/
    val partition = new TopicPartition("some-topic", 0)

    /* 4. assign partition to consumer*/
    consumer.assign(List(partition).asJavaCollection)

//    consumer.seekToEnd(List(partition).asJavaCollection)
//    val offset = consumer.position(partition)
//    consumer.seekToBeginning(List(partition).asJavaCollection)

    /* 5. seek */
    consumer.seek(partition, 15L)

    /* poll data */
    consumer.poll(1000 milliseconds).forEach( rec => {
      logger.info(
        s"message: key: ${rec.key()}| Value: ${rec.value()} | partition: ${rec.partition()} | offset: ${rec.offset()}"
      )
    })

  }
}
