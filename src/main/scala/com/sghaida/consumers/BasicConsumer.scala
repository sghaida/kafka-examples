package com.sghaida.consumers

import java.time
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.{asJavaCollectionConverter, iterableAsScalaIterableConverter}
import scala.concurrent.duration.{Duration, DurationInt}


object BasicConsumer {

  private final val logger = LoggerFactory.getLogger("ConsumerDemo")
  implicit final def fromFiniteDuration(d: Duration): time.Duration = time.Duration.ofMillis(d.toMillis)


  def main(args: Array[String]): Unit = {

    /* 1. create consumer properties */
    val config = {

      val props = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

      props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-demo")
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "latest")

      props
    }

    /* 2. create consumer */
    val consumer = new KafkaConsumer[String, String](config)

    /* 3. subscribe consumer to a topic
      for one topic Collections singleton "some-topic"
      for multiple topics List("some-topic").asJavaCollection
     */
    consumer.subscribe(List("some-topic").asJavaCollection)

    /* poll data */
    while (true){
      val records = consumer.poll(100 milliseconds)

      for {rec <- records.asScala}{
        logger.info(
          s"message: key: ${rec.key()}| Value: ${rec.value()} | partition: ${rec.partition()} | offset: ${rec.offset()}"
        )
      }
    }

//    consumer.close()

  }
}
