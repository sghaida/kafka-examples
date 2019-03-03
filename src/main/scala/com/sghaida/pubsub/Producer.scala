package com.sghaida.pubsub


import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory


object Producer {

  private final val logger = LoggerFactory.getLogger("ProducerDemo")
  def main(args: Array[String]): Unit = {

    /* 1. create producer properties */
    val config = {

      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

      props
    }

    /* 2. create producer */
    val producer = new KafkaProducer[String, String](config)

    /* 3. send data */
    for {i <- 1 to 10}{
      val rec = new ProducerRecord(s"some-topic", s"$i", s"hello $i")
      //producer.send(rec)
      producer.send(rec, (metadata: RecordMetadata, exception: Exception) => {
        exception match {
          case null => logger.info(s"offset: ${metadata.offset()} | partition: ${metadata.partition()} ")
          case ex@_ => throw ex
        }
      })
    }

    producer.close()

  }
}
