package com.sghaida.producers

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

object UserDataProducer {

  private final val logger = LoggerFactory.getLogger("UserDataProducer")

  private val config = {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    props.setProperty(ProducerConfig.RETRIES_CONFIG, "3")
    props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1")
    // set idempotent producer
    props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    props
  }

  val producer = new KafkaProducer[String, String](config)

  def main(args: Array[String]): Unit = {

    /* create a new user, then we send some data to Kafka */
    producer.send(userRecord("aa", "First=aa,Last=xx,Email=aa.xx@gmail.com")).get
    producer.send(purchaseRecord("aa", "Apples and Bananas (1)")).get
    Thread.sleep(10000)

    /*send user purchase, but it user doesn't exist*/
    producer.send(purchaseRecord("dd", "Kafka Udemy Course (2)")).get
    Thread.sleep(10000)

    /* update user "aa", and send a new transaction */
    producer.send(userRecord("aa", "First=aaa,Last=xx,Email=aa.xx@gmail.com")).get
    producer.send(purchaseRecord("aa", "Oranges (3)")).get
    Thread.sleep(10000)

    /* send a user purchase for bb, which doesnt exist but it will be created later */
    producer.send(purchaseRecord("bb", "Computer (4)")).get
    producer.send(userRecord("bb", "First=bb,Last=nn,GitHub=bbnn")).get
    producer.send(purchaseRecord("bb", "Books (4)")).get
    /* delete record*/
    producer.send(userRecord("bb", null)).get
    Thread.sleep(10000)


    /* create a user, and delete it before any purchase comes through */
    producer.send(userRecord("cc", "First=cc")).get
    producer.send(userRecord("cc", null)).get // that's the delete record
    producer.send(purchaseRecord("cc", "Apache Kafka Series (5)")).get
    Thread.sleep(10000)

    producer.close()

  }

  def userRecord(key: String, value: String):ProducerRecord[String, String] =
    new ProducerRecord[String, String]("user-table", key, value)

  def purchaseRecord(key: String, value: String):ProducerRecord[String, String] =
    new ProducerRecord[String, String]("user-purchases", key, value)

}
