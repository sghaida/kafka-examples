package com.sghaida.producers

import java.util.Properties

import com.sghaida.models.Transaction
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import scala.util.control.Breaks.{break, breakable}

object TransactionsProducer {

  private final val logger = LoggerFactory.getLogger("TransactionsProducer")

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

    val res = Transaction.generateTransactions("saddam",10)

    res.foreach(println(_))
    var count =0

    breakable {
      while(true) {
          try {
            logger.info(s"producing transactions batch: {$count}")
            producer.send(createTransaction("aa") )
            Thread.sleep(100)
            producer.send(createTransaction("bb") )
            Thread.sleep(100)
            producer.send(createTransaction("cc") )
            Thread.sleep(100)
            count+=1
          } catch {
            case ex: InterruptedException => break
          }
        }
    }

    producer.close()

  }

  def createTransaction(name: String): ProducerRecord[String, String] = {
    new ProducerRecord(
      "bank-transactions",
      name,
      Transaction.generateTransactions(name,1).head
    )
  }

}
