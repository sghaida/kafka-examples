package com.sghaida.pubsub

import java.time
import java.util.Properties
import java.util.concurrent.CountDownLatch

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.{asJavaCollectionConverter, iterableAsScalaIterableConverter}
import scala.concurrent.duration.{Duration, DurationInt}


case class RunnableConsumer(latch: CountDownLatch) extends Runnable{

  /* convert between scala duration to java duration*/
  implicit final def fromFiniteDuration(d: Duration): time.Duration = time.Duration.ofMillis(d.toMillis)

  /* set the logging*/
  private val logger = LoggerFactory.getLogger(RunnableConsumer.getClass.getName)
  /* 1. create consumer properties */
  private val config = {

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-demo")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "latest")

    props
  }

  /* 2. create consumer */
  private final val consumer = new KafkaConsumer[String, String](config)

  consumer.subscribe(List("some-topic").asJavaCollection)

  override def run(): Unit = {
    try{
      /* poll data */
      while (true){
        val records = consumer.poll(100 milliseconds)

        for {rec <- records.asScala}{
          logger.info(
            s"message: key: ${rec.key()}| Value: ${rec.value()} | partition: ${rec.partition()} | offset: ${rec.offset()}"
          )
        }
      }
    } catch {
      case _ : WakeupException => logger.info("Shutdown received")
      case ex: Exception => logger.info(s"error just happened ${ex.getMessage}")
    }finally {
      consumer.close()
      latch.countDown()
    }
  }

  def shutdown(): Unit = consumer.wakeup()
}

object ConsumerWithThread {

  def runThread(): Unit = {

    val logger = LoggerFactory.getLogger("consumer-main")
    val latch = new CountDownLatch(1 )

    val consumerThread: Runnable = RunnableConsumer(latch)
    val th = new Thread(consumerThread)
    th.start()

    /* shutdown hook*/
    Runtime.getRuntime.addShutdownHook(new Thread( () => {

      logger.info("Caught shutdown hook")
      consumerThread.asInstanceOf[RunnableConsumer].shutdown()

      try latch await()
      catch {
        case e: InterruptedException => e.printStackTrace()
      }
      logger.info("Application has exited")
    }))

    try latch await()
    catch {
      case e: InterruptedException => logger.error("Application got interrupted", e)
    }
    finally logger.info("Application is closing")
  }

  def main(args: Array[String]): Unit = runThread()
}
