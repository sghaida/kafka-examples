package com.sghaida.streams

import java.time
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.ImplicitConversions._
import Serdes._


import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, DurationLong}

object ColorCount {

  private final val logger = LoggerFactory.getLogger("ColorCount")
  implicit final def fromFiniteDuration(d: Duration): time.Duration = time.Duration.ofMillis(d.toMillis)

  private val config = {

    val props = new Properties()
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-application")

    /* make sure that the message processed exactly once*/
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest")

    props
  }

  val builder: StreamsBuilder = new StreamsBuilder
  val textLines: KStream[String, String] = builder.stream[String, String]("favourite-color-input")

  val usersAndColors: KStream[String, String] = textLines
    .filter((_,text) => text.contains(","))
    .selectKey((_,v) => v.split(",").head.toLowerCase())
    .mapValues((_,v) => v.split(",")(1).toLowerCase())
    .filter((_,v) => List("green", "red", "blue").contains(v))

/*
  val favouriteColor: KTable[String, Long] = usersAndColors
    .groupBy((_, color) => color).count()

  favouriteColor.toStream.to("favourite-color-output")
*/

  /* can be written to topic and then read as KTable as below which has a network overhead,
   * replication overhead, and storage overhead
   */

  usersAndColors.to("favourite-color-intermediate")

  val table: KTable[String, String] =
    builder.table[String, String]("favourite-color-intermediate")

  val output: KTable[String, Long] = table.groupBy((_, color) => (color,color))
    .count()

  output.toStream.to("favourite-color-output")

  def main(args: Array[String]): Unit = {

    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)

    streams.cleanUp()
    streams.start()

    sys.ShutdownHookThread {
      logger.info("shutdown received")
      streams.close(10 seconds)
    }
  }

}
