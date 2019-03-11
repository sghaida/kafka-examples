package com.sghaida.streams

import java.time
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import Serdes._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, DurationLong}

object WordCount {

  private final val logger = LoggerFactory.getLogger("WordCount")
  implicit final def fromFiniteDuration(d: Duration): time.Duration = time.Duration.ofMillis(d.toMillis)

  def main(args: Array[String]): Unit = {
    val config = {

      val props = new Properties()
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer.getClass.getName)
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest")

      props
    }

    val builder: StreamsBuilder = new StreamsBuilder
    val textLines: KStream[String, String] = builder.stream[String, String]("streams-plaintext-input")
    val wordCounts: KTable[String, Long] = textLines
      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
      .groupBy((_, word) => word)
      .count()

    wordCounts.toStream.to("streams-wordcount-output")

    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)

    streams.start()

    sys.ShutdownHookThread {
      streams.close(10 seconds)
    }
  }
}
