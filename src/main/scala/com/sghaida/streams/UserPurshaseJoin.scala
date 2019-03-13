package com.sghaida.streams

import java.time
import java.util.Properties

import com.sghaida.streams.TransactionsBalance.{config, logger}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{Consumed, GlobalKTable, KeyValueMapper, Produced}
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, DurationInt}

object UserPurshaseJoin {

  private final val logger = LoggerFactory.getLogger("UserPurshaseJoin")
  implicit final def fromFiniteDuration(d: Duration): time.Duration = time.Duration.ofMillis(d.toMillis)

  private val config = {

    val props = new Properties()

    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-purchase-application")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)

    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest")

    props
  }

  def main(args: Array[String]): Unit = {

    implicit val consumed:Consumed[String, String] = Consumed `with` (Serdes.String, Serdes.String)
    implicit val produce:Produced[String, String] = Produced `with` (Serdes.String, Serdes.String)

    val builder: StreamsBuilder = new StreamsBuilder

    val userTable: GlobalKTable[String, String] =
      builder.globalTable[String, String]("user-table")

    val userPurchases = builder.stream("user-purchases")

    /*
      join user table with the stream on key(user)
      map from the (key, value) of this stream to the key of the GlobalKTable
    */
    val userPurchasesEnrichedJoin: KStream[String, String] =
      userPurchases.join(userTable)(
        (key, _) => key,
        (userPurchase: String, userInfo: String) =>
          "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]"
      ).peek((key, value) => {
        logger.info(s"$key -> $value")
      })

    userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join")


    val userPurchasesEnrichedLeftJoin: KStream[String, String] =
      userPurchases.leftJoin(userTable)(
        (key, _) => key,
        (userPurchase, userInfo) => userInfo match {
          case user if user !=null => "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]"
          case _ => "Purchase=" + userPurchase + ",UserInfo=null"
        }
      ).peek((key, value) => {
        logger.info(s"$key -> $value")
      })

    userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join")

    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)

    streams.cleanUp()
    streams.start()
    logger.info(streams.toString)

    sys.ShutdownHookThread {
      logger.info("shutdown received")
      streams.close(10 seconds)
    }
  }
}
