package com.sghaida.streams

import java.time
import java.time.Instant
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.connect.json.{JsonDeserializer, JsonSerializer}

import Serdes._

import org.slf4j.LoggerFactory

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory

import scala.concurrent.duration.{Duration, DurationLong}

object TransactionsBalance {

  private final val logger = LoggerFactory.getLogger("ColorCount")
  implicit final def fromFiniteDuration(d: Duration): time.Duration = time.Duration.ofMillis(d.toMillis)

  private val config = {

    val props = new Properties()

    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "transactions-balance-application")
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)

    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest")

    props
  }

  def main(args: Array[String]): Unit = {

    val jsonSerializer: Serializer[JsonNode] = new JsonSerializer
    val jsonDeserialize: Deserializer[JsonNode] = new JsonDeserializer
    implicit val jsonSerde: Serde[JsonNode] = serialization.Serdes.serdeFrom(jsonSerializer, jsonDeserialize)


    val builder: StreamsBuilder = new StreamsBuilder
    val bankTransactions: KStream[String, JsonNode] = builder.stream[String, JsonNode ]("bank-transactions")


    import java.time.Instant
    val initialBalance = JsonNodeFactory.instance.objectNode()
    initialBalance.put("count", 0)
    initialBalance.put("balance", 0)
    initialBalance.put("time", Instant.ofEpochMilli(0L).toString)


    val bankBalance:KTable[String, JsonNode] = bankTransactions.groupByKey
      .aggregate[JsonNode](initialBalance)(
        (_, transaction, balance) => newBalance(transaction, balance)
      )

    bankBalance.toStream.to("bank-balance-exactly-once")

    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)

    streams.cleanUp()
    streams.start()

    sys.ShutdownHookThread {
      logger.info("shutdown received")
      streams.close(10 seconds)
    }

  }

  def newBalance(transaction: JsonNode, balance: JsonNode): JsonNode = {
    val newBalance = JsonNodeFactory.instance.objectNode()
    newBalance.put("count", balance.get("count").asInt() + 1)
    newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt())

    val balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli
    val transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli
    val newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch))
    newBalance.put("time", newBalanceInstant.toString)

    newBalance
  }
}
