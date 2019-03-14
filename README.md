## Kafka examples
some experimentation with Kafka, not the cleanest code out their but for demonstration purposes it fits perfectly.
the code is supplementary for the Scala lovers for the following awsome course provided by [Stephane Maarek] (https://www.udemy.com/user/stephane-maarek/)
1. [apache-kafka](https://www.udemy.com/apache-kafka/)
2. [kafka-streams](https://www.udemy.com/kafka-streams/)

### Kafka basic commands
Common used commands

#### Run zookeeper
```bash
./zookeeper-server-start.sh config/zookeeper.properties
```

#### Run Kafka
```bash
./kafka-server-start.sh config/server.properties
```

#### Create topic
create a kafka topic
```bash
./kafka-topics.sh --create --zookeeper localhost:2181 \
    --replication-factor 1 --partitions 1 --topic streams-plaintext-input
```

#### start Producer
Start kafka producer
```bash
./kafka-console-producer.sh --broker-list localhost:9092 --topic streams-plaintext-input
```

#### start stream consumer from console with Key/Value deserializers
```bash
./kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic streams-wordcount-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

### App Related

#### run producer
```bash
 sbt "runMain com.sghaida.producers.Producer"
```

#### run normal consumer
none-threaded implementation. basically it keeps listening forever and consume whatever has been produced for specific topic

```bash
 sbt "runMain com.sghaida.consumers.BasicConsumer"
```

#### run threaded consumer
threaded implementation. basically it keeps listening forever on its own thread and consume whatever has been produced for specific topic,
until termination signal is being caught from the main thread

```bash
 sbt "runMain com.sghaida.consumers.ConsumerWithThread"
```

#### run Play back
basically it keeps it allows you to specify topic, partition and offset to read from, you can think about it as player for events

```bash
 sbt "runMain com.sghaida.consumers.ConsumerAssignSeek"
```

#### ingest tweets in Kafka from twitter stream
read from twitter stream and ingest in kafka using Producer
```bash
sbt "runMain com.sghaida.producers.TwitterProducer"
```

#### ingest tweets in ElasticSearch using Kafka Producer
read from twitter from kafka and insert them into Elasticsearch with their ids
```bash
sbt "runMain com.sghaida.consumers.TwitterConsumer"
```


### produce random bank transaction
Ingest random bank transactions
```bash
sbt "runMain com.sghaida.producers.TransactionsProducer"
```

### aggregate account balance
for the generated random bank transactions aggregate all balances using `groubByKey` and `aggregate`
 ```bash
 sbt "runMain com.sghaida.streams.TransactionsBalance"
 ```
