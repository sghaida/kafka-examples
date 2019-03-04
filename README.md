## Kafka examples
some experimentation with Kafka, not the cleanest code out their but for demonstration purposes it fits perfectly.

### run producer
```bash
 sbt "runMain com.sghaida.pubsub.Producer"
```

### run normal consumer
none-threaded implementation. basically it keeps listening forever and consume whatever has been produced for specific topic

```bash
 sbt "runMain com.sghaida.pubsub.BasicConsumer"
```

### run threaded consumer
threaded implementation. basically it keeps listening forever on its own thread and consume whatever has been produced for specific topic,
until termination signal is being caught from the main thread

```bash
 sbt "runMain com.sghaida.pubsub.ConsumerWithThread"
```

### run Play back
basically it keeps it allows you to specify topic, partition and offset to read from, you can think about it as player for events

```bash
 sbt "runMain com.sghaida.pubsub.ConsumerAssignSeek"
```