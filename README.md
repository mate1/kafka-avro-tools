# Kafka Avro Tools

This is a Scala/Java library which provides abstract classes that can be used to easily publish and
read Avro records into Kafka!

These tools require that only a specific Avro schema be used on every Kafka topic. This does not prevent
the modification of the Avro schema associated with each topic, resulting in a new version of said schema
if the Avro Schema Repository is used. They do not however support having multiple distinct types of Avro
records within a single topic!

Uses:
* Apache Avro 1.7.5
* Apache Kafka 0.8.1
* Mate1 Avro Schema Repository (optional)

## Producers

To publish Avro records into a Kafka topic, simply create a producer instance that extend the
KafkaAvroProducer class and provides it a specific Avro schema, topic name and AvroProducerConfig for
every Avro record you want to publish.

The producer only blocks while a call to publish is made after which the result is returned to the to
the caller, they run in the same thread as the caller. **WARNING** the Avro producers are not thread safe
and a single instance should not be used across multiple thread, therefore each thread should use it's
own producer instance.

You will then be able to call it's publish function, which accepts an Avro record object built using the
Avro schema associated with the producer, to publish that record into Kafka. It's that simple!

**Scala example:**
```
    val config = AvroProducerConfig(ConfigFactory.parseFile(new File("...")).resolve())
    val producer = new KafkaAvroProducer[TestRecord](config, "topic_name") {
      override protected def onClose(): Unit = {}
      override protected def onProducerFailure(e: Exception): Unit = {}
      override protected def onSchemaRepoFailure(e: Exception): Unit = {}
      override protected def onEncodingFailure(e: Exception, message: TestRecord): Unit = {}
    }
    val record = new TestRecord()
    record.setTestId(Random.nextLong())
    record.setTimestamp(Platform.currentTime)
    producer.publish(record)
```

#### Configuration

The producers support all of the Kafka producer configuration values and the following new values, which
are all optional:

* **avro.default_schema_id**, the default schema id to write in every message's magic bytes when no
a schema version cannot be found for the target topic in the schema repository or if no repository
is defined (default value: 0).
* **avro.encoding**, specified which type of Avro encoding to use when serializing the messages, can
be either _binary_ or _json_ (default: binary).
* **avro.schema_repo_url**, the url to the Avro Schema Repository used to recover the version id of the
current schema, if no version id can be found for the current schema the default schema id will be used.

**Sample**

```
avro.default_schema_id = 0
avro.encoding = binary
avro.schema_repo_url = ""
metadata.broker.list = "localhost:19092"
producer.type = sync
request.required.acks = 1
```

## Consumers

To consume Avro records from a Kafka topic, simply create a consumer instance that extends the
KafkaAvroConsumer or KafkaAvroBatchConsumer class and provides it with an Avro record instance, topic name
and AvroConsumerConfig for every Kafka topic you want to consume from.

Because the consumer blocks until a new record is available in Kafka, every consumer implements the
**Runnable** interface and should run in it's own thread, which can be done by calling it's **start()**
function, calling the function while the consumer is already active will do nothing. The consumer can
be stopped by calling the **stop()** function, which will cleanly shutdown the consumer and it's thread,
calling the function while the consumer is already stopping will do nothing.

The consumer will need to define a **consume()** function that will be called whenever a record or batch
of record is ready to be processed. The consumer's thread will block until the **consume()** function
returns or throws an exception, if an exception is thrown then the consumer will not read any more
messages from Kafka and the thread will stop.

#### Configuration

The consumers support all of the Kafka consumer configuration values and the following new values, which
are all optional:

* **avro.schema_repo_url**, the url to the Avro Schema Repository used to recover the Avro schema used
to publish each message, if no writer schema can be obtained the consumer will attempt to deserialize the
record using the schema from it's record instance.

**Sample**

```
  auto.offset.reset = smallest
  avro.schema_repo_url = ""
  consumer.timeout.ms = 1000
  group.id = "sample"
  zookeeper.connect = "localhost:12181"
```

#### KafkaAvroBatchConsumer

That class allows you to consume several records together. The consumer will read records from Kafka and
pool them in a batch and will only call the **consume()** function once the batch is full or a certain
timeout is reached.

The consumer will override the following Kafka consumer configurations if the size of it's batch is greater
than 1, discarding any value previously set in the config; **auto.commit.enable**, **consumer.timeout.ms**.
The consumer will manually commit offsets after each call to the **consume()** function has returned
successfully. If the function throws an exception then the current offsets will not be committed.

**Scala example:**
```
    val batch = (1 to 10).map(x => new TestRecord())
    val config = AvroConsumerConfig(ConfigFactory.parseFile(new File("...")).resolve())
    val consumer = new KafkaAvroBatchConsumer[TestRecord](config, "topic_name", batch, 3.seconds) {
        override protected def consume(records: Seq[TestRecord]): Unit = {
            records.foreach(record => println(record.toString))
        }
        final override protected def onConsumerFailure(e: Exception): Unit = {}
        final override protected def onDecodingFailure(e: Exception, message: MessageAndMetadata[Array[Byte], Array[Byte]]): Unit = {}
        final override protected def onStart(): Unit = {}
        final override protected def onStop(): Unit = {}
        final override protected def onStopped(): Unit = {}
        final override protected def onSchemaRepoFailure(e: Exception): Unit = {}
    }
    consumer.start()
```

#### KafkaAvroConsumer

That class allows you to consume records one at a time. The consumer will call the **consume()** function
for every record read from Kafka.

The regular consumer will not override any of the Kafka consumer configuration and it's **consume()**
function will be called for every single record read from Kafka. The offsets will be committed
periodically, unless auto-commit is disabled in the configuration in which case it will be your
consumer's responsibility to commit them by calling the **commitOffsets()** function. If the function
throws an exception then the offset of the current offsets may or may not be committed, depending on
Kafka's high level consumer current implementation.

**Scala example:**
```
    val config = AvroConsumerConfig(ConfigFactory.parseFile(new File("...")).resolve())
    val consumer = new KafkaAvroConsumer[TestRecord](config, "topic_name", new TestRecord()) {
        override protected def consume(record: TestRecord): Unit = {
            println(record)
        }
        final override protected def onConsumerFailure(e: Exception): Unit = {}
        final override protected def onDecodingFailure(e: Exception, message: MessageAndMetadata[Array[Byte], Array[Byte]]): Unit = {}
        final override protected def onStart(): Unit = {}
        final override protected def onStop(): Unit = {}
        final override protected def onStopped(): Unit = {}
        final override protected def onSchemaRepoFailure(e: Exception): Unit = {}
    }
    consumer.start()
```
