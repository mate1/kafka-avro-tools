# Kafka Avro Tools

This is a Scala/Java library which provides abstract classes that can be used to easily publish and
read Avro records into Kafka!

These tools require that only a specific Avro schema be used on every Kafka topic. This does not prevent
the modification of the Avro schema associated with each topic, resulting in a new version of said schema
if the Avro Schema Repository is used. They do not however support having multiple distinct types of Avro
records within one topic!

Uses:
* Apache Avro 1.7.7
* Apache Kafka 0.10.0
* Confluent Platform 3 Avro Serializer & Schema Registry

## Producers

To publish Avro records into one or several Kafka topic, simply create a producer instance that extend the
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
val producer = new KafkaAvroProducer[TestRecord](config) {
override protected def onClose(): Unit = {}
override protected def onProducerFailure(e: Exception): Unit = {}
}
val record = new TestRecord()
record.setTestId(Random.nextLong())
record.setTimestamp(Platform.currentTime)
producer.publish(record, "topic_name")
```

#### Configuration

The producers support all of the standard Kafka producer configuration values and the Confluent Platform
specific ones:

**Sample**

```
bootstrap.servers = "localhost:19092"
producer.type = sync
request.required.acks = 0
schema.registry.url = "http://0.0.0.0:8081"
```

## Consumers

To consume Avro records from a Kafka topic, simply create a consumer instance that extends the
KafkaAvroConsumer or KafkaAvroBatchConsumer class for the desired Avro record type and provide it
with the configuration and topic name for every Kafka topic you want to consume from.

Every consumer implements the **Runnable** interface and should be run separate threads, which can be
done by calling it's **start()** function, calling the function while the consumer is already active will
do nothing. The consumer can be stopped by calling the **stop()** function, which will cleanly shutdown
the consumer and it's thread, calling the function while the consumer is already stopping will do nothing.

The consumer will need to define a **consume()** function that will be invoked whenever a record or batch
of records is ready to be processed. The consumer's thread will block until the **consume()** function
returns or throws an exception, if an exception is thrown then the consumer will not read any more
messages from Kafka and the thread will stop.

#### Configuration

The consumers support all of the standard Kafka consumer configuration values and the Confluent Platform
specific ones:

**Sample**

```
auto.offset.reset = earliest
bootstrap.servers = "localhost:19092"
schema.registry.url = "http://0.0.0.0:8081"
```

#### KafkaAvroBatchConsumer

That consumer processes records in batches. The consumer will read records from Kafka and pool in memory
before calling the **consume()** function, which happens once the batch is full or the specified timeout
is reached.

It will override some Kafka consumer configurations, discarding any value previously
set in the config. The configurations are **max.poll.records**, **specific.avro.reader**, it will also
disable **auto.commit.enable** if the batch size is greater than 1, instead the consumer will manually
commit offsets after each call to the **consume()** function has returned successfully. If the function
throws an exception then the current offsets will not be committed.

**Scala example:**
```
    val config = AvroConsumerConfig(ConfigFactory.parseFile(new File("...")).resolve())
    val consumer = new KafkaAvroBatchConsumer[TestRecord](config, "topic_name", 10, 3.seconds) {
        override protected def consume(records: Seq[TestRecord]): Unit = {
            records.foreach(record => println(record.toString))
        }
        final override protected def onConsumerFailure(e: Exception): Unit = {}
        final override protected def onConsumerTimeout(): Unit = {}
        final override protected def onStart(): Unit = {}
        final override protected def onStop(): Unit = {}
        final override protected def onStopped(): Unit = {}
    }
    consumer.start()
```

#### KafkaAvroConsumer

That consumer processes records one at a time. The consumer will call the **consume()** function
for every record read from Kafka.

It will not override any of the Kafka consumer configuration and it's **consume()**
function will be called for every single record read from Kafka. The offsets will be committed
periodically, unless auto-commit is disabled in the configuration in which case you will need to
call the **commitOffsets()** function manually. If the function throws an exception then the offset
of the current offsets may or may not be committed, depending on Kafka's high level consumer
implementation.

**Scala example:**
```
    val config = AvroConsumerConfig(ConfigFactory.parseFile(new File("...")).resolve())
    val consumer = new KafkaAvroConsumer[TestRecord](config, "topic_name") {
        override protected def consume(record: TestRecord): Unit = {
            println(record)
        }
        final override protected def onConsumerFailure(e: Exception): Unit = {}
        final override protected def onConsumerTimeout(): Unit = {}
        final override protected def onStart(): Unit = {}
        final override protected def onStop(): Unit = {}
        final override protected def onStopped(): Unit = {}
    }
    consumer.start()
```
