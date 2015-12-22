package com.mate1.kafka.avro

import com.mate1.kafka.avro.fixtures._
import kafka.consumer.Consumer

import scala.compat.Platform
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

/**
  * Created by Marc-Andre Lamothe on 2/24/15.
  */
class KafkaAvroProducerSpec extends UnitSpec with Zookeeper with Kafka with Config {

  behavior of "The Kafka Avro producer"

  val config = loadConfig()

  it should "prefix messages with the proper magic bytes" in {
    val topic = "MAIL_LOG"

    val producer = new KafkaAvroProducer[TestRecord](AvroProducerConfig(config.getConfig("producer")), topic) {
      /**
        * Method that gets called when the producer is closed.
        */
      override protected def onClose(): Unit = {}

      /**
        * Method that gets called when an error occurs while retrieving a schema from the repository.
        */
      override protected def onProducerFailure(e: Exception): Unit = {}

      /**
        * Method that gets called when an error occurs while retrieving a schema from the repository.
        */
      override protected def onSchemaRepoFailure(e: Exception): Unit = {}

      /**
        * Method that gets called when an error occurs while decoding a message.
        */
      override protected def onEncodingFailure(e: Exception, message: TestRecord): Unit = {}
    }
    val record = new TestRecord()
    record.setTestId(Random.nextLong())
    record.setTimestamp(Platform.currentTime)
    producer.publish(record)

    wait(1 seconds)

    val conf = AvroConsumerConfig(config.getConfig("consumer")).kafkaConsumerConfig()
    val iterator = Consumer.create(conf).createMessageStreams(Map(topic -> 1))(topic).head.iterator()
    val data = iterator.next().message()
    //println(data.map("%02X" format _).mkString)

    val encoding = data(0) match {
      case 1 => "json"
      case 0 => "binary"
      case _ => ""
    }

    val schemaId = ((data(1) << 8) | (data(2) & 0xff)).toShort
    println("Schema Id:" + schemaId)

    assert(encoding.nonEmpty)
  }
}
