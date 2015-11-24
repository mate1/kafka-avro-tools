package com.mate1.kafka.avro.utils

import com.mate1.kafka.avro.{AvroConsumerConfig, AvroProducerConfig, KafkaAvroProducer}

import scala.compat.Platform
import scala.concurrent.duration._

/**
  * Created by Marc-Andre Lamothe on 2/24/15.
  */
class KafkaAvroProducerSpec extends UnitSpec with Zookeeper with Kafka with Config {

  behavior of "The Kafka Avro producer"

  val config = loadConfig()

  it should "prefix messages with the proper magic bytes" in {
    val topic = "MAIL_LOG"

    val producer = new KafkaAvroProducer[MailMessageLog](AvroProducerConfig(config), MailMessageLog.SCHEMA$, {})
    val log = new MailMessageLog()
    log.setLogUuid(MailTrackingUtil.newUUID())
    log.setAction("foobar")
    log.setTimestamp(Platform.currentTime)
    log.setUuid(MailTrackingUtil.newUUID())
    producer.queueMessage(topic, log)

    wait(1 seconds)

    val iterator = Consumer.create(AvroConsumerConfig(config)).createMessageStreams(Map(topic -> 1))(topic).head.iterator()
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
