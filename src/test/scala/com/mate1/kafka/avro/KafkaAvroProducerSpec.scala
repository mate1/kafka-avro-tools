/*
   Copyright 2015 Mate1 inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   Created by Marc-André Lamothe on 2/24/15.
*/

package com.mate1.kafka.avro

import com.mate1.kafka.avro.fixtures._
import kafka.consumer.Consumer

import scala.compat.Platform
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

class KafkaAvroProducerSpec extends UnitSpec with Zookeeper with Kafka with Config {

  behavior of "The Kafka Avro producer"

  val config = loadConfig()

  it should "prefix messages with the proper magic bytes" in {
    val topic = "MAIL_LOG"

    val producer = new KafkaAvroProducer[TestRecord](AvroProducerConfig(config.getConfig("producer")), topic) {
      override protected def onClose(): Unit = {}

      override protected def onProducerFailure(e: Exception): Unit = {}

      override protected def onSchemaRepoFailure(e: Exception): Unit = {}

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
