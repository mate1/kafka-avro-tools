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

   Created by Marc-Andre Lamothe 07/13/16
*/

package com.mate1.kafka.avro

import com.mate1.kafka.avro.fixtures.{Config, Kafka, SchemaRegistry, UnitSpec, Zookeeper}

import scala.collection.mutable
import scala.compat.Platform
import scala.concurrent.duration._

class KafkaAvroConsumerSpec extends UnitSpec with Zookeeper with Kafka with SchemaRegistry with Config {

  behavior of "The Kafka Avro consumer"

  it should "consume 20 records one at a time" in {
    val records = mutable.Buffer[TestRecord]()
    val topic = "TEST_LOG"

    val consumer = new KafkaAvroConsumer[TestRecord](consumerConfig, topic, 0 millisecond) {
      override protected def consume(record: TestRecord): Unit = {
        records += record
      }

      final override protected def onConsumerFailure(e: Exception): Unit = { e.printStackTrace() }

      final override protected def onConsumerTimeout(): Unit = {}

      final override protected def onStart(): Unit = {}

      final override protected def onStop(): Unit = {}

      final override protected def onStopped(): Unit = {}
    }

    val producer = new KafkaAvroProducer[TestRecord](producerConfig) {
      override protected def onClose(): Unit = {}

      override protected def onProducerFailure(e: Exception): Unit = { e.printStackTrace() }
    }

    for (i <- 0 until 20) {
      val record = new TestRecord()
      record.setTestId(i.toLong)
      record.setTimestamp(Platform.currentTime)
      producer.publish(record, topic)
    }

    consumer.start()

    wait(2.seconds)

    consumer.stop()
    consumer.waitUntilStopped()

    assert(records.size == 20)

    var matches = true
    for (i <- records.indices) {
      matches = matches && records(i).getTestId == i
    }
    assert(matches)
  }

}
