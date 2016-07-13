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

   Created by Mohammad Sedighi on 12/17/15.
*/

package com.mate1.kafka.avro

import com.mate1.kafka.avro.fixtures.{Config, Kafka, SchemaRegistry, UnitSpec, Zookeeper}

import scala.collection.mutable
import scala.compat.Platform
import scala.concurrent.duration._

class KafkaAvroBatchConsumerSpec extends UnitSpec with Zookeeper with Kafka with SchemaRegistry with Config {

  behavior of "The Kafka Avro batch consumer"

  it should "consume 20 records in several batches" in {
    val batches = mutable.Buffer[Seq[TestRecord]]()
    val topic = "TEST_LOG"

    val consumer = new KafkaAvroBatchConsumer[TestRecord](consumerConfig, topic, 10, 3.seconds) {
      override protected def consume(records: Seq[TestRecord]): Unit = {
        batches += records
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

    val batch = (0 until 20).map(x => {
      val record = new TestRecord()
      record.setTestId(x.toLong)
      record.setTimestamp(Platform.currentTime)
      record
    })

    val (batch1, batch2) = batch.splitAt(6)
    batch1.foreach(record => producer.publish(record, topic))

    consumer.start()

    wait(6.seconds)

    batch2.foreach(record => producer.publish(record, topic))

    wait(6.seconds)

    consumer.stop()
    consumer.waitUntilStopped()

    assert(batches.size >= 2)
    assert(batches.foldLeft(0)((result, records) => { result + records.size }) == 20)
    assert(batches(0).size == 6)
    assert(batches(1).size >= 10)

    var matches = true
    val records = batches.flatten
    for (i <- records.indices) {
      matches = matches && records(i).getTestId == i
    }
    assert(matches)
  }

}
