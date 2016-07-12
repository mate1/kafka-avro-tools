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

class KafkaAvroToolsSpec extends UnitSpec with Zookeeper with Kafka with SchemaRegistry with Config {

  behavior of "The Kafka Avro batch consumer"

  it should "consume 20 messages in 3 batches" in {
    val batchSizes = mutable.Buffer[Int]()
    val topic = "TEST_LOG"

    val consumer = new KafkaAvroBatchConsumer[TestRecord](consumerConfig, topic, 10, 3.seconds) {
      override protected def consume(message: Seq[TestRecord]): Unit = {
        batchSizes += message.size
      }

      final override protected def onConsumerFailure(e: Exception): Unit = { e.printStackTrace() }

      final override protected def onStart(): Unit = {}

      final override protected def onStop(): Unit = {}

      final override protected def onStopped(): Unit = {}
    }

    val producer = new KafkaAvroProducer[TestRecord](producerConfig) {
      override protected def onClose(): Unit = {}

      override protected def onProducerFailure(e: Exception): Unit = { e.printStackTrace() }
    }

    val batch = (1 to 20).map(x => {
      val record = new TestRecord()
      record.setTestId(x.toLong)
      record.setTimestamp(Platform.currentTime)
      record
    })

    val (batchPart1, batchPart2) = batch.splitAt(6)
    batchPart1.foreach(record => producer.publish(record, topic))

    val thread = new Thread(consumer)
    thread.start()

    wait(6.seconds)

    batchPart2.foreach(record => producer.publish(record, topic))

    wait(6.seconds)

    assert(batchSizes.size == 3)
    assert(batchSizes.sum == 20)
    assert(batchSizes.head == 6)
    assert(batchSizes(1) == 10)
    assert(batchSizes(2) == 4)

    thread.stop()
  }

}
