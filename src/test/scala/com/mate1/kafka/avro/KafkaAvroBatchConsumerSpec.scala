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


import com.mate1.kafka.avro.fixtures.{Config, Kafka, UnitSpec, Zookeeper}
import kafka.message.MessageAndMetadata

import scala.collection.mutable.MutableList
import scala.compat.Platform
import scala.concurrent.duration._

class KafkaAvroBatchConsumerSpec extends UnitSpec with Zookeeper with Kafka with Config {

  behavior of "The Kafka Avro batch consumer"

  val config = loadConfig()

  val batchSize = 10

  it should "consume a batch smaller than batchSize" in {
    val arrivedBatch = MutableList(1)

    val topic = "TEST_LOG"

    val consumer = new KafkaAvroBatchConsumer[TestRecord](AvroConsumerConfig(config.getConfig("consumer")), topic, (1 to batchSize).map(x => new TestRecord()), 3.seconds) {
      override protected def consume(message: Seq[TestRecord]): Unit = {
        arrivedBatch += message.size
      }

      final override protected def onConsumerFailure(e: Exception): Unit = {}

      final override protected def onDecodingFailure(e: Exception, message: MessageAndMetadata[Array[Byte], Array[Byte]]): Unit = {}

      final override protected def onStart(): Unit = {}

      final override protected def onStop(): Unit = {}

      final override protected def onStopped(): Unit = {}

      final override protected def onSchemaRepoFailure(e: Exception): Unit = {}
    }

    val producer = new KafkaAvroProducer[TestRecord](AvroProducerConfig(config.getConfig("producer")), topic) {

      override protected def onClose(): Unit = {}

      override protected def onProducerFailure(e: Exception): Unit = {}

      override protected def onSchemaRepoFailure(e: Exception): Unit = {}

      override protected def onEncodingFailure(e: Exception, message: TestRecord): Unit = {}
    }

    val batch = (1 to 20).map(x => {
      val record = new TestRecord()
      record.setTestId(x.toLong)
      record.setTimestamp(Platform.currentTime)
      record
    })

    val (batchPart1, batchPart2) = batch.splitAt(6)
    batchPart1.foreach(record => producer.publish(record))

    val thread = new Thread(consumer)
    thread.start()

    wait(5.seconds)

    batchPart2.foreach(record => producer.publish(record))

    wait(4.seconds)

    assert(arrivedBatch.size == 4)
    assert(arrivedBatch(1) == 6)
    assert(arrivedBatch(2) == 10)
    assert(arrivedBatch(3) == 4)

    thread.stop()
  }

}
