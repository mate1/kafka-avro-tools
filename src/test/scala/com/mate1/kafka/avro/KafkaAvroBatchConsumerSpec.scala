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


import com.mate1.kafka.avro.fixtures.{Config, Kafka, Zookeeper}
import org.scalatest.WordSpec

import scala.collection.mutable
import scala.compat.Platform
import scala.concurrent.duration._

class KafkaAvroBatchConsumerSpec extends WordSpec with Zookeeper with Kafka with Config {

  val config = loadConfig()

  "The Kafka Avro batch consumer" should {

    "consume 20 messages in 3 batches" in {
      val batches = mutable.Buffer[Seq[Long]]()

      val topic = this.getClass.getSimpleName

      val consumer = new KafkaAvroBatchConsumer[TestRecord](config.getConfig("consumer"), topic, 10, 3.seconds) {
        override protected def consume(records: Seq[TestRecord]): Unit = batches += records.map(_.getTestId.toLong)
        override protected def onConsumerFailure(e: Exception): Unit = e.printStackTrace()
        override protected def onStart(): Unit = {}
        override protected def onStop(): Unit = {}
        override protected def onStopped(): Unit = {}
      }

      val producer = new KafkaAvroProducer[TestRecord](config.getConfig("producer"), topic) {
        override protected def onClose(): Unit = {}
        override protected def onProducerFailure(e: Exception): Unit = e.printStackTrace()
      }

      val batch = (0 until 20).map(x => {
        val record = new TestRecord()
        record.setTestId(x.toLong)
        record.setTimestamp(Platform.currentTime)
        record
      })

      val (batch1, batch2) = batch.splitAt(6)
      batch1.foreach(record => producer.publish(record))

      consumer.start()

      wait(6.seconds)

      batch2.foreach(record => producer.publish(record))

      wait(6.seconds)

      consumer.stop()
      consumer.waitUntilStopped()

      assert(batches.size == 3)
      assert(batches.foldLeft(0)((result, records) => {
        result + records.size
      }) == 20)
      assert(batches(0).size == 6)
      assert(batches(1).size == 10)
      assert(batches(2).size == 4)

      var matches = true
      val records = batches.flatten
      records.indices.foreach(i => matches = matches && records(i) == i)
      assert(matches)
    }

    "consume from two topics in parallel" in {
      val messages1 = mutable.Buffer[Long]()
      val messages2 = mutable.Buffer[Long]()

      val topic1 = s"${this.getClass.getSimpleName}-1"
      val topic2 = s"${this.getClass.getSimpleName}-2"

      val consumer1 = new KafkaAvroBatchConsumer[TestRecord](config.getConfig("consumer"), topic1, 10, 3.seconds) {
        override protected def consume(records: Seq[TestRecord]): Unit = messages1 ++= records.map(_.getTestId.toLong)
        override protected def onConsumerFailure(e: Exception): Unit = e.printStackTrace()
        override protected def onStart(): Unit = {}
        override protected def onStop(): Unit = {}
        override protected def onStopped(): Unit = {}
      }

      val consumer2 = new KafkaAvroBatchConsumer[TestRecord](config.getConfig("consumer"), topic2, 10, 3.seconds) {
        override protected def consume(records: Seq[TestRecord]): Unit = messages2 ++= records.map(_.getTestId.toLong)
        override protected def onConsumerFailure(e: Exception): Unit = e.printStackTrace()
        override protected def onStart(): Unit = {}
        override protected def onStop(): Unit = {}
        override protected def onStopped(): Unit = {}
      }

      val producer1 = new KafkaAvroProducer[TestRecord](config.getConfig("producer"), topic1) {
        override protected def onClose(): Unit = {}
        override protected def onProducerFailure(e: Exception): Unit = e.printStackTrace()
      }

      val producer2 = new KafkaAvroProducer[TestRecord](config.getConfig("producer"), topic2) {
        override protected def onClose(): Unit = {}
        override protected def onProducerFailure(e: Exception): Unit = e.printStackTrace()
      }

      (0 until 17).map(x => {
        val record = new TestRecord()
        record.setTestId(x.toLong)
        record.setTimestamp(Platform.currentTime)
        record
      }).foreach(record => producer1.publish(record))

      (0 until 23).map(x => {
        val record = new TestRecord()
        record.setTestId(x.toLong)
        record.setTimestamp(Platform.currentTime)
        record
      }).foreach(record => producer2.publish(record))

      consumer1.start()
      consumer2.start()

      wait(6.seconds)

      consumer1.stop()
      consumer1.waitUntilStopped()

      consumer2.stop()
      consumer2.waitUntilStopped()

      assert(messages1.size == 17)
      assert(messages2.size == 23)

      var matches = true
      messages1.indices.foreach(i => matches = matches && messages1(i) == i)
      assert(matches)

      matches = true
      messages2.indices.foreach(i => matches = matches && messages2(i) == i)
      assert(matches)
    }

  }
}
