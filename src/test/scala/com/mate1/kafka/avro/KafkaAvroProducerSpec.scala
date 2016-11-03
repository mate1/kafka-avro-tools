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
import org.scalatest.WordSpec

import scala.collection.mutable
import scala.compat.Platform
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

class KafkaAvroProducerSpec extends WordSpec with Zookeeper with Kafka with Config {

  val config = loadConfig()

  "The Kafka Avro producer" should {

    "prefix messages with the proper magic bytes" in {
      val topic = this.getClass.getSimpleName

      val producer = new KafkaAvroProducer[TestRecord](config.getConfig("producer"), topic) {
        override protected def onClose(): Unit = {}

        override protected def onProducerFailure(e: Exception): Unit = {}
      }
      val record = new TestRecord()
      record.setTestId(Random.nextLong())
      record.setTimestamp(Platform.currentTime)
      producer.publish(record)

      wait(1 seconds)

      val conf = AvroConsumerConfig(config.getConfig("consumer"))
      val consumer = Consumer.create(conf)
      val iterator = consumer.createMessageStreams(Map(topic -> 1))(topic).head.iterator()
      val data = iterator.next().message()
      consumer.shutdown()

      val encoding = data(0) match {
        case 1 => "json"
        case 0 => "binary"
        case _ => ""
      }
      info("Encoding: " + encoding)

      val schemaId = ((data(1) << 8) | (data(2) & 0xff)).toShort
      info("Schema Id: " + schemaId)

      assert(encoding.nonEmpty)
    }

    "be thread safe" in {
      val topic = s"${this.getClass.getSimpleName}-1"

      val messages = mutable.Buffer[Long]()

      val producer = new KafkaAvroProducer[TestRecord](config.getConfig("producer"), topic) {
        override protected def onClose(): Unit = {}
        override protected def onProducerFailure(e: Exception): Unit = {}
      }

      Future {
        for (x <- 0 until 20) {
          val record = new TestRecord()
          record.setTestId(x.toLong)
          record.setTimestamp(Platform.currentTime)
          producer.publish(record)
        }
      }

      Future {
        for (x <- 100 until 120) {
          val record = new TestRecord()
          record.setTestId(x.toLong)
          record.setTimestamp(Platform.currentTime)
          producer.publish(record)
        }
      }

      wait(1 seconds)

      val consumer = new KafkaAvroBatchConsumer[TestRecord](config.getConfig("consumer"), topic, 10, 3.seconds) {
        override protected def consume(records: Seq[TestRecord]): Unit = messages ++= records.map(_.getTestId.toLong)
        override protected def onConsumerFailure(e: Exception): Unit = { e.printStackTrace() }
        override protected def onStart(): Unit = {}
        override protected def onStop(): Unit = {}
        override protected def onStopped(): Unit = {}
      }

      consumer.start()

      wait(4.seconds)

      consumer.stop()
      consumer.waitUntilStopped()

      info("Message ids: " + messages.mkString(", "))

      assert(messages.length == 40)
      var matches = true
      val messages1 = messages.filter(_ < 100)
      messages1.indices.foreach(i => matches = matches && messages1(i) == i)
      val messages2 = messages.filter(_ >= 100)
      messages2.indices.foreach(i => matches = matches && messages2(i) == i + 100)
      assert(matches)
    }

  }
}
