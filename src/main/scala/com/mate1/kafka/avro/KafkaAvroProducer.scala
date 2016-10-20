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

   Created by Marc-André Lamothe on 2/27/15.
*/

package com.mate1.kafka.avro

import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.config.Config
import kafka.producer.{KeyedMessage, Producer}
import org.apache.avro.specific.SpecificRecordBase

import scala.reflect._

/**
 * A Kafka producer implementation that publishes Avro messages unto a topic.
 *
 * Some magic bytes that specify the encoding format and the version of the schema used will be written before each message's data.
 */
abstract class KafkaAvroProducer[T <: SpecificRecordBase](config: Config, topic: String)(implicit tag: ClassTag[T]) {

  /**
   * Whether the producer was closed.
   */
  private val closed = new AtomicBoolean(false)

  /**
   * Kafka producer.
   */
  private var producer: Option[Producer[String, T]] = None

  /**
   * Kafka producer config.
   */
  private val producerConfig = AvroProducerConfig(config, Map[String, String](
    "avro.topic_name" -> topic,
    "key.serializer.class" -> "kafka.serializer.StringEncoder",
    "serializer.class" -> "com.mate1.kafka.avro.AvroEncoder"
  ))

  /**
   * Close this producer, preventing further messages from being published.
   */
  final def close(): Unit = {
    if (!closed.getAndSet(true)) {
      onClose()

      if (producer.isDefined)
        producer.get.close()
      producer = None
    }
  }

  /**
   * Whether the producer is closed.
   */
  final def isClosed: Boolean = closed.get()

  /**
   * Method that gets called when the producer is closed.
   */
  protected def onClose(): Unit

  /**
   * Method that gets called when an error occurs while retrieving a schema from the repository.
   */
  protected def onProducerFailure(e: Exception): Unit

  /**
   * Adds the specified message to the specified topic.
   * @param message the message to be published
   * @return true if the message was published successfully, false otherwise
   */
  final def publish(message: T): Boolean = try {
    if (!closed.get()) {
      // Serialize and queue the message on the broker
      if (producer.isEmpty)
        producer = Some(new Producer[String, T](producerConfig))
      producer.get.send(new KeyedMessage(topic, message))
      true
    }
    else
      false
  }
  catch {
    case e: Exception =>
      onProducerFailure(e)
      if (producer.isDefined)
        producer.get.close()
      producer = None
      false
  }

}
