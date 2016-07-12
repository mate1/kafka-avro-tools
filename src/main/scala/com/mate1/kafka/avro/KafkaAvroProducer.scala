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

import java.util.Map.Entry
import java.util.Properties
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.config.{Config, ConfigValue}
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.JavaConverters._
import scala.reflect._
import scala.util.{Failure, Success, Try}

/**
 * A Kafka producer implementation that publishes Avro messages onto one or several topics.
 *
 * Some magic bytes that specify the encoding format and the version of the schema used will be written before each message's data.
 */
abstract class KafkaAvroProducer[T <: SpecificRecord](config: Config)(implicit tag: ClassTag[T]) {

  /**
   * Whether the producer was closed.
   */
  private val closed = new AtomicBoolean(false)

  /**
   * Kafka producer.
   */
  private var producer: Option[KafkaProducer[Unit, T]] = None

  /**
   * Kafka producer config.
   */
  private val producerConfig = getKafkaProducerConfig

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
   * @return the Kafka producer config
   */
  private def getKafkaProducerConfig: Properties = {
    val props = new Properties()

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])

    for (entry: Entry[String, ConfigValue] <- config.entrySet.asScala)
      props.put(entry.getKey, entry.getValue.unwrapped.toString)

    props
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
   * @param topic the topic onto which to publish the message
   * @return true if the message was published successfully, false otherwise
   */
  final def publish(message: T, topic: String): Boolean = Try {
    if (!closed.get()) {
      // Initialize the producer on first message or if an error occurred
      if (producer.isEmpty)
        producer = Some(new KafkaProducer[Unit, T](producerConfig))

      // Send the message to the brokers
      Try(producer.get.send(new ProducerRecord[Unit, T](topic, message)).get) match {
        case Failure(e: ExecutionException) =>
          throw e.getCause
        case Failure(_) =>
          false
        case Success(_) =>
          true
      }
    }
    else
      false
  } match {
    case Failure(e: Exception) =>
      onProducerFailure(e)
      if (producer.isDefined)
        producer.get.close()
      producer = None
      false
    case Failure(e: Throwable) =>
      throw e
    case Success(result) =>
      result
  }

}
