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

import java.util.Map.Entry
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.compat.Platform
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.{Failure, Try}

/**
 * A Kafka consumer implementation that reads several Avro messages from the topic into a batch before processing them.
 *
 * The batch will be passed to the consume function once it is full or its age exceeds the specified timeout.
 *
 * This consumer will disable the auto-commit feature and enable read timeout on the underlying Kafka consumer
 * if the size of the batch of messages used is greater than 1. Offsets will instead be committed manually after
 * each batch has been processed successfully.
 *
 * If any exceptions are thrown by the consume function then the offsets will not be committed and the consumer will stop.
 */
abstract class KafkaAvroBatchConsumer[T <: SpecificRecord](config: Config, topic: String, batchSize: Int, timeout: Duration)(implicit tag: ClassTag[T]) extends Runnable {

  /**
   * Whether this consumer thread is still running.
   */
  private val active = new AtomicBoolean(false)

  /**
   * List of messages in the current batch.
   */
  private val batch = mutable.ListBuffer[T]()

  /**
   * Timestamp of when the current batch was started.
   */
  private var batchTimestamp = 0L

  /**
   * Kafka consumer connector.
   */
  private var consumer: KafkaConsumer[Unit, T] = _

  /**
   * Kafka consumer config.
   */
  private val consumerConfig = getKafkaConsumerConfig

  /**
   * Whether this consumer was stopped.
   */
  private val stopped = new AtomicBoolean(false)

  /**
   * Commits the offset the last message consumed from the the queue.
   */
  protected final def commitOffsets(): Unit = consumer match {
    case consumer: KafkaConsumer[Unit, T] if !stopped.get =>
      consumer.commitSync()
    case _ =>
  }

  /**
   * Method that gets called each time a new batch of messages is ready for processing.
   * @param messages the message to process
   */
  protected def consume(messages: Seq[T]): Unit

  /**
   * @return the Kafka config used by this consumer
   */
  final def getConfig: Properties = consumerConfig

  /**
   * @return the Kafka consumer config
   */
  private def getKafkaConsumerConfig: Properties = {
    val overrides = mutable.Map[String, String]()

    // Force disable auto commit if batch size is greater than 1, because we will manually commit after each batch
    if (batchSize > 1)
      overrides.put("enable.auto.commit", "false")

    // Generate Kafka consumer config
    val conf = if (overrides.nonEmpty) ConfigFactory.parseMap(overrides.asJava).withFallback(config) else config
    val props = new Properties()

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])

    for (entry: Entry[String, ConfigValue] <- conf.entrySet.asScala)
        props.put(entry.getKey, entry.getValue.unwrapped.toString)

    props
  }

  /**
   * @return the topic targeted by this consumer
   */
  final def getTopic: String = topic

  /**
   * @return whether this consumer is still active or not
   */
  final def isActive: Boolean = active.get

  /**
   * Method that gets called when an error occurs while consuming from Kafka.
   */
  protected def onConsumerFailure(e: Exception): Unit

  /**
   * Method that gets called when the consumer is starting.
   */
  protected def onStart(): Unit

  /**
   * Method that gets called when the consumer is stopping.
   */
  protected def onStop(): Unit

  /**
   * Method that gets called when the consumer has stopped.
   */
  protected def onStopped(): Unit

  /**
   * Kafka consumer thread.
   */
  @Override
  final def run(): Unit = {
    Try {
      // Update status
      active.set(true)

      // Initialize consumer
      consumer = new KafkaConsumer[Unit, T](consumerConfig)

      // Start the consumer
      onStart()

      // Initialize message iterator
      batchTimestamp = Platform.currentTime
      while (!stopped.get) {
        val iterator = consumer.poll(timeout.toMillis).iterator()
        while (iterator.hasNext)
          batch += iterator.next().value()

        if (batch.size >= batchSize || Platform.currentTime - batchTimestamp > timeout.toMillis) {
          if (batch.nonEmpty) {
            consume(batch)
            if (batchSize > 1)
              commitOffsets()
            batch.clear()
          }
          batchTimestamp = Platform.currentTime
        }
      }
    } match {
      case Failure(e: Exception) =>
        onConsumerFailure(e)
        // Stop the consumer
        stop()
      case Failure(e: Throwable) =>
        // Stop the consumer
        stop()
      case _ =>
    }

    // Terminate the consumer
    onStopped()

    // Update status
    active.set(false)
  }

  /**
   * Starts the consumer on a new thread.
   */
  final def start(): Unit = {
    if (!active.get()) {
      // Run the consumer
      new Thread(this).start()

      // Update status
      active.set(true)
      stopped.set(false)
    }
  }

  /**
   * Stops the consumer.
   */
  final def stop(): Unit = {
    if (!stopped.getAndSet(true)) {
      onStop()

      consumer match {
        case consumer: KafkaConsumer[Unit, T] =>
          consumer.close()
        case _ =>
      }
    }
  }

  /**
   * Wait for the consumer to have shutdown completely if a call to stop was made, otherwise this does nothing.
   */
  @tailrec
  final def waitUntilStopped(): Unit = {
    if (stopped.get && active.get) {
      Try(Thread.sleep(100))
      waitUntilStopped()
    }
  }
}
