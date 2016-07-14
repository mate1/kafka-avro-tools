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

import com.typesafe.config.{Config, ConfigValue}
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.compat.Platform
import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}

/**
 * A Kafka consumer implementation that reads several Avro records from the topic into a batch before processing them.
 *
 * The batch will be passed to the consume function once it is full or its age exceeds the specified timeout.
 *
 * This consumer will disable the auto-commit feature and enable read timeout on the underlying Kafka consumer
 * if the size of the batch of records used is greater than 1. Offsets will instead be committed manually after
 * each batch has been processed successfully.
 *
 * If any exceptions are thrown by the consume function then the offsets will not be committed and the consumer will stop.
 */
abstract class KafkaAvroBatchConsumer[T <: SpecificRecord](config: Config, topic: String, batchSize: Int, timeout: Duration) extends Runnable {

  /**
   * Whether this consumer thread is still running.
   */
  private val active = new AtomicBoolean(false)

  /**
   * Kafka consumer config.
   */
  private val consumerConfig = getKafkaConsumerConfig

  /**
   * Flag that indicates whether offset should be committed before next poll.
   */
  private var flushOffsets = false

  /**
   * The number of milliseconds to wait between consecutive poll requests when timeout is 0.
   */
  protected val pollInterval = 100

  /**
   * Whether this consumer was stopped.
   */
  private val stopped = new AtomicBoolean(false)

  /**
   * Commits the offset the last record consumed from the the queue before the next cycle.
   */
  protected final def commitOffsets(): Unit = {
    if (!stopped.get)
      flushOffsets = true
  }

  /**
   * Method that gets called each time a new batch of records is ready for processing.
   * @param records the record to process
   */
  protected def consume(records: Seq[T]): Unit

  /**
   * @return the Kafka config used by this consumer
   */
  final def getConfig: Properties = consumerConfig

  /**
   * @return the Kafka consumer config
   */
  private def getKafkaConsumerConfig: Properties = {
    val props = new Properties()

    for (entry: Entry[String, ConfigValue] <- config.entrySet.asScala)
        props.put(entry.getKey, entry.getValue.unwrapped.toString)

    // Set the key & value deserializers
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])

    // Disable auto commit if batch size is greater than 1, because we will manually commit after each batch
    if (batchSize > 1)
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    // Set the max polled records to the batch size
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batchSize.toString)

    // Enable the creation of specific Avro records
    props.put("specific.avro.reader", "true")

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
   * Method that gets called when a timeout occurs while consuming from Kafka.
   */
  protected def onConsumerTimeout(): Unit

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
      stopped.set(false)

      // Initialize consumer
      val consumer = new KafkaConsumer[Unit, T](consumerConfig)
      consumer.subscribe(Seq(topic).asJava)

      // Start the consumer
      Try(onStart())

      // Polling loop
      Try {
        var batch = mutable.ListBuffer[T]()
        var batchTimestamp = Platform.currentTime
        while (!stopped.get) {
          // Commit offsets
          if (flushOffsets) {
            flushOffsets = false
            consumer.commitSync()
          }

          // Pool records from the topic into the current batch
          val records = consumer.poll(Math.max(timeout.toMillis, 0))
          if (!records.isEmpty) {
            val iterator = records.iterator()
            while (iterator.hasNext)
              batch += iterator.next().value()
          }
          else if (timeout.toMillis > 0)
            Try(onConsumerTimeout())
          else
            // Stagger poll requests to prevent processor saturation when timeout is 0
            Thread.sleep(pollInterval)

          // Process records if batch is full or timeout is reached
          if (batch.size >= batchSize || Platform.currentTime - batchTimestamp >= timeout.toMillis) {
            if (batch.nonEmpty) {
              consume(batch)
              if (batchSize > 1) {
                consumer.commitSync()
                flushOffsets = false
              }
              batch = mutable.ListBuffer[T]()
            }
            batchTimestamp = Platform.currentTime
          }
        }
      } match {
        case Failure(e: Exception) =>
          Try(onConsumerFailure(e))
        case _ =>
      }

      // Close the consumer
      consumer.close()
    } match {
      case Failure(e: Exception) =>
        Try(onConsumerFailure(e))
      case _ =>
    }

    // Terminate the consumer
    Try(onStopped())

    // Update status
    active.set(false)
  }

  /**
   * Starts the consumer on a new thread.
   */
  final def start(): Unit = {
    if (!active.get()) {
      // Run the consumer in a new thread
      new Thread(this).start()

      // Wait for the thread to have started
      while (!active.get)
        Try(Thread.sleep(10))
    }
  }

  /**
   * Stops the consumer.
   */
  final def stop(): Unit = {
    if (!stopped.getAndSet(true))
      Try(onStop())
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
