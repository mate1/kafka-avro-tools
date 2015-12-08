package com.mate1.kafka.avro

import java.util.concurrent.atomic.AtomicBoolean

import kafka.consumer.{Consumer, ConsumerConnector, ConsumerTimeoutException}
import org.apache.avro.specific.SpecificRecord

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.util.{Failure, Try}

/**
  * Class that reads and decodes Avro messages from a Kafka queue.
  *
  * Created by Marc-Andre Lamothe on 2/24/15.
  */
abstract class KafkaAvroConsumer[T <: SpecificRecord](config: AvroConsumerConfig, topic: String, message: T)(implicit tag: ClassTag[T])
  extends AvroDecoder[T](config.schema_repo_url, topic) with Runnable {

  /**
    * Whether this consumer thread is still running.
    */
  private val active = new AtomicBoolean(false)

  /**
    * Kafka consumer connector.
    */
  private var consumer: ConsumerConnector = _

  /**
    * Whether this consumer was stopped.
    */
  private val stopped = new AtomicBoolean(false)

  /**
    * Commits the offset the last message consumed from the the queue.
    */
  protected final def commitOffsets(): Unit = {
    if (!stopped.get && active.get())
      consumer.commitOffsets
  }

  /**
    * Method that gets called each time a new message is ready for processing.
    * @param message the message to process
    */
  protected def consume(message: T): Unit

  /**
    * @return the group used by this consumer
    */
  final def getGroup: String = config.groupId

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
      // Initialize consumer
      consumer = Consumer.create(config)

      // Update status
      active.set(true)
      onStart()

      // Initialize message iterator
      val iterator = consumer.createMessageStreams(Map(topic -> 1))(topic).head.iterator()
      while (iterator.hasNext()) {
        // Get the next message and de-serialize it
        val msg = deserializeMessage(iterator.next(), message)
        if (msg.isDefined)
          consume(msg.get)
      }
    } match {
      case Failure(e: Exception) =>
        if (!e.isInstanceOf[ConsumerTimeoutException])
          onConsumerFailure(e)
        // Stop the consumer
        stop()
      case Failure(e: Throwable) =>
        // Stop the consumer
        stop()
      case _ =>
    }

    // Update status
    active.set(false)
    onStopped()
  }

  /**
   * Starts the consumer on a new thread.
   */
  final def start(): Unit = {
    new Thread(this).start()
  }

  /**
    * Stops the consumer.
    */
  final def stop(): Unit = {
    if (!stopped.getAndSet(true)) {
      onStop()

      if (active.get())
        consumer.shutdown()
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
