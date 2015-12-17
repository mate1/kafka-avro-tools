package com.mate1.kafka.avro

import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.config.ConfigFactory
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, ConsumerTimeoutException}
import org.apache.avro.specific.SpecificRecord

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.compat.Platform
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.{Failure, Try}

/**
  * Class that reads and decodes Avro messages from a Kafka queue.
  *
  * Created by Marc-Andre Lamothe on 2/24/15.
  */
abstract class KafkaAvroBatchConsumer[T <: SpecificRecord](config: AvroConsumerConfig, topic: String, messages: Seq[T], timeout: Duration)(implicit tag: ClassTag[T])
  extends AvroDecoder[T](config.schema_repo_url, topic) with Runnable {

  /**
    * Whether this consumer thread is still running.
    */
  private val active = new AtomicBoolean( false)

  /**
   * Batch of events
   */
  private val batch = mutable.ListBuffer[T]()

  /**
    * Kafka consumer connector.
    */
  private var consumer: ConsumerConnector = _

  /**
    * Kafka consumer config.
    */
  private val consumerConfig = config.generateConsumerConfig(Some(ConfigFactory.parseMap(Seq[(String,String)](
    // Force disable auto commit if batch size is greater than 1, because we will manually commit after each batch
    if (messages.size > 1)
      "auto.commit.enable" -> "false"
    else
      null
    ,
    // Force consumer timeout if batch size is greater than 1, because we want the stream iterator to timeout periodically to check the time since last consume
    if (messages.size > 1 && Try(config.conf.getString("consumer.timeout.ms").toLong).filter(value => value > 0).isFailure)
      "consumer.timeout.ms" -> Math.max(timeout.toMillis/10,200).toString
    else
      null
  ).toMap.asJava)))

  /**
    * Time when we last called consume.
    */
  private var lastConsumedTime = 0L

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
    * Method that gets called each time a new batch of messages is ready for processing.
    * @param messages the message to process
    */
  protected def consume(messages: Seq[T]): Unit

  /**
    * @return the Kafka config used by this consumer
    */
  final def getConfig: ConsumerConfig = consumerConfig

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
      consumer = Consumer.create(consumerConfig)

      // Update status
      active.set(true)
      onStart()

      // Initialize message iterator
      val iterator = consumer.createMessageStreams(Map(topic -> 1))(topic).head.iterator()
      lastConsumedTime = Platform.currentTime
      while (true) {
        // Get the next message and de-serialize it
        Try {
          if (iterator.hasNext()) {
            val msg = deserializeMessage(iterator.next(), messages(batch.size))
            if (msg.isDefined)
              batch += msg.get
          }
        } match {
          case Failure(e: ConsumerTimeoutException) if messages.size > 1 =>
            // Ignore consumer timeouts while batching
          case Failure(e: Throwable) =>
            throw e
          case _ =>
        }

        if (batch.size >= messages.size || Platform.currentTime - lastConsumedTime > timeout.toMillis) {
          if (batch.nonEmpty) {
            consume(batch)
            if (messages.size > 1)
              commitOffsets()
            batch.clear()
          }
          lastConsumedTime = Platform.currentTime
        }
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
