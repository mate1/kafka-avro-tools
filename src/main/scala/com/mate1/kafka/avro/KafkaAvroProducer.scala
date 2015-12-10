package com.mate1.kafka.avro

import java.util.concurrent.atomic.AtomicBoolean

import kafka.producer.{KeyedMessage, Producer}
import org.apache.avro.specific.SpecificRecord

import scala.reflect._
import scala.util.{Failure, Success, Try}

/**
  * Class that encodes and publishes Avro messages to a Kafka queue.
  *
  * Created by Marc-Andre Lamothe on 2/27/15.
  */
abstract class KafkaAvroProducer[T <: SpecificRecord](config: AvroProducerConfig, topic: String)(implicit tag: ClassTag[T])
  extends AvroEncoder[T](config.default_schema_id, config.encoding, config.schema_repo_url, topic) {

  /**
    * Whether the producer was closed.
    */
  private val closed = new AtomicBoolean(false)

  /**
    * Kafka producer.
    */
  private var producer: Option[Producer[String, Array[Byte]]] = None

  /**
    * Kafka producer config.
    */
  private val producerConfig = config.generateProducerConfig()

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
  final def publish(message: T): Boolean = Try {
    if (!closed.get()) {
      // Serialize and queue the message on the broker
      val data = serializeMessage(message)
      if (data.isDefined) {
        if (producer.isEmpty)
          producer = Some(new Producer[String, Array[Byte]](producerConfig))
        producer.get.send(new KeyedMessage(topic, data.get))
        true
      }
      else
        false
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
