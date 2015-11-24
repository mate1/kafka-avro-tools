package com.mate1.kafka.avro

import java.io.ByteArrayOutputStream
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.producer.{KeyedMessage, Producer}
import org.apache.avro.io.{BinaryEncoder, Encoder, EncoderFactory, JsonEncoder}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecord}

import scala.reflect._
import scala.util.{Failure, Success, Try}

/**
  * Class that encodes and publishes Avro messages to a Kafka queue.
  *
  * Created by Marc-Andre Lamothe on 2/27/15.
  */
class KafkaAvroProducer[T <: SpecificRecord](config: AvroProducerConfig, topic: String)(implicit tag: ClassTag[T]) extends LazyLogging {

  /**
    * Whether the producer was closed.
    */
  private val closed = new AtomicBoolean(false)

  /**
    * Last used encoder instance.
    */
  private var encoder: Encoder = _

  /**
    * Kafka producer.
    */
  private var producer: Option[Producer[String, Array[Byte]]] = None

  /**
    * Avro writer.
    */
  private val writer = new SpecificDatumWriter[T](tag.runtimeClass.asInstanceOf[Class[T]])

  /**
    * Close this producer, preventing further messages from being published.
    */
  final def close(): Unit = {
    if (!closed.getAndSet(true)) {
      logger.debug(getClass.getSimpleName + " closing")
      if (producer.isDefined)
        producer.get.close()
      producer = None
    }
  }

  /**
    * Whether the producer is closed.
    */
  def isClosed: Boolean = closed.get()

  /**
    * Adds the specified message to the specified topic.
    * @param message the message to be published
    * @return true if the message was published successfully, false otherwise
    */
  final def publish(message: T): Boolean = Try {
    if (!closed.get()) {
      logger.debug(getClass.getSimpleName + s" publishing message onto topic $topic")

      // Serialize and queue the message on the broker
      if (producer.isEmpty)
        producer = Some(new Producer[String, Array[Byte]](config))
      producer.get.send(new KeyedMessage(topic, serializeMessage(message)))

      true
    }
    else
      false
  } match {
    case Failure(e: Exception) =>
      logger.error("An exception occurred:", e)
      if (producer.isDefined)
        producer.get.close()
      producer = None
      false
    case Failure(e: Throwable) =>
      throw e
    case Success(result) =>
      result
  }

  /**
    * @param message the message to be published
    * @return a byte array containing the serialized message
    */
  private final def serializeMessage(message: T): Array[Byte] = {
    logger.debug(getClass.getSimpleName + " serializing message")

    // Configure encoder
    val out = new ByteArrayOutputStream()
    encoder match {
      case oldEncoder: BinaryEncoder =>
        encoder = EncoderFactory.get().binaryEncoder(out, oldEncoder)
      case oldEncoder: JsonEncoder =>
        encoder = oldEncoder.configure(out)
      case _ =>
        config.encoding match {
          case AvroEncoding.Binary =>
            encoder = EncoderFactory.get().binaryEncoder(out, null)
          case AvroEncoding.JSON =>
            encoder = EncoderFactory.get().jsonEncoder(message.getSchema, out)
        }
    }

    // Try to retrieve the schema's version from the repository
    val schemaId = Try(AvroSchemaRepository(config.schema_repo_url.trim).getSchemaId(topic, message.getSchema).get) match {
      case Success(version: Short) => version
      case Failure(e: Throwable) =>
        if (config.schema_repo_url.trim.nonEmpty) {
          logger.error(getClass.getSimpleName + s" failed to recover schema id for topic $topic from repository, proceeding with default schema id!")
          logger.error("An exception occurred:", e)
        }
        config.default_schema_id
    }

    // Write the magic byte and schema version
    out.write(config.encoding.id)
    out.write(schemaId >> 8)
    out.write(schemaId & 0x00FF)

    // Encode and write the message
    writer.write(message, encoder)
    encoder.flush()

    out.toByteArray
  }

}
