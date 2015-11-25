package com.mate1.kafka.avro

import java.io.ByteArrayOutputStream
import java.util.concurrent.atomic.AtomicBoolean

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
abstract class KafkaAvroProducer[T <: SpecificRecord](config: AvroProducerConfig, topic: String)(implicit tag: ClassTag[T]) {

  /**
    * Whether the producer was closed.
    */
  private val closed = new AtomicBoolean(false)

  /**
    * Avro encoder.
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
    * Method that gets called when an error occurs while decoding a message.
    */
  protected def onEncodingFailure(e: Exception, message: T): Unit

  /**
    * Method that gets called when an error occurs while retrieving a schema from the repository.
    */
  protected def onProducerFailure(e: Exception): Unit

  /**
    * Method that gets called when an error occurs while retrieving a schema from the repository.
    */
  protected def onSchemaRepoFailure(e: Exception): Unit

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
          producer = Some(new Producer[String, Array[Byte]](config))
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

  /**
    * @param message the message to be published
    * @return a byte array containing the serialized message
    */
  private final def serializeMessage(message: T): Option[Array[Byte]] = Try {
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
    val schemaId = config.schema_repo_url match {
      case repoUrl: String if repoUrl.trim.nonEmpty =>
        Try(AvroSchemaRepository(config.schema_repo_url).getSchemaId(topic, message.getSchema).get) match {
          case Failure(e: Exception) =>
            onSchemaRepoFailure(e)
            config.default_schema_id
          case Failure(e: Throwable) =>
            throw e
          case Success(id: Short) =>
            id
        }
      case _ =>
        config.default_schema_id
    }

    // Write the magic byte and schema version
    out.write(config.encoding.id)
    out.write(schemaId >> 8)
    out.write(schemaId & 0x00FF)

    // Encode and write the message
    writer.write(message, encoder)
    encoder.flush()

    Some(out.toByteArray)
  } match {
    case Failure(e: Exception) =>
      onEncodingFailure(e, message)
      None
    case Failure(e: Throwable) =>
      throw e
    case Success(result: Option[Array[Byte]]) =>
      result
  }

}
