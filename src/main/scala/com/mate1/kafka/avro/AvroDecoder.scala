package com.mate1.kafka.avro

import kafka.message.MessageAndMetadata
import org.apache.avro.Schema
import org.apache.avro.io.{BinaryDecoder, Decoder, DecoderFactory, JsonDecoder}
import org.apache.avro.specific.{SpecificDatumReader, SpecificRecord}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
  * Class that reads and decodes Avro messages from a Kafka queue.
  *
  * Created by Marc-Andre Lamothe on 2/24/15.
  */
abstract class AvroDecoder[T <: SpecificRecord](schema_repo_url: String, topic: String)(implicit tag: ClassTag[T]) {

  /**
    * Avro Decoder for each schema version.
    */
  private val decoders = mutable.Map[Short, Decoder]()

  /**
    * The Avro message reader.
    */
  private val reader = new SpecificDatumReader[T](tag.runtimeClass.asInstanceOf[Class[T]])

  /**
    * @param kafkaMessage the message and metadata to decode
    * @return a MailMessage if decoded successfully, None otherwise
    */
  protected final def deserializeMessage(kafkaMessage: MessageAndMetadata[Array[Byte], Array[Byte]], message: T): Option[T] = Try {
    // Read the encoding and schema id
    val data = kafkaMessage.message()
    val encoding = AvroEncoding(data(0))
    val schemaId = ((data(1) << 8) | (data(2) & 0xff)).toShort

    // Retrieve the writer's schema from the Avro schema repository
    val schema = schema_repo_url match {
      case repoUrl: String if repoUrl.trim.nonEmpty =>
        Try(AvroSchemaRepository(schema_repo_url).getSchema(topic, schemaId).get) match {
          case Failure(e: Exception) =>
            onSchemaRepoFailure(e)
            message.getSchema
          case Failure(e: Throwable) =>
            throw e
          case Success(schema: Schema) =>
            schema
        }
      case _ =>
        message.getSchema
    }
    reader.setSchema(schema)

    // Decoder configuration
    decoders.get(schemaId) match {
      case Some(oldDecoder: BinaryDecoder) =>
        decoders.put(schemaId, DecoderFactory.get.binaryDecoder(data, 3, data.length - 3, oldDecoder))
      case Some(oldDecoder: JsonDecoder) =>
        decoders.put(schemaId, oldDecoder.configure(new String(data, 3, data.length - 3, "UTF-8")))
      case _ =>
        encoding match {
          case AvroEncoding.Binary =>
            decoders.put(schemaId, DecoderFactory.get.binaryDecoder(data, 3, data.length - 3, null))
          case AvroEncoding.JSON =>
            decoders.put(schemaId, DecoderFactory.get.jsonDecoder(schema, new String(data, 3, data.length - 3, "UTF-8")))
        }
    }

    // Decode the message
    Some(reader.read(message, decoders(schemaId)))
  } match {
    case Failure(e: Exception) =>
      onDecodingFailure(e, kafkaMessage)
      None
    case Failure(e: Throwable) =>
      throw e
    case Success(result: Option[T]) =>
      result
  }

  /**
    * Method that gets called when an error occurs while decoding a message.
    */
  protected def onDecodingFailure(e: Exception, message: MessageAndMetadata[Array[Byte], Array[Byte]]): Unit

  /**
    * Method that gets called when an error occurs while retrieving a schema from the repository.
    */
  protected def onSchemaRepoFailure(e: Exception): Unit
}
