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

import java.io.ByteArrayOutputStream

import org.apache.avro.io.{BinaryEncoder, Encoder, EncoderFactory, JsonEncoder}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecord}

import scala.reflect._
import scala.util.{Failure, Success, Try}

/**
 * Abstract class that provides Avro message to Kafka message encoding functionality.
 */
abstract class AvroEncoder[T <: SpecificRecord](default_schema_id: Short, encoding: AvroEncoding.Value, schema_repo_url: String, topic: String)(implicit tag: ClassTag[T]) {

  /**
   * Avro encoder.
   */
  private var encoder: Encoder = _

  /**
   * Avro writer.
   */
  private val writer = new SpecificDatumWriter[T](tag.runtimeClass.asInstanceOf[Class[T]])

  /**
   * Method that gets called when an error occurs while decoding a message.
   */
  protected def onEncodingFailure(e: Exception, message: T): Unit

  /**
   * Method that gets called when an error occurs while retrieving a schema from the repository.
   */
  protected def onSchemaRepoFailure(e: Exception): Unit

  /**
   * Serializes an Avro message into binary data that can be published into Kafka.
   *
   * @param message the message to be published
   * @return a byte array containing the serialized message
   */
  protected final def serializeMessage(message: T): Option[Array[Byte]] = Try {
    // Configure encoder
    val out = new ByteArrayOutputStream()
    encoder match {
      case oldEncoder: BinaryEncoder =>
        encoder = EncoderFactory.get().binaryEncoder(out, oldEncoder)
      case oldEncoder: JsonEncoder =>
        encoder = oldEncoder.configure(out)
      case _ =>
        encoding match {
          case AvroEncoding.Binary =>
            encoder = EncoderFactory.get().binaryEncoder(out, null)
          case AvroEncoding.JSON =>
            encoder = EncoderFactory.get().jsonEncoder(message.getSchema, out)
        }
    }

    // Try to retrieve the schema's version from the repository
    val schemaId = schema_repo_url match {
      case repoUrl: String if repoUrl.trim.nonEmpty =>
        Try(AvroSchemaRepository(schema_repo_url).getSchemaId(topic, message.getSchema).get) match {
          case Failure(e: Exception) =>
            onSchemaRepoFailure(e)
            default_schema_id
          case Failure(e: Throwable) =>
            throw e
          case Success(id: Short) =>
            id
        }
      case _ =>
        default_schema_id
    }

    // Write the magic byte and schema version
    out.write(encoding.id)
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
