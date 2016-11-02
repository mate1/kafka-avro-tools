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
import java.util.logging.{Level, Logger}

import kafka.utils.VerifiableProperties
import org.apache.avro.io.{BinaryEncoder, Encoder, EncoderFactory, JsonEncoder}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecordBase}

import scala.util.Try

/**
 * Abstract class that provides Avro message to Kafka message encoding functionality.
 */
class AvroEncoder[T <: SpecificRecordBase](props: VerifiableProperties) extends kafka.serializer.Encoder[T] {

  private val defaultSchemaId = Try(props.getString("avro.default_schema_id").toShort).getOrElse(0:Short)

  private var binaryEncoder: BinaryEncoder = _

  private val encoding = props.getString("avro.encoding", "") match {
    case "json" => AvroEncoding.JSON
    case _ =>      AvroEncoding.Binary
  }

  private var jsonEncoder: JsonEncoder = _

  private val logger = Logger.getLogger(this.getClass.getCanonicalName)

  private val schemaRepo = Try(props.getString("avro.schema_repo_url")).filter(_.trim.nonEmpty).map(AvroSchemaRepository.apply).toOption

  private val topic = Try(props.getString("avro.topic_name")).filter(_.trim.nonEmpty).toOption

  private var writer: SpecificDatumWriter[T] = _

  private final def getEncoder(message: T, out: ByteArrayOutputStream): Encoder = {
    encoding match {
      case AvroEncoding.Binary =>
        binaryEncoder = EncoderFactory.get().binaryEncoder(out, binaryEncoder)
        binaryEncoder
      case AvroEncoding.JSON =>
        jsonEncoder match {
          case oldEncoder: JsonEncoder =>
            oldEncoder.configure(out)
          case _ =>
            jsonEncoder = EncoderFactory.get().jsonEncoder(message.getSchema, out)
            jsonEncoder
        }
    }
  }

  /**
   * Serializes an Avro message into binary data that can be published into Kafka.
   *
   * @param message the message to be published
   * @return a byte array containing the serialized message
   */
  final override def toBytes(message: T) : Array[Byte] = {
    try {
      // Initialize encoder & output stream
      val out = new ByteArrayOutputStream()
      val encoder = getEncoder(message, out)

      // Try to retrieve the schema's version from the repository
      val schemaId = schemaRepo.flatMap(repo => topic.flatMap(repo.getSchemaId(_, message.getSchema))).getOrElse(defaultSchemaId)

      // Write the magic byte and schema version
      out.write(encoding.id)
      out.write(schemaId >> 8)
      out.write(schemaId & 0x00FF)

      // Encode and write the message
      if (writer == null)
        writer = new SpecificDatumWriter[T](message.getSchema)
      writer.write(message, encoder)
      encoder.flush()

      out.toByteArray
    }
    catch {
      case e: Exception =>
        logger.log(Level.SEVERE, "Failed to encode an Avro message", e)
        throw e
    }
  }

}
