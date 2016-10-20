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

import kafka.utils.VerifiableProperties
import org.apache.avro.Schema
import org.apache.avro.io.{BinaryDecoder, Decoder, DecoderFactory, JsonDecoder}
import org.apache.avro.specific.{SpecificDatumReader, SpecificRecordBase}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.util.Try

/**
 * Abstract class that provides Kafka message to Avro message decoding functionality.
 */
class AvroDecoder[T >: Null <: SpecificRecordBase](props: VerifiableProperties)(implicit tag: ClassTag[T]) extends kafka.serializer.Decoder[T] {

  private var binaryDecoder: BinaryDecoder = _

  private val jsonDecoders = mutable.Map[Short, JsonDecoder]()

  private val messageConstructor = {
    val mirror = universe.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(tag.runtimeClass.asInstanceOf[Class[T]])
    mirror.reflectClass(classSymbol).reflectConstructor(classSymbol.toType.declarations.find({
      case s if s.isMethod =>
        val m = s.asMethod
        m.isConstructor && m.paramss.flatten.isEmpty
      case _ =>
        false
    }).get.asMethod)
  }

  private var reader: SpecificDatumReader[T] = _

  private val schemaRepo = Try(props.getString("avro.schema_repo_url")).filter(_.trim.nonEmpty).map(AvroSchemaRepository.apply).toOption

  private val topic = Try(props.getString("avro.topic_name")).filter(_.trim.nonEmpty).toOption

  private final def getDecoder(data: Array[Byte], encoding: AvroEncoding.Value, schema: Schema, schemaId: Short): Decoder = {
    encoding match {
      case AvroEncoding.Binary =>
        binaryDecoder = DecoderFactory.get.binaryDecoder(data, 3, data.length - 3, binaryDecoder)
        binaryDecoder
      case AvroEncoding.JSON =>
        jsonDecoders.get(schemaId) match {
          case Some(decoder: JsonDecoder) =>
            decoder.configure(new String(data, 3, data.length - 3, "UTF-8"))
          case _ =>
            val decoder = DecoderFactory.get.jsonDecoder(schema, new String(data, 3, data.length - 3, "UTF-8"))
            jsonDecoders.put(schemaId, decoder)
            decoder
        }
    }
  }

  /**
   * Deserializes the binary data from a Kafka message into an Avro message.
   *
   * @param data the data to decode
   * @return a MailMessage if decoded successfully, None otherwise
   */
  override def fromBytes(data: Array[Byte]): T = {
    val message = messageConstructor().asInstanceOf[T]

    // Read the encoding and schema id
    val encoding = AvroEncoding(data(0))
    val schemaId = ((data(1) << 8) | (data(2) & 0xff)).toShort

    // Retrieve the writer's schema from the Avro schema repository
    val schema = schemaRepo.flatMap(repo => topic.flatMap(repo.getSchema(_, schemaId))).getOrElse(message.getSchema)

    // Decode the message
    if (reader == null)
      reader = new SpecificDatumReader[T](message.getSchema)
    reader.setSchema(schema)
    reader.read(message, getDecoder(data, encoding, schema, schemaId))
  }
}
