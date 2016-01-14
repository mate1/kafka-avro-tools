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

   Created by Marc-André Lamothe on 3/26/15.
*/

package com.mate1.kafka.avro

import java.util.Map.Entry
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import kafka.producer.ProducerConfig

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Avro producer configuration, instantiated from a Typesafe Config object containing the
 * standard Kafka producer configuration keys + some extra keys specific to Avro.
 */
case class AvroProducerConfig private(conf: Config, default_schema_id: Short, encoding: AvroEncoding.Value, schema_repo_url: String) {
  final def kafkaProducerConfig(overrides: Map[String,String] = Map.empty[String,String]): ProducerConfig = {
    // Apply overrides to config, if any
    val config = if (overrides.nonEmpty) ConfigFactory.parseMap(overrides.asJava).withFallback(conf) else conf

    // Generate Kafka producer config
    val props = new Properties()
    for (entry: Entry[String, ConfigValue] <- config.entrySet.asScala) {
      if (!entry.getKey.startsWith("avro"))
        props.put(entry.getKey, entry.getValue.unwrapped.toString)
    }
    new ProducerConfig(props)
  }
}

object AvroProducerConfig {
  final def apply(conf: Config): AvroProducerConfig = {
    // Read Avro producer specific config
    val default_schema_id = Try(conf.getInt("avro.default_schema_id").toShort).getOrElse(0.toShort)
    val encoding = Try(AvroEncoding.values.find(_.toString.equalsIgnoreCase(conf.getString("avro.encoding"))).get).getOrElse(AvroEncoding.Binary)
    val schema_repo_url = Try(conf.getString("avro.schema_repo_url")).getOrElse("")

    // Generate Avro producer config
    new AvroProducerConfig(conf, default_schema_id, encoding, schema_repo_url)
  }
}
