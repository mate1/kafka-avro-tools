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
import kafka.consumer.ConsumerConfig

import scala.collection.JavaConverters._

/**
 * Avro consumer configuration, instantiated from a Typesafe Config object containing the
 * standard Kafka consumer configuration keys + some extra keys specific to Avro.
 */
object AvroConsumerConfig {
  final def apply(conf: Config, overrides: Map[String,String] = Map.empty[String,String]): ConsumerConfig = {
    // Apply overrides to config, if any
    val config = if (overrides.nonEmpty) ConfigFactory.parseMap(overrides.asJava).withFallback(conf) else conf

    // Generate Kafka consumer config
    val props = new Properties()
    for (entry: Entry[String, ConfigValue] <- config.entrySet.asScala)
      props.put(entry.getKey, entry.getValue.unwrapped.toString)
    new ConsumerConfig(props)
  }
}
