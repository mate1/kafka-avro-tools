package com.mate1.kafka.avro

import java.util.Map.Entry
import java.util.Properties

import com.typesafe.config.{Config, ConfigValue}
import kafka.producer.ProducerConfig

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created by Marc-Andre Lamothe on 3/26/15.
  */
case class AvroProducerConfig private(conf: Config, default_schema_id: Short, encoding: AvroEncoding.Value, schema_repo_url: String) {
  final def generateProducerConfig(overrides: Option[Config] = None): ProducerConfig = {
    // Apply overrides to config, if any
    val config = overrides.map(_.withFallback(conf)).getOrElse(conf)

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
