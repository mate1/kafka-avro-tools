package com.mate1.kafka.avro

import java.util.Map.Entry
import java.util.Properties

import com.typesafe.config.{Config, ConfigValue}
import kafka.producer.ProducerConfig

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created by Marc-Andre Lamothe on 3/26/15.
  */
case class AvroProducerConfig(default_schema_id: Short, encoding: AvroEncoding.Value, schema_repo_url: String)(implicit props: Properties) extends ProducerConfig(props)

object AvroProducerConfig {
  final def apply(config: Config): AvroProducerConfig = {
    // Read Avro producer specific config
    val default_schema_id = Try(config.getInt("avro.default_schema_id").toShort).getOrElse(0.toShort)
    val encoding = Try(AvroEncoding.values.find(_.toString.equalsIgnoreCase(config.getString("avro.encoding"))).get).getOrElse(AvroEncoding.Binary)
    val schema_repo_url = Try(config.getString("avro.schema_repo_url")).getOrElse("")

    // Generate Kafka producer config
    implicit val props = new Properties()
    for (entry: Entry[String, ConfigValue] <- config.entrySet) {
      if (!entry.getKey.startsWith("avro"))
        props.put(entry.getKey, entry.getValue.unwrapped.toString)
    }

    // Generate Avro producer config
    AvroProducerConfig(default_schema_id, encoding, schema_repo_url)
  }
}
