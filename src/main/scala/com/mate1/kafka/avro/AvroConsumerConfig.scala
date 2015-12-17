package com.mate1.kafka.avro

import java.util.Map.Entry
import java.util.Properties

import com.typesafe.config.{Config, ConfigValue}
import kafka.consumer.ConsumerConfig

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created by Marc-Andre Lamothe on 3/26/15.
  */
case class AvroConsumerConfig private (conf: Config, schema_repo_url: String) {
  final def generateConsumerConfig(overrides: Option[Config] = None): ConsumerConfig = {
    // Apply overrides to config, if any
    val config = overrides.map(_.withFallback(conf)).getOrElse(conf)

    // Generate Kafka consumer config
    val props = new Properties()
    for (entry: Entry[String, ConfigValue] <- config.entrySet.asScala) {
      if (!entry.getKey.startsWith("avro"))
        props.put(entry.getKey, entry.getValue.unwrapped.toString)
    }
    new ConsumerConfig(props)
  }
}

object AvroConsumerConfig {
  final def apply(conf: Config): AvroConsumerConfig = {
    // Read Avro consumer specific config
    val schema_repo_url = Try(conf.getString("avro.schema_repo_url")).getOrElse("")

    // Generate Avro consumer config
    new AvroConsumerConfig(conf, schema_repo_url)
  }
}
