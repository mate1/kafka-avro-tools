package com.mate1.kafka.avro

import java.util.Map.Entry
import java.util.Properties

import com.typesafe.config.{ConfigValue, Config}
import kafka.consumer.ConsumerConfig

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created by Marc-Andre Lamothe on 3/26/15.
  */
case class AvroConsumerConfig(schema_repo_url: String)(implicit props: Properties) extends ConsumerConfig(props)

object AvroConsumerConfig {
  final def apply(config: Config): AvroConsumerConfig = {
    // Read Avro consumer specific config
    val schema_repo_url = Try(config.getString("avro.schema_repo_url")).getOrElse("")

    // Generate Kafka consumer config
    implicit val props = new Properties()
    for (entry: Entry[String, ConfigValue] <- config.entrySet) {
      if (!entry.getKey.startsWith("avro"))
        props.put(entry.getKey, entry.getValue.unwrapped.toString)
    }

    // Generate Avro consumer config
    AvroConsumerConfig(schema_repo_url)
  }
}
