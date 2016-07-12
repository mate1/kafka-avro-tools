package com.mate1.kafka.avro.fixtures

import java.util.Properties

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel
import io.confluent.kafka.schemaregistry.rest.{SchemaRegistryConfig, SchemaRegistryRestApplication}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Created by malamothe on 7/11/16.
 */
trait SchemaRegistry extends BeforeAndAfterAll { this: Suite =>

  private var server: SchemaRegistryRestApplication = _

  override protected def beforeAll() {
    super.beforeAll()

    println("Test: Initializing Schema Registry")

    val prop = new Properties()
    prop.put(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, "127.0.0.1:12181")
    prop.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC)
    prop.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, AvroCompatibilityLevel.NONE.name)
    prop.put(SchemaRegistryConfig.MASTER_ELIGIBILITY, "true")

    server = new SchemaRegistryRestApplication(prop)
    server.createServer().start()
  }

  override protected def afterAll() {
    println("Test: Terminating Schema Registry")

    try {
      server match {
        case server: SchemaRegistryRestApplication =>
          server.stop()
          server.join()
        case _ =>
      }
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }

    super.afterAll()
  }
}

