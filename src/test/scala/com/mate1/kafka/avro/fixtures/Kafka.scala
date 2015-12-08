package com.mate1.kafka.avro.fixtures

import java.io.File
import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Created by Marc-Andre Lamothe on 3/3/15.
 */
trait Kafka extends BeforeAndAfterAll { this: Suite =>

  private var server: KafkaServer = _
  private var tmpDir: File = _

  override protected def beforeAll() {
    super.beforeAll()

    println("Test: Initializing Kafka")

    tmpDir = new File(System.getProperty("java.io.tmpdir"), "kafka")
    FileUtils.deleteDirectory(tmpDir)
    tmpDir.mkdirs()

    val props = new Properties()
    props.setProperty("host.name", "localhost")
    props.setProperty("port", "19092")
    props.setProperty("broker.id", "0")
    props.setProperty("log.dirs", tmpDir.getAbsolutePath)
    props.setProperty("zookeeper.connect", "localhost:12181")

    server = new KafkaServer(new KafkaConfig(props))
    server.startup()
  }

  override protected def afterAll() {
    println("Test: Terminating Kafka")

    try {
      server match {
        case server: KafkaServer =>
          server.shutdown()
          server.awaitShutdown()
        case _ =>
      }
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }

    try {
      tmpDir match {
        case file: File =>
          FileUtils.deleteDirectory(file)
        case _ =>
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    super.afterAll()
  }
}
