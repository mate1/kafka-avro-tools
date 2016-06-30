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

   Created by Marc-André Lamothe on 3/3/15.
*/

package com.mate1.kafka.avro.fixtures

import java.io.File
import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, Suite}

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
