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
import java.net.InetSocketAddress

import org.apache.commons.io.FileUtils
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait Zookeeper extends BeforeAndAfterAll { this: Suite =>

  private var factory: NIOServerCnxnFactory = _
  private var tmpDir: File = _

  override protected def beforeAll() {
    super.beforeAll()

    println("Test: Initializing Zookeeper")

    tmpDir = new File(System.getProperty("java.io.tmpdir"), "zookeeper")
    FileUtils.deleteDirectory(tmpDir)
    tmpDir.mkdirs()

    factory = new NIOServerCnxnFactory()
    factory.configure(new InetSocketAddress(12181), 1000)
    factory.startup(new ZooKeeperServer(tmpDir, tmpDir, 2000))
  }

  override protected def afterAll() {
    println("Test: Terminating Zookeeper")

    try {
      factory match {
        case factory: NIOServerCnxnFactory =>
          factory.shutdown()
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
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }

    super.afterAll()
  }
}
