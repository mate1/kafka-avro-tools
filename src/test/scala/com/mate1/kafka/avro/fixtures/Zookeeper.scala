package com.mate1.kafka.avro.fixtures

import java.io.File
import java.net.InetSocketAddress

import org.apache.commons.io.FileUtils
import org.apache.zookeeper.server.{NIOServerCnxn, ZooKeeperServer}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Created by Marc-Andre Lamothe on 3/3/15.
 */
trait Zookeeper extends BeforeAndAfterAll { this: Suite =>

  private var factory: NIOServerCnxn.Factory = _
  private var tmpDir: File = _

  override protected def beforeAll() {
    super.beforeAll()

    println("Test: Initializing Zookeeper")

    tmpDir = new File(System.getProperty("java.io.tmpdir"), "zookeeper")
    FileUtils.deleteDirectory(tmpDir)
    tmpDir.mkdirs()

    factory = new NIOServerCnxn.Factory(new InetSocketAddress(12181), 5000)
    factory.startup(new ZooKeeperServer(tmpDir, tmpDir, 2000))
  }

  override protected def afterAll() {
    println("Test: Terminating Zookeeper")

    try {
      factory match {
        case factory: NIOServerCnxn.Factory =>
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
