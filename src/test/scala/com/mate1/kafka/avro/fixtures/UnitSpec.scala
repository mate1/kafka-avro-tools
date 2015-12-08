package com.mate1.kafka.avro.fixtures

import org.scalatest.FlatSpec

import scala.concurrent.duration.Duration

/**
 * Created by Marc-Andre Lamothe on 3/11/15.
 */
trait UnitSpec extends FlatSpec {

  final def wait(duration: Duration): Unit = {
    try {
      Thread.sleep(duration.toMillis)
    }
    catch {
      case e: Exception =>
    }
  }

}
