package com.mate1.kafka.avro.fixtures

import java.io.File

import com.typesafe.config.ConfigFactory
import org.scalatest.{Suite, SuiteMixin}

import scala.collection.JavaConverters._

/**
 * Created by Marc-Andre Lamothe on 3/11/15.
 */
trait Config extends SuiteMixin { this: Suite =>

  final def loadConfig(): com.typesafe.config.Config = {
    ConfigFactory.parseFile(new File("src/test/resources/test.conf")).resolve()
  }

  final def loadConfig(overrides: Map[String, String]): com.typesafe.config.Config = {
    ConfigFactory.parseMap(overrides.asJava).withFallback(loadConfig()).resolve()
  }
}
