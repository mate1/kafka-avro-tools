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

   Created by Marc-André Lamothe on 3/11/15.
*/

package com.mate1.kafka.avro.fixtures

import com.typesafe.config.ConfigFactory
import org.scalatest.{Suite, SuiteMixin}

import scala.collection.JavaConverters._

trait Config extends SuiteMixin { this: Suite =>

  val consumerConfig = ConfigFactory.parseMap(Map(
    "auto.offset.reset" -> "earliest",
    "bootstrap.servers" -> "localhost:19092",
    "group.id" -> "test",
    "schema.registry.url" -> "http://0.0.0.0:8081"
  ).asJava)

  val producerConfig = ConfigFactory.parseMap(Map(
    "bootstrap.servers" -> "localhost:19092",
    "producer.type" -> "sync",
    "request.required.acks" -> "1",
    "schema.registry.url" -> "http://0.0.0.0:8081"
  ).asJava)

}
