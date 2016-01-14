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

   Created by Marc-André Lamothe on 2/24/15.
*/

package com.mate1.kafka.avro

import org.apache.avro.specific.SpecificRecord

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

/**
 * A Kafka consumer implementation that processes Avro messages from the topic one at a time.
 *
 * Every message will be passed to the consume function as they are read.
 *
 * If any exceptions are thrown by the consume function then the offset of the current message
 * may or may not be committed (if auto-commit is not disabled) and the consumer will stop.
 */
abstract class KafkaAvroConsumer[T <: SpecificRecord](config: AvroConsumerConfig, topic: String, message: T)(implicit tag: ClassTag[T])
  extends KafkaAvroBatchConsumer[T](config, topic, Seq(message), 0 millisecond) {

  /**
   * Method that gets called each time a new message is ready for processing.
   * @param message the message to process
   */
  protected def consume(message: T): Unit

  /**
   * Method that gets called each time a new batch of messages is ready for processing.
   * @param messages the message to process
   */
  final override protected def consume(messages: Seq[T]): Unit = consume(messages.head)

}
