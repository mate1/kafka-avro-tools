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

import com.typesafe.config.Config
import org.apache.avro.specific.SpecificRecord

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * A Kafka consumer implementation that processes Avro records from the topic one at a time.
 *
 * Every record will be passed to the consume function as they are read.
 *
 * If any exceptions are thrown by the consume function then the offset of the current record
 * may or may not be committed (if auto-commit is not disabled) and the consumer will stop.
 */
abstract class KafkaAvroConsumer[T <: SpecificRecord](config: Config, topic: String, timeout: Duration = 0 millisecond) extends KafkaAvroBatchConsumer[T](config, topic, 1, timeout) {

  /**
   * Method that gets called each time a new record is ready for processing.
   * @param record the record to process
   */
  protected def consume(record: T): Unit

  /**
   * Method that gets called each time a new batch of records is ready for processing.
   * @param records the record to process
   */
  final override protected def consume(records: Seq[T]): Unit = records.foreach(consume)

}
