package com.mate1.kafka.avro

import org.apache.avro.specific.SpecificRecord

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

/**
  * Class that reads and decodes Avro messages from a Kafka queue.
  *
  * Created by Marc-Andre Lamothe on 2/24/15.
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
