package com.mate1.kafka.avro

import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.consumer.{Consumer, ConsumerConnector, ConsumerTimeoutException}
import org.apache.avro.Schema
import org.apache.avro.io.{BinaryDecoder, Decoder, DecoderFactory, JsonDecoder}
import org.apache.avro.specific.{SpecificDatumReader, SpecificRecord}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Success, Failure, Try}

/**
  * Class that reads and decodes Avro messages from a Kafka queue.
  *
  * Created by Marc-Andre Lamothe on 2/24/15.
  */
abstract class KafkaAvroConsumer[T <: SpecificRecord](config: AvroConsumerConfig, topic: String, message: T)(implicit tag: ClassTag[T]) extends Runnable with LazyLogging {

  /**
    * Whether this consumer thread is still running.
    */
  private val active = new AtomicBoolean(false)

  /**
    * Kafka consumer connector.
    */
  private var consumer: ConsumerConnector = _

  /**
    * Last used decoder, for each schema version.
    */
  private val decoders = mutable.Map[Short, Decoder]()

  /**
    * The Avro message reader.
    */
  private val reader = new SpecificDatumReader[T](tag.runtimeClass.asInstanceOf[Class[T]])

  /**
    * Whether this consumer was stopped.
    */
  private val stopped = new AtomicBoolean(false)

  /**
    * Commits the offset the last message consumed from the the queue.
    */
  protected def commitOffsets(): Unit = {
    if (!stopped.get() && active.get())
      consumer.commitOffsets
  }

  /**
    * Method that gets called each time a new message is ready for processing.
    * @param message the message to process
    */
  protected def consume(message: T): Unit

  /**
    * @param data the data to decode
    * @return a MailMessage if decoded successfully, None otherwise
    */
  private final def deserializeMessage(data: Array[Byte]): Option[T] = Try {
    logger.debug(getClass.getSimpleName + " de-serializing message")

    // Read the encoding and schema id
    val encoding = AvroEncoding(data(0))
    val schemaId = ((data(1) << 8) | (data(2) & 0xff)).toShort

    // Retrieve the writer's schema from the Avro schema repository
    val schema = Try(AvroSchemaRepository(config.schema_repo_url.trim).getSchema(topic, schemaId).get) match {
      case Success(schema: Schema) => schema
      case Failure(e: Throwable) =>
        if (config.schema_repo_url.trim.nonEmpty) {
          logger.error(getClass.getSimpleName + s" failed to recover version $schemaId of schema for topic $topic from repository, proceeding with current schema version!")
          logger.error("An exception occurred:", e)
        }
        message.getSchema
    }
    reader.setSchema(schema)

    // Decoder configuration
    decoders.get(schemaId) match {
      case Some(oldDecoder: BinaryDecoder) =>
        decoders.put(schemaId, DecoderFactory.get().binaryDecoder(data, 3, data.length - 3, oldDecoder))
      case Some(oldDecoder: JsonDecoder) =>
        decoders.put(schemaId, oldDecoder.configure(new String(data, 3, data.length - 3, "UTF-8")))
      case _ =>
        encoding match {
          case AvroEncoding.Binary =>
            decoders.put(schemaId, DecoderFactory.get().binaryDecoder(data, 3, data.length - 3, null))
          case AvroEncoding.JSON =>
            decoders.put(schemaId, DecoderFactory.get().jsonDecoder(schema, new String(data, 3, data.length - 3, "UTF-8")))
        }
    }

    // Decode the message
    Some(reader.read(message, decoders(schemaId)))
  } match {
    case Failure(e: Exception) =>
      logger.error("An exception occurred:", e)
      None
    case Failure(e: Throwable) =>
      throw e
    case Success(result: Option[T]) =>
      result
  }

  /**
    * @return the group used by this consumer
    */
  final def getGroup: String = config.groupId

  /**
    * @return the topic targeted by this consumer
    */
  final def getTopic: String = topic

  /**
    * @return whether this consumer is still active or not
    */
  def isActive: Boolean = active.get()

  /**
    * Method that gets called when the consumer is starting.
    */
  protected def onStart(): Unit

  /**
    * Method that gets called when the consumer is stopping.
    */
  protected def onStop(): Unit

  /**
    * Method that gets called when the consumer has stopped.
    */
  protected def onStopped(): Unit

  /**
    * Kafka consumer thread.
    */
  @Override
  final def run(): Unit = {
    Try {
      logger.debug(getClass.getSimpleName + " initializing")

      // Initialize consumer
      consumer = Consumer.create(config)

      // Update status
      active.set(true)
      onStart()

      // Initialize message iterator
      val iterator = consumer.createMessageStreams(Map(topic -> 1))(topic).head.iterator()
      while (iterator.hasNext()) {
        // Get the next message and de-serialize it
        val message = deserializeMessage(iterator.next().message())
        if (message.isDefined)
          consume(message.get)
      }
    } match {
      case Failure(e: Throwable) =>
        if (!e.isInstanceOf[ConsumerTimeoutException])
          logger.error("An exception occurred:", e)
        // Stop the consumer
        stop()
      case _ =>
    }

    // Update status
    active.set(false)
    onStopped()
  }

  /**
    * Stops the consumer.
    */
  final def stop(): Unit = {
    if (!stopped.getAndSet(true)) {
      logger.debug(getClass.getSimpleName + " stopping")

      onStop()

      if (active.get())
        consumer.shutdown()
    }
  }

  /**
    * Wait for the consumer to have shutdown completely if a call to stop was made, otherwise this does nothing.
    */
  @tailrec
  final def waitUntilStopped(): Unit = {
    if (stopped.get() && active.get()) {
      Try(Thread.sleep(100))
      waitUntilStopped()
    }
  }
}
