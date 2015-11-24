package com.mate1.kafka.avro

/**
  * The list of supported Avro encoding formats. Order is important
  *
  * Created by Marc-Andre Lamothe on 11/23/15.
  */
object AvroEncoding extends Enumeration {
  val Binary = Value(0)
  val JSON = Value(1)
}
