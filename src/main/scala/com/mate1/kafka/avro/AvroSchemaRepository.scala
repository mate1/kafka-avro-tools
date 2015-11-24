package com.mate1.kafka.avro

import com.mate1.avro.repo.client.{AvroSchema, GenericSchemaRepository, ShortId}
import org.apache.avro.Schema

import scala.collection.mutable

/**
  * An GenericSchemaRepository implementation for AvroSchema objects.
  * Each instance maintains an internal cache of schemas.
  *
  * Created by Marc-Andre Lamothe on 4/9/15.
  */
object AvroSchemaRepository {
  private val instances = mutable.Map[String, AvroSchemaRepository]()

  def apply(url: String): AvroSchemaRepository = {
    instances.get(url) match {
      case Some(repo: AvroSchemaRepository) =>
        repo
      case _ =>
        val repo = new AvroSchemaRepository(url)
        instances.put(url, repo)
        repo
    }
  }
}
final class AvroSchemaRepository protected (url: String) extends GenericSchemaRepository[Short, Schema] with AvroSchema with ShortId {
  override protected def getRepositoryURL: String = url
}
