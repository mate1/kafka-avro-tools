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

   Created by Marc-André Lamothe on 4/9/15.
*/

package com.mate1.kafka.avro

import com.mate1.avro.repo.client.{AvroSchema, GenericSchemaRepository, ShortId}
import org.apache.avro.Schema

import scala.collection.mutable

/**
 * An implementation of the Mate1 GenericSchemaRepository for AvroSchema objects.
 *
 * Maintains an internal cache of schemas per topic name.
 */
object AvroSchemaRepository {
  private val instances = mutable.Map[String, AvroSchemaRepository]()

  def apply(url: String): AvroSchemaRepository = {
    instances.get(url.trim) match {
      case Some(repo: AvroSchemaRepository) =>
        repo
      case _ =>
        val repo = new AvroSchemaRepository(url.trim)
        instances.put(url.trim, repo)
        repo
    }
  }
}
final class AvroSchemaRepository protected (url: String) extends GenericSchemaRepository[Short, Schema] with AvroSchema with ShortId {
  override protected def getRepositoryURL: String = url
}
