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
*/

import sbt.Keys._
import sbt._
import sbtavro.SbtAvro._
import sbtavro.SbtAvro.avroSettings._

object Build extends Build {

  // Global build settings
  override lazy val settings = super.settings ++ Seq(
    name         := "KafkaAvroTools",
    version      := "1.1.0",
    organization := "com.mate1",
    scalaVersion := "2.10.4",
    parallelExecution in ThisBuild := false,
    publishArtifact in packageDoc := false,
    publishArtifact in packageSrc := false,
    publishArtifact in GlobalScope in Test := true,
    sources in doc := Seq.empty,
    sourcesInBase := false,
    resolvers ++= Seq(Resolver.mavenLocal,
      "Confluent Repository" at "http://packages.confluent.io/maven/",
      "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Mate1 Repository" at "https://raw.github.com/mate1/maven/master/public/"
    ),
    javacOptions ++= Seq("-g:none"),
    scalacOptions ++= Seq("-feature", "-g:none")
  )

  // Scalamail project
  lazy val kafkaAvroUtils = Project("kafka-avro-tools", file("."),
    settings = super.settings ++ Seq(
      libraryDependencies ++= Seq(
        // General dependencies
        "com.typesafe" % "config" % "1.2.1",
        "io.confluent" % "kafka-avro-serializer" % "3.0.0",
        "org.apache.avro" % "avro" % "1.7.7",
        "org.apache.kafka" %% "kafka" % "0.10.0.0-cp1" % Provided,
        "org.apache.kafka" % "kafka-clients" % "0.10.0.0-cp1",

        // Test dependencies
        "commons-io" % "commons-io" % "2.4" % Test,
        "org.scalatest" %% "scalatest" % "2.2.1" % Test,
        "io.confluent" % "kafka-schema-registry" % "3.0.0" % Test exclude("org.apache.kafka", "kafka_2.11")
      ),

      (version in avroConfig) := "1.7.7"
    )

    // Avro compiler settings
    ++ avroSettings ++ Seq(
      sourceDirectory in avroConfig <<= (sourceDirectory in Test)(_ / "resources/avro")
    )
  )
  .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
}
