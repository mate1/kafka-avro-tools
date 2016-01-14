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

object Build extends Build {

  // Global build settings
  override lazy val settings = super.settings ++ Seq(
    name         := "KafkaAvroTools",
    version      := "1.0.0",
    organization := "com.mate1",
    scalaVersion := "2.10.4",
    parallelExecution in ThisBuild := false,
    publishArtifact in packageDoc := false,
    publishArtifact in packageSrc := false,
    sources in doc := Seq.empty,
    sourcesInBase := false,
    resolvers ++= Seq(Resolver.mavenLocal,
      "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Mate1 Repository" at "https://raw.github.com/mate1/maven/master/public/"
    ),
    javacOptions ++= Seq("-g:none"),
    scalacOptions ++= Seq("-feature", "-g:none")
    ,credentials += Credentials("Sonatype Nexus Repository Manager", "maven.mate1", "admin", "foobar")
    ,publishTo := Some("Sonatype Nexus Repository Manager" at "http://maven.mate1:8081/nexus/content/repositories/releases/")
  )

  // Scalamail project
  lazy val kafkaAvroUtils = Project("kafka-avro-tools", file("."),
    settings = super.settings ++ Seq(
      libraryDependencies ++= Seq(
        // General dependencies
        "com.mate1.avro" %% "schema-repo-client" % "0.1-SNAPSHOT",
        "com.typesafe" % "config" % "1.2.1",
        "org.apache.avro" % "avro" % "1.7.5",
        "org.apache.kafka" %% "kafka" % "0.8.1" exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri"),

        // Test dependencies
        "commons-io" % "commons-io" % "2.4" % Test,
        "org.scalatest" %% "scalatest" % "2.2.1" % Test
      )
    )

    // Avro compiler settings
    ++ avroSettings ++ Seq(
      sourceDirectory in avroConfig <<= (sourceDirectory in Test)(_ / "resources/avro")
    )
  )
  .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
}
