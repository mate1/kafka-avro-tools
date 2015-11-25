import sbt._
import sbt.Keys._

object Build extends Build {

  // Global build settings
  override lazy val settings = super.settings ++ Seq(
    name         := "KafkaAvroUtils",
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
  )

  // Scalamail project
  lazy val kafkaAvroUtils = Project("kafka-avro-utils", file("."),
    settings = super.settings ++ Seq(
      libraryDependencies ++= Seq(
        // General dependencies
        "com.mate1.avro" %% "schema-repo-client" % "0.1-SNAPSHOT",
        "com.typesafe" % "config" % "1.2.1",
        "org.apache.avro" % "avro" % "1.7.5",
        "org.apache.kafka" %% "kafka" % "0.8.1" exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri"),

        // Test dependencies
        "org.scalatest" %% "scalatest" % "2.2.1" % Test
      )
    )
  )
  .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
}
