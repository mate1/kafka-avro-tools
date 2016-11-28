import sbtavro.SbtAvro._

name := "KafkaAvroTools"
organization := "com.mate1"
version := "1.1.0"
scalaVersion := "2.11.8"

javacOptions ++= Seq("-g:none")
scalacOptions ++= Seq("-feature", "-g:none")
parallelExecution in ThisBuild := false
publishArtifact in packageDoc := false
publishArtifact in packageSrc := false
publishArtifact in GlobalScope in Test := true
resolvers ++= Seq(
  "Confluent Repository" at "http://packages.confluent.io/maven/",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)
sources in doc := Seq.empty
sourcesInBase := false

lazy val `kafka-avro-tools` = project.in(file("."))
  .settings(avroSettings)
  .settings(
    libraryDependencies ++= Seq(
      // General dependencies
      "com.typesafe" % "config" % "1.3.1",
      "io.confluent" % "kafka-avro-serializer" % "3.1.1",
      "org.apache.avro" % "avro" % "1.8.1",
      "org.apache.kafka" %% "kafka" % "0.10.0.1-cp1" % Provided,
      "org.apache.kafka" % "kafka-clients" % "0.10.0.1-cp1",

      // Test dependencies
      "commons-io" % "commons-io" % "2.5" % Test,
      "org.scalatest" %% "scalatest" % "3.0.1" % Test,
      "io.confluent" % "kafka-schema-registry" % "3.1.1" % Test
    ),

    // Avro compiler settings
    (version in avroConfig) := "1.8.1",
    sourceDirectory in avroConfig <<= (sourceDirectory in Test)(_ / "avro")
  )