import sbt.Keys._
import sbt._

object Version {
  val jaxbApi = "2.3.0"
  
  val logback = "1.2.3"
  val scalaLogging = "3.9.0"

  val config = "1.3.3"

  val influx = "2.13"
  val influxCli = "0.6.0"
  val influxFlink = "1.0"

  val clickhouse = "0.1.42"
  val flink = "1.6.1"

  val akka = "2.5.17"
  val akkaHttp = "10.1.5"

  val cats = "1.4.0"

  val twitterUtilVersion = "6.43.0"

  val scalaTest = "3.0.5"
  val scalaCheck = "1.14.0"
  val testContainers = "0.20.0"
  val postgres = "42.2.5"

  val jackson = "2.9.7"

  val spark = "2.3.2"

  val avro = "1.8.2"

  val parboiled = "2.1.5"

  val shapeless = "2.3.3"

  val jaxb = "2.3.0"
  val activation = "1.2.0"
}

object Library {
  
  val javaCore = Seq(
    "javax.xml.bind" % "jaxb-api" % Version.jaxbApi
  )

  val logging = Seq(
    "ch.qos.logback" % "logback-classic" % Version.logback,
    "com.typesafe.scala-logging" %% "scala-logging" % Version.scalaLogging
  )

  val config = Seq(
    "com.typesafe" % "config" % Version.config
  )

  val influx = Seq(
    "org.influxdb" % "influxdb-java" % Version.influx
  )
  val clickhouse = Seq("ru.yandex.clickhouse" % "clickhouse-jdbc" % Version.clickhouse)
  val postgre = Seq("org.postgresql" % "postgresql" % Version.postgres)
  val dbDrivers = influx ++ clickhouse ++ postgre

  val flinkCore = Seq("org.apache.flink" %% "flink-scala" % Version.flink)

  val flink = flinkCore ++ Seq(
    "org.apache.flink" %% "flink-runtime-web" % Version.flink,
    "org.apache.flink" %% "flink-streaming-scala" % Version.flink,
    "org.apache.flink" %% "flink-connector-kafka-0.10" % Version.flink,
    "org.apache.flink" % "flink-jdbc" % Version.flink,
    "org.apache.flink" % "flink-metrics-dropwizard" % Version.flink
  )

  val akka = Seq(
    "com.typesafe.akka" %% "akka-slf4j" % Version.akka,
    "com.typesafe.akka" %% "akka-stream" % Version.akka
  )

  val akkaHttp = Seq(
    "com.typesafe.akka" %% "akka-http" % Version.akkaHttp,
    "com.typesafe.akka" %% "akka-http-spray-json" % Version.akkaHttp,
    "com.typesafe.akka" %% "akka-http-testkit" % Version.akkaHttp
  )

  val cats = Seq(
    "org.typelevel" %% "cats-core" % Version.cats
  )

  val twitterUtil = Seq("com.twitter" %% "util-eval" % Version.twitterUtilVersion)

  val scalaTest = Seq(
    "org.scalactic" %% "scalactic" % Version.scalaTest,
    "org.scalatest" %% "scalatest" % Version.scalaTest % "test",
    "org.scalacheck" %% "scalacheck" % Version.scalaCheck % "test"
  )

  val testContainers = Seq(
    "com.dimafeng" %% "testcontainers-scala" % Version.testContainers % "test"
  )

  val jackson = Seq(
    "com.fasterxml.jackson.core" % "jackson-databind" % Version.jackson,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Version.jackson,
    "javax.xml.bind" % "jaxb-api" % Version.jaxb,
    "com.sun.xml.bind" % "jaxb-core" % Version.jaxb,
    "com.sun.xml.bind" % "jaxb-impl" % Version.jaxb,
    "com.sun.activation" % "javax.activation" % Version.activation,
  )

  val sparkStreaming = Seq(
    "org.apache.spark" %% "spark-core" % Version.spark,
    "org.apache.spark" %% "spark-streaming" % Version.spark,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % Version.spark,
    // Needed for structured streams
    "org.apache.spark" %% "spark-sql-kafka-0-10" % Version.spark,
    "org.apache.spark" %% "spark-sql" % Version.spark
  )

  val kafka = Seq(
    "org.apache.avro" % "avro" % Version.avro
  )

  val parboiled = Seq(
    "org.parboiled" %% "parboiled" % Version.parboiled
  )

  val shapeless = Seq(
    "com.chuusai" %% "shapeless" % Version.shapeless
  )
}
