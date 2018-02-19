import sbt.Keys._
import sbt._

object Version {
  val logback = "1.2.3"
  val scalaLogging = "3.7.2"

  val clickhouse = "0.1.34"
  val flink = "1.4.0"

  val akka = "2.4.20"
  val akkaHttp = "10.0.11"

  val cats = "1.0.1"

  val jodaTime = "2.9.9"
  val twitterUtilVersion = "6.43.0"

  val scalaTest = "3.0.4"
  val testContainers = "0.13.0"
  val testContainersPgsql = "1.5.1"
  val pgsql = "42.2.0"

  val jackson = "2.9.4"

  val spark = "2.2.1"
}

object Library {

  val logging = Seq(
    "ch.qos.logback" % "logback-classic" % Version.logback,
    "com.typesafe.scala-logging" %% "scala-logging" % Version.scalaLogging
  )

  val clickhouse = Seq("ru.yandex.clickhouse" % "clickhouse-jdbc" % Version.clickhouse)

  val flinkCore = Seq("org.apache.flink" %% "flink-scala" % Version.flink)

  val flink = flinkCore ++ Seq(
    "org.apache.flink" %% "flink-streaming-scala" % Version.flink,
    "org.apache.flink" %% "flink-connector-kafka-0.10" % Version.flink,
    "org.apache.flink" % "flink-jdbc" % Version.flink
  )

  val akka = Seq(
    //    "com.typesafe.spark" %% "spark-actor" % Version.spark,
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

  val jodaTime = Seq("joda-time" % "joda-time" % Version.jodaTime)

  val scalaTest = Seq(
    "org.scalactic" %% "scalactic" % Version.scalaTest,
    "org.scalatest" %% "scalatest" % Version.scalaTest % "test"
  )

  val testContainers = Seq(
    "com.dimafeng" %% "testcontainers-scala" % Version.testContainers % "test"
  )

  val jackson = Seq(
    "com.fasterxml.jackson.core" % "jackson-databind" % Version.jackson,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Version.jackson
  )

  val sparkStreaming = Seq(
    "org.apache.spark" %% "spark-core" % Version.spark,
    "org.apache.spark" %% "spark-streaming" % Version.spark,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % Version.spark,
    // Needed for structured streams
    "org.apache.spark" %% "spark-sql-kafka-0-10" % Version.spark,
    "org.apache.spark" %% "spark-sql" % Version.spark
  )

}
