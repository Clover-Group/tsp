import sbt.Keys._
import sbt._

object Version {

  val clickhouse = "0.1.34"
  val flink = "1.3.2"
  val scalaTest = "3.0.4"
  val jodaTime = "2.9.9"
  val akkaHttp = "10.1.0-RC1"
  val akkaStreams = "2.5.8"
  val twitterUtilVersion = "6.43.0"
}

object Library {

  val clickhouse = "ru.yandex.clickhouse" % "clickhouse-jdbc" % Version.clickhouse

  val flinkCore = "org.apache.flink" %% "flink-scala" % Version.flink

  val flink = Seq(
    flinkCore,
    "org.apache.flink" %% "flink-streaming-scala" % Version.flink,
    "org.apache.flink" %% "flink-connector-kafka-0.10" % Version.flink,
    "org.apache.flink" % "flink-jdbc" % Version.flink
  )

  val scalaTest = Seq(
    "org.scalactic" %% "scalactic" % Version.scalaTest,
    "org.scalatest" %% "scalatest" % Version.scalaTest % "test"
  )

  val akkaHttp = Seq(
    "com.typesafe.akka" %% "akka-http" % Version.akkaHttp,
    "com.typesafe.akka" %% "akka-http-spray-json" % Version.akkaHttp,
    "com.typesafe.akka" %% "akka-http-testkit" % Version.akkaHttp
  )

  val akkaStreams = "com.typesafe.akka" %% "akka-stream" % Version.akkaStreams

  val twitterUtil = "com.twitter" %% "util-eval" % Version.twitterUtilVersion

  val jodaTime = "joda-time" % "joda-time" % Version.jodaTime
}
