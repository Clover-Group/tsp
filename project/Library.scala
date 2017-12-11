import sbt.Keys._
import sbt._

object Version {

  val clickhouse = "0.1.34"
  val flink = "1.3.2"
  val scalaTest = "3.0.4"
  val jodaTime = "2.9.9"
}

object Library {

  val clickhouse = "ru.yandex.clickhouse" % "clickhouse-jdbc" % Version.clickhouse

  val flink = Seq(
    "org.apache.flink" %% "flink-scala" % Version.flink % "provided",
    "org.apache.flink" %% "flink-streaming-scala" % Version.flink % "provided",
    "org.apache.flink" %% "flink-connector-kafka-0.10" % Version.flink,
    "org.apache.flink" % "flink-jdbc" % Version.flink
  )

  val scalaTest = Seq(
    "org.scalactic" %% "scalactic" % Version.scalaTest,
    "org.scalatest" %% "scalatest" % Version.scalaTest % "test"
  )

  val jodaTime = "joda-time" % "joda-time" % Version.jodaTime
}
