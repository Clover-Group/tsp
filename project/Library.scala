import sbt.Keys._
import sbt._

object Version {

  val flink = "1.3.2"
  val scalaTest = "3.0.4"
}


object Library {

  val flink = Seq(
    "org.apache.flink" %% "flink-scala" % Version.flink % "provided",
    "org.apache.flink" %% "flink-streaming-scala" % Version.flink % "provided")

  val scalaTest = Seq(
    "org.scalactic" %% "scalactic" % Version.scalaTest,
    "org.scalatest" %% "scalatest" % Version.scalaTest % "test"
  )
}
