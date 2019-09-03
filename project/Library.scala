import sbt.Keys._
import sbt._

object Version {
  val logback = "1.2.3"
  val scalaLogging = "3.9.2"

  val config = "1.3.4"

  val influx = "2.15"

  val clickhouse = "0.1.54"
  val flink = "1.9.0"

  val akka = "2.5.25"
  val akkaHttp = "10.1.9"

  val cats = "2.0.0-RC2"

  val scalaTest = "3.0.8"
  val scalaCheck = "1.14.0"
  val jmh = "0.3.7"

  val testContainers = "0.30.0"
  val postgres = "42.2.6"

  val arrow = "0.14.1"

  val parboiled = "2.1.8"

  val jackson = "2.10.0.pr1"
  val jaxbCore = "2.3.0.1"
  val jaxbApi = "2.3.1"
  val jaxbImpl = "2.3.2"
  val activation = "1.2.0"

  val sentry = "1.7.27"

  val SemanticDB = "4.2.4"
}

object Library {

  val jackson = Seq(
    "com.fasterxml.jackson.core" % "jackson-databind" % Version.jackson,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Version.jackson
    //"javax.xml.bind" % "jaxb-api" % Version.jaxbApi,
    //"com.sun.xml.bind" % "jaxb-core" % Version.jaxbCore,
    //"com.sun.xml.bind" % "jaxb-impl" % Version.jaxbImpl,
    //"com.sun.activation" % "javax.activation" % Version.activation
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

  val clickhouse = Seq(
    ("ru.yandex.clickhouse" % "clickhouse-jdbc" % Version.clickhouse % "compile")
      .exclude("net.jpountz.lz4", "lz4")
  )

  val postgre = Seq("org.postgresql" % "postgresql" % Version.postgres)
  val dbDrivers = influx ++ clickhouse ++ postgre

  val flinkCore = Seq("org.apache.flink" %% "flink-scala" % Version.flink)

  val flink = flinkCore ++ Seq(
      //"org.apache.flink" %% "flink-runtime-web" % Version.flink,
      "org.apache.flink" %% "flink-streaming-scala" % Version.flink,
      "org.apache.flink" %% "flink-connector-kafka" % Version.flink,
      "org.apache.flink" % "flink-jdbc_2.12" % Version.flink
      //"org.apache.flink" % "flink-metrics-dropwizard" % Version.flink
    )

  val arrow = Seq(
    //"org.apache.arrow" % "arrow-format" % Version.arrow,
    //"org.apache.arrow" % "arrow-memory" % Version.arrow
    //exclude ("io.netty", "netty-common")
    //exclude ("io.netty", "netty-buffer"),
    ("org.apache.arrow" % "arrow-vector" % Version.arrow)
      .excludeAll(ExclusionRule("io.netty", "netty-common"))
      .excludeAll(ExclusionRule("io.netty", "netty-buffer"))
    //exclude ("com.fasterxml.jackson.core", "jackson-databind")
    //exclude ("io.netty", "netty-common")
    //exclude ("io.netty", "netty-buffer")
    //exclude ("org.apache.arrow", "arrow-format")
    //exclude ("org.apache.arrow", "arrow-memory")
  )

  val akka = Seq(
    "com.typesafe.akka" %% "akka-slf4j" % Version.akka,
    ("com.typesafe.akka" %% "akka-stream" % Version.akka % "compile")
      .excludeAll(ExclusionRule("org.apache.arrow", "arrow-vector"))
      .excludeAll(ExclusionRule("org.apache.arrow", "arrow-memory"))
  )

  val akkaHttp = Seq(
    ("com.typesafe.akka" %% "akka-http" % Version.akkaHttp % "compile")
      .excludeAll(ExclusionRule("org.apache.arrow", "arrow-vector"))
      .excludeAll(ExclusionRule("org.apache.arrow", "arrow-memory")),
    ("com.typesafe.akka" %% "akka-http-spray-json" % Version.akkaHttp % "compile")
      .excludeAll(ExclusionRule("org.apache.arrow", "arrow-vector"))
      .excludeAll(ExclusionRule("org.apache.arrow", "arrow-memory")),
    ("com.typesafe.akka" %% "akka-http-testkit" % Version.akkaHttp % "compile")
      .excludeAll(ExclusionRule("org.apache.arrow", "arrow-vector"))
      .excludeAll(ExclusionRule("org.apache.arrow", "arrow-memory"))
  )

  val cats = Seq(
    "org.typelevel" %% "cats-core" % Version.cats
  )

  val scalaTest = Seq(
    "org.scalactic" %% "scalactic" % Version.scalaTest,
    "org.scalatest" %% "scalatest" % Version.scalaTest % "test",
    "org.scalacheck" %% "scalacheck" % Version.scalaCheck % "test"
  )

  //val perf = Seq(
  //  "pl.project13.scala" %% "sbt-jmh" % Version.testContainers % Version.jmh
  //)

  val testContainers = Seq(
    "com.dimafeng" %% "testcontainers-scala" % Version.testContainers % "test"
  )

  val parboiled = Seq(
    "org.parboiled" %% "parboiled" % Version.parboiled
  )

  val sentrylog = Seq(
    "io.sentry" %% "sentry-logback" % Version.sentry
  )

  //val semantic = Seq(
  //  "org.scalameta" %% "semanticdb-scalac" % "4.2.3"
  //)

}
