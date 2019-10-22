import sbt._

object Version {
  val logback = "1.2.3"
  val scalaLogging = "3.9.2"

  val config = "1.3.4"

  val influx = "2.15"

  val clickhouse = "0.2"
  val flink = "1.9.0"

  val akka = "2.5.25"
  val akkaHttp = "10.1.9"

  val cats = "2.0.0"

  val scalaTest = "3.0.8"
  val scalaCheck = "1.14.0"
  val jmh = "0.3.7"

  val testContainers = "0.28.0"
  val testContainersKafka = "1.12.1"
  val postgres = "42.2.6"

  val avro = "1.8.2"

  val parboiled = "2.1.8"

  val shapeless = "2.3.3"

  val jackson = "2.9.7"
  val jaxb = "2.3.0"
  val activation = "1.2.0"

  val kindProjector = "0.9.8"

  val simulacrum = "0.15.0"
  val sentry = "1.7.27"

  val arrow = "0.14.1"
  val parquet = "0.11.0"
  val hadoopClient = "3.2.1"

  val twitterUtil = "6.43.0"
}

object Library {

  val jackson = Seq(
    "com.fasterxml.jackson.core" % "jackson-databind" % Version.jackson,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Version.jackson,
    "javax.xml.bind" % "jaxb-api" % Version.jaxb,
    "com.sun.xml.bind" % "jaxb-core" % Version.jaxb,
    "com.sun.xml.bind" % "jaxb-impl" % Version.jaxb,
    "com.sun.activation" % "javax.activation" % Version.activation
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
    "org.apache.flink" % "flink-connector-kafka_2.12" % Version.flink,
    "org.apache.flink" % "flink-jdbc_2.12" % Version.flink,
    "org.apache.flink" % "flink-metrics-dropwizard" % Version.flink,
    "org.apache.flink" % "flink-avro" % Version.flink
  )

  val akka = Seq(
    "com.typesafe.akka" %% "akka-slf4j" % Version.akka,
    "com.typesafe.akka" %% "akka-stream" % Version.akka,
    "com.typesafe.akka" %% "akka-testkit" % Version.akka
  )

  val akkaHttp = Seq(
    "com.typesafe.akka" %% "akka-http" % Version.akkaHttp,
    "com.typesafe.akka" %% "akka-http-spray-json" % Version.akkaHttp,
    "com.typesafe.akka" %% "akka-http-testkit" % Version.akkaHttp
  )

  val cats = Seq(
    "org.typelevel" %% "cats-core" % Version.cats
  )

  val scrum = Seq(
    "com.github.mpilquist" %% "simulacrum" % Version.simulacrum
  )

  val twitterUtil = Seq("com.twitter" %% "util-eval" % Version.twitterUtil)

  val scalaTest = Seq(
    "org.scalactic" %% "scalactic" % Version.scalaTest,
    "org.scalatest" %% "scalatest" % Version.scalaTest % "test",
    "org.scalacheck" %% "scalacheck" % Version.scalaCheck % "test"
  )

  val perf = Seq(
    "pl.project13.scala" %% "sbt-jmh" % Version.testContainers % Version.jmh
  )

  val testContainers = Seq(
    "com.dimafeng" %% "testcontainers-scala" % Version.testContainers % "test",
    "org.testcontainers" % "kafka" % Version.testContainersKafka % "test"
  )

  val parboiled = Seq(
    "org.parboiled" %% "parboiled" % Version.parboiled
  )

  val sentrylog = Seq(
    "io.sentry" %% "sentry-logback" % Version.sentry
  )

  val strawman = Seq(
    "ch.epfl.scala" %% "collection-strawman" % "0.9.0"
  )

  val arrowDeps = Seq(
    "org.apache.arrow" % "arrow-memory" % Version.arrow,
    "org.apache.arrow" % "arrow-vector" % Version.arrow
  )

  val parquetDeps = Seq(
    "com.github.mjakubowski84" %% "parquet4s-core" % Version.parquet,
    "org.apache.hadoop" % "hadoop-client" % Version.hadoopClient
  )

}
