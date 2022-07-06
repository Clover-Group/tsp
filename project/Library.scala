import sbt._

object Version {
  val logback = "1.2.3"
  val scalaLogging = "3.9.2"
  val logbackContrib = "0.1.5"

  val config = "1.3.4"

  val influx = "2.15"

  val clickhouse = "0.3.0"

  val akka = "2.5.25"
  val akkaHttp = "10.1.9"

  val cats = "3.3.12"
  val fs2 = "3.2.8"
  val fs2Kafka = "2.4.0"
  val doobie = "1.0.0-RC2"

  val scalaTest = "3.0.8"
  val scalaCheck = "1.14.0"
  val jmh = "0.3.7"

  val testContainers = "0.38.8"
  val testContainersKafka = "1.16.0"
  val postgres = "42.2.6"

  val avro = "1.8.2"

  val parboiled = "2.1.8"

  val shapeless = "2.3.3"

  val jackson = "2.10.0"
  val jaxb = "2.3.0"
  val activation = "1.2.0"

  val kindProjector = "0.9.8"

  val simulacrum = "0.15.0"
  val sentry = "1.7.27"

  val arrow = "0.15.1"
  val parquet = "0.11.0"
  val hadoopClient = "3.2.1"
  val parquetCodecs = "1.10.1"
  val brotli = "0.1.1"

  val twitterUtil = "6.43.0"

  val jolVersion = "0.9"

  val strawmanVersion = "0.9.0"

  val redissonVersion = "3.11.6"

  val akkaHttpMetrics = "0.6.0"

  val swayDB = "0.16.2"

}

object Library {

  val jackson: Seq[ModuleID] = Seq(
    "com.fasterxml.jackson.core" % "jackson-databind" % Version.jackson,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Version.jackson,
    "javax.xml.bind" % "jaxb-api" % Version.jaxb,
    "com.sun.xml.bind" % "jaxb-core" % Version.jaxb,
    "com.sun.xml.bind" % "jaxb-impl" % Version.jaxb,
    "com.sun.activation" % "javax.activation" % Version.activation
  )

  val logging: Seq[ModuleID] = Seq(
    "ch.qos.logback" % "logback-classic" % Version.logback,
    "com.typesafe.scala-logging" %% "scala-logging" % Version.scalaLogging,
    "ch.qos.logback.contrib" % "logback-jackson" % Version.logbackContrib,
    "ch.qos.logback.contrib" % "logback-json-classic" % Version.logbackContrib
  )

  val config: Seq[ModuleID] = Seq(
    "com.typesafe" % "config" % Version.config
  )

  val influx: Seq[ModuleID] = Seq(
    "org.influxdb" % "influxdb-java" % Version.influx
  )
  val clickhouse: Seq[ModuleID] = Seq("ru.yandex.clickhouse" % "clickhouse-jdbc" % Version.clickhouse)
  val postgre: Seq[ModuleID] = Seq("org.postgresql" % "postgresql" % Version.postgres)
  val dbDrivers: Seq[ModuleID] = influx ++ clickhouse ++ postgre

  val akka: Seq[ModuleID] = Seq(
    "com.typesafe.akka" %% "akka-slf4j" % Version.akka,
    "com.typesafe.akka" %% "akka-stream" % Version.akka,
    "com.typesafe.akka" %% "akka-testkit" % Version.akka
  )

  val akkaHttp: Seq[ModuleID] = Seq(
    "com.typesafe.akka" %% "akka-http" % Version.akkaHttp,
    "com.typesafe.akka" %% "akka-http-spray-json" % Version.akkaHttp,
    "com.typesafe.akka" %% "akka-http-testkit" % Version.akkaHttp,
    "fr.davit" %% "akka-http-metrics-prometheus" % Version.akkaHttpMetrics
  )

  val cats: Seq[ModuleID] = Seq(
    "org.typelevel" %% "cats-effect-kernel" % Version.cats,
    "org.typelevel" %% "cats-effect" % Version.cats
  )

  val fs2: Seq[ModuleID] = Seq(
    "co.fs2" %% "fs2-core" % Version.fs2
  )

  val fs2Kafka: Seq[ModuleID] = Seq(
    "com.github.fd4s" %% "fs2-kafka" % Version.fs2Kafka
  )

  val doobie: Seq[ModuleID] = Seq(
    "org.tpolecat" %% "doobie-core" % Version.doobie
  )

  val scrum: Seq[ModuleID] = Seq(
    "com.github.mpilquist" %% "simulacrum" % Version.simulacrum
  )

  val twitterUtil: Seq[ModuleID] = Seq("com.twitter" %% "util-eval" % Version.twitterUtil)

  val scalaTest: Seq[ModuleID] = Seq(
    "org.scalactic" %% "scalactic" % Version.scalaTest,
    "org.scalatest" %% "scalatest" % Version.scalaTest % "test",
    "org.scalacheck" %% "scalacheck" % Version.scalaCheck % "test"
  )

  val perf: Seq[ModuleID] = Seq(
    "pl.project13.scala" %% "sbt-jmh" % Version.testContainers % Version.jmh
  )

  val testContainers: Seq[ModuleID] = Seq(
    "com.dimafeng" %% "testcontainers-scala" % Version.testContainers % "test",
    "org.testcontainers" % "kafka" % Version.testContainersKafka % "test"
  )

  val parboiled: Seq[ModuleID] = Seq(
    "org.parboiled" %% "parboiled" % Version.parboiled
  )

  val sentrylog: Seq[ModuleID] = Seq(
    "io.sentry" %% "sentry-logback" % Version.sentry
  )

  val strawman: Seq[ModuleID] = Seq(
    "ch.epfl.scala" %% "collection-strawman" % Version.strawmanVersion
  )

  val arrowDeps: Seq[ModuleID] = Seq(
    "org.apache.arrow" % "arrow-memory" % Version.arrow,
    "org.apache.arrow" % "arrow-vector" % Version.arrow
  )

  val parquetDeps: Seq[ModuleID] = Seq(
    "org.apache.hadoop" % "hadoop-client" % Version.hadoopClient exclude (
      "org.slf4j", "slf4j-log4j12"
    ) exclude ("log4j", "log4j"),
    "org.apache.hadoop" % "hadoop-common" % Version.hadoopClient exclude (
      "org.slf4j", "slf4j-log4j12"
    ) exclude ("log4j", "log4j"),
    "org.apache.parquet" % "parquet-hadoop" % Version.parquetCodecs exclude (
      "org.slf4j", "slf4j-log4j12"
    ) exclude ("log4j", "log4j"),
    "com.github.rdblue" % "brotli-codec" % Version.brotli
  )

  val jol: Seq[ModuleID] = Seq(
    "org.openjdk.jol" % "jol-core" % Version.jolVersion
  )

  val redisson: Seq[ModuleID] = Seq(
    "org.redisson" % "redisson" % Version.redissonVersion
  )

  val swayDB: Seq[ModuleID] = Seq(
    "io.swaydb" %% "swaydb" % Version.swayDB,
    "io.swaydb" %% "boopickle" % Version.swayDB
  )

}
