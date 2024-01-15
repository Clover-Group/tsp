import sbt._

object Version {
  val logback = "1.4.14"
  val scalaLogging = "3.9.5"
  val logbackContrib = "0.1.5"

  val config = "1.4.3"

  val influx = "2.15"

  val clickhouse = "0.5.0"

  val akka = "2.8.5"
  val akkaHttp = "10.5.3"
  val sprayJson = "1.3.6"

  val cats = "3.5.2"
  val fs2 = "3.9.3"
  val fs2Kafka = "3.2.0"
  val doobie = "1.0.0-RC5"

  val scalaTest = "3.2.17"
  val scalaCheck = "3.2.17.0"
  val jmh = "0.3.7"

  val testContainers = "0.41.0"
  val testContainersKafka = "1.19.3"
  val postgres = "42.7.1"

  val avro = "1.8.2"

  val parboiled = "2.5.1"

  val shapeless = "2.3.3"

  val jackson = "2.16.1"
  val jaxb = "4.0.4"
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

  val jolVersion = "0.17"

  val strawmanVersion = "0.9.0"

  val redissonVersion = "3.25.2"
  val kryoVersion = "5.6.0"

  val akkaHttpMetrics = "1.7.1"

}

object Library {

  val jackson: Seq[ModuleID] = Seq(
    "com.fasterxml.jackson.core" % "jackson-databind" % Version.jackson,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Version.jackson,
    // "javax.xml.bind" % "jaxb-api" % Version.jaxb,
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

  val clickhouse: Seq[ModuleID] = Seq("com.clickhouse" % "clickhouse-jdbc" % Version.clickhouse)
  val postgre: Seq[ModuleID] = Seq("org.postgresql" % "postgresql" % Version.postgres)
  val dbDrivers: Seq[ModuleID] = clickhouse ++ postgre

  val akka: Seq[ModuleID] = Seq(
    "com.typesafe.akka" %% "akka-slf4j" % Version.akka,
    "com.typesafe.akka" %% "akka-stream" % Version.akka,
    "com.typesafe.akka" %% "akka-testkit" % Version.akka
  ).map(_.cross(CrossVersion.for3Use2_13))

  val akkaHttp: Seq[ModuleID] = Seq(
    "com.typesafe.akka" %% "akka-http" % Version.akkaHttp,
    "com.typesafe.akka" %% "akka-http-spray-json" % Version.akkaHttp,
    "com.typesafe.akka" %% "akka-http-testkit" % Version.akkaHttp,
    "fr.davit" %% "akka-http-metrics-prometheus" % Version.akkaHttpMetrics
  ).map(_.cross(CrossVersion.for3Use2_13))

  val sprayJson: Seq[ModuleID] = Seq(
    "io.spray" %% "spray-json" % Version.sprayJson
  ).map(_.cross(CrossVersion.for3Use2_13))

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
    "org.tpolecat" %% "doobie-core" % Version.doobie,
    "org.tpolecat" %% "doobie-hikari" % Version.doobie
  )

  val scrum: Seq[ModuleID] = Seq(
    "com.github.mpilquist" %% "simulacrum" % Version.simulacrum
  )

  val twitterUtil: Seq[ModuleID] = Seq("com.twitter" %% "util-eval" % Version.twitterUtil)

  val scalaTest: Seq[ModuleID] = Seq(
    "org.scalactic" %% "scalactic" % Version.scalaTest,
    "org.scalatest" %% "scalatest" % Version.scalaTest,
    "org.scalatestplus" %% "scalacheck-1-17" % Version.scalaCheck
  )

  val perf: Seq[ModuleID] = Seq(
    "pl.project13.scala" %% "sbt-jmh" % Version.testContainers % Version.jmh
  )

  val testContainers: Seq[ModuleID] = Seq(
    "com.dimafeng" %% "testcontainers-scala" % Version.testContainers % "test",
    "com.dimafeng" %% "testcontainers-scala-clickhouse" % Version.testContainers % "test",
    "org.testcontainers" % "testcontainers" % Version.testContainersKafka % "test",
    "org.testcontainers" % "clickhouse" % Version.testContainersKafka % "test",
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

  val jol: Seq[ModuleID] = Seq(
    "org.openjdk.jol" % "jol-core" % Version.jolVersion
  )

  val redisson: Seq[ModuleID] = Seq(
    "org.redisson" % "redisson" % Version.redissonVersion,
    "com.esotericsoftware" % "kryo" % Version.kryoVersion
  )

}
