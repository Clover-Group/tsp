resolvers in ThisBuild ++= Seq("Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/", Resolver.mavenLocal)

name := "StreamMachine"

version := "0.1-SNAPSHOT"

organization := "ru.itclover"

scalaVersion in ThisBuild := "2.11.7"

val flinkVersion = "1.3.2"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

lazy val root = (project in file("."))
  .aggregate(core, config, http)

mainClass in assembly := Some("ru.itclover.Job")

// make run command include the provided dependencies
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

lazy val core = project.in(file("core"))

lazy val config = project.in(file("config"))
  .dependsOn(core)

lazy val http = project.in(file("http"))
  .dependsOn(core, config)

lazy val flinkConnector = project.in(file("flink"))
  .settings(libraryDependencies ++= flinkDependencies)
