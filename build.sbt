resolvers in ThisBuild ++= Seq("Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/", Resolver.mavenLocal)

name := "StreamMachine"

version := "0.1-SNAPSHOT"

organization := "ru.itclover"

scalaVersion in ThisBuild := "2.11.8"

lazy val root = (project in file("."))
  .aggregate(core, config, http, flinkConnector, spark)

mainClass in assembly := Some("Job")

// make run command include the provided dependencies
//run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

lazy val core = project.in(file("core"))
  .settings(libraryDependencies ++= Library.scalaTest ++ Library.jodaTime ++ Library.logging)

lazy val config = project.in(file("config"))
  .dependsOn(core)

lazy val flinkConnector = project.in(file("flink"))
  .settings(
    libraryDependencies
      ++= Library.twitterUtil
      ++ Library.flink
      ++ Library.scalaTest
      ++ Library.clickhouse
      ++ Library.jackson
  )
  .dependsOn(core, config)

lazy val http = project.in(file("http"))
  .settings(libraryDependencies ++=
    Library.scalaTest ++ Library.flink ++ Library.akka ++ Library.akkaHttp ++ Library.twitterUtil)
  .dependsOn(core, config, flinkConnector)

lazy val mainRunner = project.in(file("mainRunner")).dependsOn(flinkConnector).settings(
  // we set all provided dependencies to none, so that they are included in the classpath of mainRunner
  libraryDependencies := (libraryDependencies in flinkConnector).value.map {
    module =>
      if (module.configurations.contains("provided")) {
        module.withConfigurations(configurations = None)
      } else {
        module
      }
  },
  mainClass in(Compile, run) := Some("ru.itclover.streammachine.RulesDemo")
)

lazy val integration = project.in(file("integration"))
  .settings(libraryDependencies ++= Library.flink ++ Library.scalaTest ++ Library.clickhouse ++ Library.testContainers)
  .dependsOn(core, flinkConnector, http, config)

lazy val spark = project.in(file("spark"))
  .settings(
    fork in run := true,
    libraryDependencies ++= Library.sparkStreaming)
  .dependsOn(core, config)

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))