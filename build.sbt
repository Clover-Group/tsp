
/*** Settings ***/

name  := "StreamMachine"
organization in ThisBuild := "ru.itclover" // Fallback-settings for all sub-projects (ThisBuild task)

version in ThisBuild := IO.read(file("./VERSION"))
scalaVersion in ThisBuild := "2.11.12"
resolvers in ThisBuild ++= Seq("Apache Development Snapshot Repository" at
    "https://repository.apache.org/content/repositories/snapshots/", Resolver.mavenLocal)

lazy val launcher = "ru.itclover.streammachine.http.Launcher"

lazy val commonSettings = Seq(
  // Improved type inference via the fix for SI-2712 (for Cats dep.)
  scalacOptions ++= Seq("-Ypartial-unification")
)

lazy val assemblySettings = Seq(
  assemblyJarName := s"StreamMachine_v${version.value}.jar"
)

// make run command include the provided dependencies (for sbt run)
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))


/*** Projects configuration ***/

lazy val mainRunner = project.in(file("mainRunner")).dependsOn(http)
  .settings(commonSettings)
  .settings(
    // we set all provided dependencies to none, so that they are included in the classpath of mainRunnerZZ
    libraryDependencies := (libraryDependencies in http).value.map { module =>
      if (module.configurations.contains("provided")) {
        module.withConfigurations(configurations = None)
      } else {
        module
      }
    },
    mainClass := Some(launcher),
    inTask(assembly)(assemblySettings)
  )


lazy val root = (project in file("."))
  .settings(commonSettings)
  .aggregate(core, config, http, flinkConnector, spark)

lazy val core = project.in(file("core"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Library.scalaTest ++ Library.jodaTime ++ Library.logging ++ Library.config
  )

lazy val config = project.in(file("config"))
  .settings(commonSettings)
  .dependsOn(core)

lazy val flinkConnector = project.in(file("flink"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Library.twitterUtil ++ Library.flink ++ Library.scalaTest ++ Library.dbDrivers
      ++ Library.kafka ++ Library.jackson ++ Library.cats
  )
  .dependsOn(core, config)

lazy val http = project.in(file("http"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Library.scalaTest ++ Library.flink ++ Library.akka ++
      Library.akkaHttp ++ Library.twitterUtil
  )
  .dependsOn(core, config, flinkConnector)

lazy val integration = project.in(file("integration"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Library.flink ++ Library.scalaTest ++ Library.dbDrivers ++ Library.testContainers
  )
  .dependsOn(core, flinkConnector, http, config)

lazy val spark = project.in(file("spark"))
  .settings(commonSettings)
  .settings(
    fork in run := true,
    libraryDependencies ++= Library.sparkStreaming
  )
  .dependsOn(core, config)

lazy val oldsyntax = project.in(file("oldsyntax"))
  .settings(
    resolvers += "bintray-djspiewak-maven" at "https://dl.bintray.com/djspiewak/maven",
    libraryDependencies ++=  Library.scalaTest ++ Library.parseback ++ Library.cats
  ).dependsOn(core)

lazy val newsyntax = project.in(file("newsyntax"))
  .settings(
    resolvers += "bintray-djspiewak-maven" at "https://dl.bintray.com/djspiewak/maven",
    libraryDependencies ++=  Library.scalaTest ++ Library.parboiled
  ).dependsOn(core)