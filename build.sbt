
/*** Settings ***/

name := "TSP"
organization in ThisBuild := "ru.itclover" // Fallback-settings for all sub-projects (ThisBuild task)
maintainer in Docker := "Clover Group"
dockerUsername in Docker := Some("clovergrp")

//version in ThisBuild := IO.read(file("./VERSION"))
scalaVersion in ThisBuild := "2.11.12"
resolvers in ThisBuild ++= Seq("Apache Development Snapshot Repository" at
    "https://repository.apache.org/content/repositories/snapshots/", Resolver.mavenLocal)

lazy val launcher = "ru.itclover.tsp.http.Launcher"

lazy val commonSettings = Seq(
  // Improved type inference via the fix for SI-2712 (for Cats dep.)
  scalacOptions ++= Seq(
    "-Ypartial-unification", // allow the compiler to unify type constructors of different arities
    "-deprecation"           // warn about use of deprecated APIs
    // "-Xlint"                 // enable handy linter warnings
  ),
  ghreleaseNotes := Utils.releaseNotes,
  ghreleaseRepoOrg := "Clover-Group",
  ghreleaseRepoName := "tsp",
  githubRelease := null // don't release subprojects
)

lazy val assemblySettings = Seq(
  assemblyJarName := s"TSP_v${version.value}.jar"
)

// make run command include the provided dependencies (for sbt run)
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

// make native packager use only the fat jar
mappings in Universal := {
  // universalMappings: Seq[(File,String)]
  val universalMappings = (mappings in Universal).value
  val fatJar = (assembly in mainRunner).value
  // removing means filtering
  val filtered = universalMappings filter {
    case (file, name) => !name.contains(".jar")
  }
  // add the fat jar
  filtered :+ (fatJar -> ("lib/" + fatJar.getName))
}

mappings in Docker := {
  // universalMappings: Seq[(File,String)]
  val universalMappings = (mappings in Universal).value
  val fatJar = (assembly in mainRunner).value
  // removing means filtering
  val filtered = universalMappings filter {
    case (file, name) => !name.contains(".jar")
  }
  // add the fat jar
  filtered :+ (fatJar -> ("lib/" + fatJar.getName))
}

scriptClasspath := Seq((assemblyJarName in (assembly in mainRunner)).value)

// clear the existing docker commands
dockerCommands := Seq()

import com.typesafe.sbt.packager.docker._
dockerCommands := Seq(
  Cmd("FROM", "openjdk:11"),
  Cmd("LABEL", s"""MAINTAINER="${(maintainer in Docker).value}""""),
  Cmd("ADD", s"lib/${(assembly in mainRunner).value.getName}", "/opt/tsp.jar"),
  ExecCmd("CMD", "sh", "-c", "java -XX:+UnlockExperimentalVMOptions -XX:+UseJVMCICompiler -jar /opt/tsp.jar $EXECUTION_TYPE")
)


/*** Projects configuration ***/

lazy val mainRunner = project.in(file("mainRunner")).dependsOn(http)
  .settings(commonSettings)
  .settings(
    // we set all provided dependencies to none, so that they are included in the classpath of mainRunner
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
  .enablePlugins(GitVersioning, JavaAppPackaging, UniversalPlugin)
  .settings(commonSettings)
  .settings(githubRelease := Utils.defaultGithubRelease.evaluated)
  .aggregate(core, config, http, flinkConnector, spark, dsl)
  .dependsOn(core, config, http, flinkConnector, spark, dsl)

lazy val core = project.in(file("core"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Library.scalaTest ++ Library.logging ++ Library.config ++ Library.cats ++ Library.shapeless
  )

lazy val config = project.in(file("config"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "ru.itclover.tsp"
  )
  .dependsOn(core)

lazy val flinkConnector = project.in(file("flink"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Library.twitterUtil ++ Library.flink ++ Library.scalaTest ++ Library.dbDrivers
      ++ Library.kafka ++ Library.jackson
  )
  .dependsOn(core, config, dsl)

lazy val http = project.in(file("http"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Library.scalaTest ++ Library.flink ++ Library.akka ++
      Library.akkaHttp ++ Library.twitterUtil
  )
  .dependsOn(core, config, flinkConnector, dsl)

lazy val spark = project.in(file("spark"))
  .settings(commonSettings)
  .settings(
    fork in run := true,
    libraryDependencies ++= Library.sparkStreaming
  )
  .dependsOn(core, config)

lazy val dsl = project.in(file("dsl"))
  .settings(commonSettings)
  .settings(
    resolvers += "bintray-djspiewak-maven" at "https://dl.bintray.com/djspiewak/maven",
    libraryDependencies ++=  Library.scalaTest ++ Library.parboiled
  ).dependsOn(core)


lazy val integrationCorrectness = project.in(file("integration/correctness"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Library.flink ++ Library.scalaTest ++ Library.dbDrivers ++ Library.testContainers
  )
  .dependsOn(core, flinkConnector, http, config)

lazy val integrationPerformance = project.in(file("integration/performance"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Library.flink ++ Library.scalaTest ++ Library.dbDrivers ++ Library.testContainers
  )
  .dependsOn(integrationCorrectness)

// Git-specific settings
import sbtrelease.Version.Bump
import sbtrelease.{versionFormatError, Version => ReleaseVersion}

git.useGitDescribe := true
git.baseVersion := IO.read(file("./VERSION")) // if no tags are present
val VersionRegex = "v([0-9]+.[0-9]+.[0-9]+)-?(.*)?".r

def nextVersion(v: String): String = Bump.Next.bump(ReleaseVersion(v).getOrElse(versionFormatError)).string

git.gitTagToVersionNumber := {
  case VersionRegex(v, "") => Some(v)
  case VersionRegex(v, "SNAPSHOT") => Some(s"${nextVersion(v)}-SNAPSHOT")
  case VersionRegex(v, s) if s.matches("[0-9].+") => Some(s"${nextVersion(v)}-$s")
  case VersionRegex(v, s) => Some(s"$v-$s")
  case _ => None
}

// Release specific settings
import ReleaseTransformations.{setReleaseVersion => _, _}


lazy val setReleaseVersion: ReleaseStep = Utils.setVersion(_._1)
lazy val commitChangelogs: ReleaseStep = Utils.commitChangelogs

releaseVersionFile := file("./VERSION")
releaseUseGlobalVersion := false

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,              // : ReleaseStep
  inquireVersions,                        // : ReleaseStepdockerUsername
  runClean,                               // : ReleaseStep
  runTest,                                // : ReleaseStep
  setReleaseVersion,                      // : ReleaseStep (custom)
  commitReleaseVersion,                   // : ReleaseStep, performs the initial git checks
  commitChangelogs,                       // : ReleaseStep (custom)
  tagRelease,                             // : ReleaseStep
  // TODO: Configure publishing on GitHub (if needed)
  // publishArtifacts,                    // : ReleaseStep, checks whether `publishTo` is properly set up
  pushChanges                             // : ReleaseStep, also checks that an upstream branch is properly configured
)

ghreleaseAssets := Seq(file(s"./mainRunner/target/scala-2.11/TSP_v${version.value}.jar"))

githubRelease := githubRelease.dependsOn(assembly in mainRunner).evaluated
