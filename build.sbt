
/*** Settings ***/

name := "TSP"
organization in ThisBuild := "ru.itclover" // Fallback-settings for all sub-projects (ThisBuild task)
maintainer in Docker := "Clover Group"
dockerUsername in Docker := Some("clovergrp")
dockerUpdateLatest := true

scalaVersion in ThisBuild := "2.12.8"
resolvers in ThisBuild ++= Seq("Apache Development Snapshot Repository" at
    "https://repository.apache.org/content/repositories/snapshots/", Resolver.mavenLocal)
//javaOptions in ThisBuild += "--add-modules=java.xml.bind"

lazy val launcher = "ru.itclover.tsp.http.Launcher"
 
lazy val commonSettings = Seq(
  // Improved type inference via the fix for SI-2712 (for Cats dep.)
  ghreleaseNotes := Utils.releaseNotes,
  ghreleaseRepoOrg := "Clover-Group",
  ghreleaseRepoName := "tsp",
  //scalacOptions ++= Seq(
  //  //"-language:reflectiveCalls"
  //    "-Yrangepos",
  //    "-Ywarn-unused-import",
  //),
  //addCompilerPlugin(scalafixSemanticdb),
  scalacOptions ++= Seq(
    "-Ypartial-unification", // allow the compiler to unify type constructors of different arities
    "-deprecation",          // warn about use of deprecated APIs
    "-feature",               // warn about feature warnings 
  ),
  // don't release subprojects
  githubRelease := null,
  skip in publish := true,
  maxErrors := 5, 
)

lazy val assemblySettings = Seq(
  assemblyJarName := s"TSP_v${version.value}.jar",
  javaOptions += "--add-modules=java.xml.bind"
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
  //Cmd("FROM", "openjdk:12.0.1-jdk-oracle"),
  Cmd("FROM", "openjdk:11-jre-slim"),
  Cmd("LABEL", s"""MAINTAINER="${(maintainer in Docker).value}""""),
  Cmd("ADD", s"lib/${(assembly in mainRunner).value.getName}", "/opt/tsp.jar"),
  ExecCmd("CMD", "sh", "-c", "java ${TSP_JAVA_OPTS:--Xms1G -Xmx6G} -jar /opt/tsp.jar $EXECUTION_TYPE")
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
    skip in publish := false,
    mainClass := Some(launcher),
    inTask(assembly)(assemblySettings)
  )


lazy val runTask = taskKey[Unit]("App runner")

//runTask := {
// (http/runMain ${TSP_LAUNCHER:-ru.itclover.tsp.http.Launcher} ${TSP_LAUNCHER_ARGS:-flink-local})
//}

lazy val root = (project in file("."))
  .enablePlugins(GitVersioning, JavaAppPackaging, UniversalPlugin, JmhPlugin)
  .settings(commonSettings)
  .settings(githubRelease := Utils.defaultGithubRelease.evaluated)
  .aggregate(core, config, http, flink, dsl, itValid)
  .dependsOn(core, config, http, flink, dsl, itValid)

lazy val core = project.in(file("core"))
  .enablePlugins(JmhPlugin)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Library.scalaTest ++ Library.logging ++ Library.config ++ Library.cats
  )

lazy val config = project.in(file("config"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "ru.itclover.tsp"
  )
  .dependsOn(core)

lazy val flink = project.in(file("flink"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Library.flink ++ Library.scalaTest ++ Library.dbDrivers
  )
  .dependsOn(core, config, dsl)

lazy val http = project.in(file("http"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Library.scalaTest ++ Library.flink ++ Library.akka ++
      Library.akkaHttp ++ Library.arrow
  )
  .dependsOn(core, config, flink, dsl)

lazy val dsl = project.in(file("dsl"))
  .settings(commonSettings)
  .settings(
    resolvers += "bintray-djspiewak-maven" at "https://dl.bintray.com/djspiewak/maven",
    libraryDependencies ++=  Library.scalaTest ++ Library.logging ++ Library.parboiled
  ).dependsOn(core)

lazy val itValid = project.in(file("integration/correctness"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Library.flink ++ Library.scalaTest ++ Library.dbDrivers ++ Library.testContainers
  )
  .dependsOn(core, flink, http, config)

lazy val itPerf = project.in(file("integration/performance"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Library.flink ++ Library.scalaTest ++ Library.dbDrivers ++ Library.testContainers
  )
  .dependsOn(itValid)


/*** Other settings ***/

// Kind projector
resolvers += Resolver.sonatypeRepo("releases")
//addCompilerPlugin("org.spire-math" %% "kind-projector" % Version.kindProjector)
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)


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
  checkSnapshotDependencies,              // prevents the release if any dependencies are SNAPSHOT
  inquireVersions,                        // ask the user for version tag to be released
  runClean,                               // performs cleaning task
  setReleaseVersion,                      // sets the version and transfers notes to CHANGELOG
  commitReleaseVersion,                   // performs the initial git checks
  commitChangelogs,                       // commits the changes in CHANGELOG files
  tagRelease,                             // tags the prepared release
  pushChanges                             // pushes into upstream, also checks that an upstream branch is properly configured
)

ghreleaseAssets := Seq(file(s"./mainRunner/target/scala-2.12/TSP_v${version.value}.jar"))

githubRelease := githubRelease.dependsOn(assembly in mainRunner).evaluated


addCommandAlias("com", "all compile test:compile it:compile")
addCommandAlias("lint", "; compile:scalafix --check ; test:scalafix --check")
addCommandAlias("fix", "all compile:scalafix test:scalafix")
addCommandAlias("fmt", "; scalafmtSbt; scalafmtAll; test:scalafmtAll")
addCommandAlias("chk", "; scalafmtSbtCheck; scalafmtCheck; test:scalafmtCheck")
addCommandAlias("cov", "; clean; coverage; test; coverageReport")
addCommandAlias("tree", "dependencyTree::toFile target/tree.txt -f")
addCommandAlias("pub", "docker:publish")
