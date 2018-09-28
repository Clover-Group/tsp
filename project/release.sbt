addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.9")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.9")
// see https://github.com/ohnosequences/sbt-github-release/pull/29
// addSbtPlugin("ohnosequences" % "sbt-github-release" % "0.7.0")
lazy val root = project.in(file(".")).dependsOn(ghReleasePlugin)
lazy val ghReleasePlugin = RootProject(uri("https://github.com/hyst329/sbt-github-release.git"))