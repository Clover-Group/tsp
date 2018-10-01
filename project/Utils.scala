import sbt.Keys._
import sbt._
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease._
import ReleaseTransformations._
import ohnosequences.sbt.GithubRelease.keys.TagName

object Utils {

  def setVersion(selectVersion: Versions => String): ReleaseStep = { st: State =>
    val vs = st
      .get(ReleaseKeys.versions)
      .getOrElse(sys.error("No versions are set! Was this release part executed before inquireVersions?"))
    val selected = selectVersion(vs)

    st.log.info("Setting version to '%s'." format selected)
    val useGlobal = Project.extract(st).get(releaseUseGlobalVersion)
    val versionStr = "%s" format selected
    val file = Project.extract(st).get(releaseVersionFile)
    IO.writeLines(file, Seq(versionStr))

    // Write release notes from temporary WIP changelog to regular one
    writeWipToChangelog(versionStr)

    reapply(
      Seq(
        if (useGlobal) version in ThisBuild := selected
        else version := selected
      ),
      st
    )
  }

  def releaseNotes(tag: TagName): String = {
    val changelog: String = IO.read(file("./CHANGELOG"))
    val pattern = s"(?s)(?:^|\\n)## ${tag.stripPrefix("v")}\\s*(.*?)(?:\\n## |$$)".r
    pattern.findAllIn(changelog).group(1)
  }

  def writeWipToChangelog(tag: TagName): Unit = {
    val changelogWip: String = IO.read(file("./CHANGELOG.wip"))
    val changelog: String = IO.read(file("./CHANGELOG"))
    val newChangelog: String = s"## $tag\n$changelogWip\n$changelog"
    IO.write(file("./CHANGELOG"), newChangelog)
    IO.write(file("./CHANGELOG.wip"), "")
  }
}