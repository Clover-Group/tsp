import sbt.Keys._
import sbt.{Def, _}
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease._
import ReleaseTransformations._
import ohnosequences.sbt.GithubRelease.DefTask
import ohnosequences.sbt.GithubRelease.keys.TagName
import ohnosequences.sbt.GithubRelease.defs.githubRelease
import ohnosequences.sbt.SbtGithubReleasePlugin.tagNameArg
import org.kohsuke.github.GHRelease

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

  val changelogFileName: TagName = "./CHANGELOG"
  val changelogWipFileName: TagName = "./CHANGELOG.wip"

  def releaseNotes(tag: TagName): String = {
    val changelog: String = IO.read(file(changelogFileName))
    val pattern = s"(?s)(?:^|\\n)## ${tag.stripPrefix("v")}\\s*(.*?)(?:\\n## |$$)".r
    pattern.findAllIn(changelog).group(1)
  }

  def writeWipToChangelog(tag: TagName): Unit = {
    val changelogWip: String = IO.read(file(changelogWipFileName))
    val changelog: String = IO.read(file(changelogFileName))
    val newChangelog: String = s"## $tag\n$changelogWip\n$changelog"
    IO.write(file(changelogFileName), newChangelog)
    IO.write(file(changelogWipFileName), "")
  }

  private def vcs(st: State): Vcs = {
    Project.extract(st).get(releaseVcs).getOrElse(sys.error("Aborting release. Working directory is not a repository of a recognized VCS."))
  }

  def commitChangelogs: ReleaseStep = { st: State =>
    vcs(st).add(changelogFileName, changelogWipFileName)
    val sign = Project.extract(st).get(releaseVcsSign)
    val ver = Project.extract(st).get(version)
    vcs(st).commit(s"updated CHANGELOGS for $ver", sign)
    st
  }

  def defaultGithubRelease: Def.Initialize[InputTask[GHRelease]] = Def.inputTaskDyn {
    githubRelease(tagNameArg.parsed)
  }
}