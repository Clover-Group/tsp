import sbt.Keys._
import sbt._
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease._
import ReleaseTransformations._

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

    reapply(
      Seq(
        if (useGlobal) version in ThisBuild := selected
        else version := selected
      ),
      st
    )
  }
}