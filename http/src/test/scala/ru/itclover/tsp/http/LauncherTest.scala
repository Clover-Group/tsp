package ru.itclover.tsp.http

import org.scalatest.{FlatSpec, Matchers}

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class LauncherTest extends FlatSpec with Matchers {
  "Launcher" should "launch" in {
    noException should be thrownBy Launcher.main(Array.empty)
  }
}
