package ru.itclover.tsp.http

import org.scalatest.{FlatSpec, Matchers}

class LauncherTest extends FlatSpec with Matchers {
  "Launcher" should "launch" in {
    a[RuntimeException] should be thrownBy Launcher.main(Array())
    a[RuntimeException] should be thrownBy Launcher.main(Array("flink", "local"))
    a[RuntimeException] should be thrownBy Launcher.main(Array("flink local"))
    a[RuntimeException] should be thrownBy Launcher.main(Array("flink-cluster")) // Cannot be run from here
    noException should be thrownBy Launcher.main(Array("flink-local"))
  }
}
