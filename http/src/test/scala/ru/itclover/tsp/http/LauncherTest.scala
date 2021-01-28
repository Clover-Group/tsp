package ru.itclover.tsp.http

import org.scalatest.{FlatSpec, Matchers}

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class LauncherTest extends FlatSpec with Matchers {
  "Launcher" should "launch" in {
    a[RuntimeException] should be thrownBy Launcher.main(Array())
    a[RuntimeException] should be thrownBy Launcher.main(Array("flink", "local"))
    a[RuntimeException] should be thrownBy Launcher.main(Array("flink local"))
    a[RuntimeException] should be thrownBy Launcher.main(Array("flink-cluster spark-cluster")) // Cannot be run from here
    //noException should be thrownBy Launcher.main(Array("flink-local spark-local"))
  }

//  "Launcher" should "launch in cluster mode" in {
//    noException should be thrownBy Launcher.main(Array("flink-cluster-test", ""))
//  }
}
