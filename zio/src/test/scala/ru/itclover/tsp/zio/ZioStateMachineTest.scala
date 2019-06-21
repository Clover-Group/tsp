package ru.itclover.tsp.zio

import org.scalatest._
import ru.itclover.tsp.v2.Common._
import ru.itclover.tsp.v2._
import scalaz.zio.{DefaultRuntime, UIO}

class ZioStateMachineTest extends FlatSpec with Matchers {

  it should "process ConstPattern correctly" in {

    // Run FSM
    val pat = ConstPattern[EInt, Int](0)(extractor)
    val input = UIO(Seq(event))
    val result = (for (output <- ZioStateMachine.run(pat)(input);
                       result <- output.takeAll) yield {
      result.size shouldBe 1
      result.head shouldBe IdxValue(1, Result.succ(0))
    })

    new DefaultRuntime {}.unsafeRun(result)

  }

}
