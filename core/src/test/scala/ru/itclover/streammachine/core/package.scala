package ru.itclover.streammachine

import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core.Time.TimeExtractor

package object core {

  case class TestEvent(i: Int, s: String)

  val probe = TestEvent(1, "")

  class ConstantResult(result: PhaseResult[Int]) extends PhaseParser[TestEvent, Unit, Int] {
    override def apply(v1: TestEvent, v2: Unit): (PhaseResult[Int], Unit) = result -> ()

    override def initialState = ()
  }

  val alwaysSuccess = new ConstantResult(Success(0))
  val alwaysFailure = new ConstantResult(Failure("failed"))
  val alwaysStay = new ConstantResult(Stay)

  def fakeMapper[Event, PhaseOut](p: PhaseParser[Event, _, PhaseOut]) = FakeMapper[Event, PhaseOut]()

  def runRule[Event, Out](rule: PhaseParser[Event, _, Out], events: Seq[Event]): Vector[PhaseResult.TerminalResult[Out]] = {
    val mapResults = fakeMapper(rule)
    events
      .foldLeft(StateMachineMapper(rule, mapResults)) { case (machine, event) => machine(event) }
      .result
  }

}
