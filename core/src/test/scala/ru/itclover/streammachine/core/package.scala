package ru.itclover.streammachine

import org.joda.time.{DateTime, Instant}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.phases.NoState
import ru.itclover.streammachine.phases.NumericPhases.{NumericPhaseParser, SymbolNumberExtractor}

package object core {

  case class TestingEvent[T](result: PhaseResult[T], time: DateTime = DateTime.now())

  implicit val doubleTestEvent = new TimeExtractor[TestingEvent[Double]] {
    override def apply(event: TestingEvent[Double]) = event.time
  }

  implicit val boolTestEvent = new TimeExtractor[TestingEvent[Boolean]] {
    override def apply(event: TestingEvent[Boolean]) = event.time
  }

  case class TestPhase[T]() extends PhaseParser[TestingEvent[T], NoState, T] {
    override def initialState: NoState = NoState.instance

    override def apply(event: TestingEvent[T], state: NoState) = event.result -> NoState.instance

    override def format(event: TestingEvent[T], state: NoState) = s"TestPhase(${event})"
  }


  class ConstResult[T](result: PhaseResult[T]) extends PhaseParser[TestingEvent[T], Unit, T] {
    override def apply(v1: TestingEvent[T], v2: Unit): (PhaseResult[T], Unit) = result -> ()

    override def initialState: Unit = ()
  }

  val probe = TestingEvent(Success(1))

  val alwaysSuccess: PhaseParser[TestingEvent[Int], Unit, Int] = new ConstResult(Success(0))
  val alwaysFailure: PhaseParser[TestingEvent[Int], Unit, Int] = new ConstResult(Failure("failed"))
  val alwaysStay: PhaseParser[TestingEvent[Int], Unit, Int] = new ConstResult(Stay)

  val t = DateTime.now()
  val times = t.minusMillis(11000) :: t.minusMillis(10000) :: t.minusMillis(9000) :: t.minusMillis(8000) ::
    t.minusMillis(7000) :: t.minusMillis(6000) :: t.minusMillis(5000) :: t.minusMillis(4000) :: t.minusMillis(3000) ::
    t.minusMillis(2000) :: t.minusMillis(1000) :: t :: Nil

  private val staySuccessRes = Seq(Stay, Success(1.0), Stay, Success(2.0), Success(1.0), Success(3.0), Failure("Test"), Success(4.0))
  val staySuccesses = for((t, res) <- times.take(staySuccessRes.length).zip(staySuccessRes)) yield TestingEvent(res, t)

  def getTestingEvents[T](innerResults: Seq[PhaseResult[T]]) = for(
    (t, res) <- times.take(innerResults.length).zip(innerResults)
  ) yield TestingEvent(res, t)

  val failsRes = Seq(Stay, Success(1.0), Failure("Test"), Success(2.0), Failure("Test"), Success(1.0), Stay, Failure("Test"), Success(3.0), Failure("Test"), Stay)
  val fails = for((t, res) <- times.take(failsRes.length).zip(failsRes)) yield TestingEvent(res, t)


  def fakeMapper[Event, PhaseOut](p: PhaseParser[Event, _, PhaseOut]) = FakeMapper[Event, PhaseOut]()

  def runRule[Event, Out](rule: PhaseParser[Event, _, Out], events: Seq[Event]): Vector[PhaseResult.TerminalResult[Out]] = {
    val mapResults = fakeMapper(rule)
    events
      .foldLeft(PatternMapper(rule, mapResults)) { case (machine, event) => machine(event) }
      .result
  }

}
